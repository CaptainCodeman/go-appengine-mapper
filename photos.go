package main

import (
	"time"

	"encoding/gob"
	"net/http"

	"github.com/labstack/echo"
	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/delay"
	"google.golang.org/appengine/log"
)

type (
	// PhotoProcessor is the interface that any processor has to implement
	PhotoProcessor interface {
		// Start is called when a new batch is starting. It should return the
		// query range parameters including the cursor start position, the query
		// batch size to use and how long the processing should be allowed
		// to run before being scheduled to continue. It can also initialize any
		// aggregation collections that it wants to use
		Start(c context.Context) *photoProcessorRange

		// Process is called once for each item returned
		Process(c context.Context, photo *Photo)

		// Complete is called at the end of the processing so that the processor
		// can write any aggregated values it wants to before the processing is
		// scheduled to continue (if necessary) or the complete task is done
		Complete(c context.Context)
	}

	photoProcessorRange struct {
		From    time.Time
		To      time.Time
		Start   string
		Size    int
		Timeout time.Duration
	}

	photoProcessorFn func(r *photoProcessorRange) PhotoProcessor
)

var (
	processPhotosFunc *delay.Function
	photoProcessors = make(map[string]photoProcessorFn)
)

func init() {
	// created in init because it's called inside the function
	processPhotosFunc = delay.Func("processPhotos", processPhotos)

	// complete endpoint will be something like "/_ah/cron/photos/log"
	cron.Get("/photos/:processor", processPhotosHandler)
}

// each processor needs to register itself so we can find it by name
// because the instance that picks up the task may not be the same one
// that initiated it. We also need to register the processor type with
// the gob serializer
func registerPhotoProcessor(name string, fn photoProcessorFn) {
	photoProcessors[name] = fn
	processor := fn(&photoProcessorRange{})
	gob.Register(processor)
}

func processPhotosHandler(c *echo.Context) error {
	ctx := appengine.NewContext(c.Request())

	var from time.Time
	var to time.Time
	var err error

	processorName := c.Param("processor")
	fromStr := c.Query("from")
	toStr := c.Query("to")
	now := time.Now().UTC()

	// default to previous day but allow any
	if toStr == "" {
		to = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		to, err = time.Parse(dateFormat, toStr)
		if err != nil {
			log.Errorf(ctx, "to error %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}

	if fromStr == "" {
		from = to.Add(time.Duration(-24) * time.Hour)
	} else {
		from, err = time.Parse(dateFormat, fromStr)
		if err != nil {
			log.Errorf(ctx, "from error %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}

	r := &photoProcessorRange{
		From: from,
		To: to,
	}
	processorFn := photoProcessors[processorName]
	processor := processorFn(r)
	log.Debugf(c, "%#v", processor)
	processPhotosFunc.Call(ctx, processor)

	return c.NoContent(http.StatusOK)
}

func processPhotos(c context.Context, processor PhotoProcessor) error {
	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10) * time.Minute)

	r := processor.Start(c)
	log.Debugf(c, "processPhotos from %s to %s cursor %s", r.From.Format(dateFormat), r.To.Format(dateFormat), r.Start)

	// TODO: describe pros & cons of different querying + continuation strategies

	q := datastore.NewQuery("photo")
	q = q.Filter("taken >=", r.From)
	q = q.Filter("taken <", r.To)
	q = q.Order("taken")

	// I use keys only because it saves on cost - entities come from memcache if possible
	q = q.KeysOnly()

	var cursor *datastore.Cursor
	if r.Start != "" {
		newCursor, err := datastore.DecodeCursor(r.Start)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return err
		}
		cursor = &newCursor
	}

	// only one entity is loaded at a time
	p := new(Photo)

	timeout := make(chan bool, 1)
	timer := time.AfterFunc(r.Timeout, func(){
		timeout <- true
	})
	defer timer.Stop()

Loop:
	for {
		// check if we've timed out or whether to keep going
		select {
			case <- timeout:
				break Loop
			default:
		}

		processed := 0

		q = q.Limit(r.Size)
		if cursor != nil {
			q = q.Start(*cursor)
		}
		it := q.Run(c)
		for {
			// if not using keys only then we would load the actual entity here using
			// key, err := it.Next(p)
			key, err := it.Next(nil)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "get key error %s", err.Error())
				return err
			}

			// loads the actual entity from memcache / datastore
			err = nds.Get(c, key, p)
			if err != nil {
				log.Errorf(c, "get photo error %s", err.Error())
				return err
			}

			// call the processor with the entity
			p.ID = key.IntID()
			processor.Process(c, p)

			processed++
		}

		// did we process a full batch? if so, there may be more
		if processed == r.Size {
			newCursor, err := it.Cursor()
			if err != nil {
				log.Errorf(c, "get next cursor error %s", err.Error())
				return err
			}
			cursor = &newCursor
		} else {
			// otherwise we're finished
			cursor = nil
			break
		}
	}

	// let the processor write any aggregation entries / tasks etc...
	processor.Complete(c)

	// if we didn't complete everything then continue from the cursor
	if cursor != nil {
		r.Start = cursor.String()
		processPhotosFunc.Call(c, processor)
	}

	return nil
}
