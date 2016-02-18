package main

import (
	"fmt"
	"strings"
	"time"

	"encoding/gob"
	"net/http"
	// "net/url"

	"github.com/labstack/echo"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/delay"
	"google.golang.org/appengine/log"
)

// TODO: use named tasks instead of delay for proper fan-out / fan-in

type (
	// Processor is the interface that any processor has to implement
	Processor interface {
		// Start is called when a new batch is starting. It should return the
		// query to use and how long the processing should be allowed to run
		// before being scheduled to continue. It can also initialize any
		// aggregation collections that it wants to use
		Start(c context.Context) (*datastore.Query, interface{})

		// Process is called once for each item
		Process(c context.Context, key *datastore.Key)

		// Complete is called at the end of the processing so that the processor
		// can write any aggregated values it wants to before the processing is
		// scheduled to continue (if necessary) or the complete task is done
		Complete(c context.Context)
	}

	// ParamAdapter is a simple interface to avoid coupling the processor structs
	// to the web framework being used, we can instead provide an adapter to get
	// any querystring parameters that we need (or pass in URL?)
	ParamAdapter interface {
		Get(name string) string
	}

	echoParamAdapter struct {
		c *echo.Context
	}

	processorFn func(params ParamAdapter) (Processor, error)
)

var (
	processFunc *delay.Function
	processors = make(map[string]processorFn)
)

func init() {
	// created in init because it's called inside the function itself
	processFunc = delay.Func("process", process)

	// complete endpoint will be something like "/_ah/cron/process/logPhotos"
	cron.Get("/process/:name", processHandler)
}

// each processor needs to register itself so we can find it by name
// because the instance that picks up the task may not be the same one
// that initiated it. We also need to register the processor type with
// the gob serializer. Note we are passing the function to create the
// processor, not the processor itself
func registerProcessor(fn processorFn) {
	processor, _ := fn(nil)
	name := fmt.Sprintf("%T", processor)
	name = name[strings.LastIndex(name, ".")+1:len(name)]
	processors[name] = fn
	gob.Register(processor)
}

// Adapter for echo to get params
func newEchoParamAdapter(c *echo.Context) ParamAdapter {
	return &echoParamAdapter{c}
}

func (a *echoParamAdapter) Get(name string) string {
	return a.c.Query(name)
}

// callable handler to kick off a processing run
func processHandler(c *echo.Context) error {
	ctx := appengine.NewContext(c.Request())

	name := c.Param("name")
	processorFn, found := processors[name]
	if !found {
		log.Errorf(ctx, "processor %s not found", name)
		return echo.NewHTTPError(http.StatusInternalServerError, "processor not found")
	}

	paramAdapter := newEchoParamAdapter(c)
	processor, err := processorFn(paramAdapter)
	if err != nil {
		log.Errorf(ctx, "error %s", err.Error())
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	processFunc.Call(ctx, processor, "")

	return c.NoContent(http.StatusOK)
}

func process(c context.Context, processor Processor, start string) error {
	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10) * time.Minute)

	// get the query to iterate and the entity slot to load (could be nill for keys_only)
	q, e := processor.Start(c)

	var cursor *datastore.Cursor
	if start != "" {
		newCursor, err := datastore.DecodeCursor(start)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return err
		}
		cursor = &newCursor
	}

	// signal a timeout after 5 minutes
	timeout := make(chan bool, 1)
	timer := time.AfterFunc(time.Duration(5)*time.Minute, func(){
		timeout <- true
	})
	defer timer.Stop()

	// TODO: error handling to retry
Loop:
	for {
		// check if we've timed out or whether to keep going
		select {
			case <- timeout:
				break Loop
			default:
		}

		processed := 0

		if cursor != nil {
			q = q.Start(*cursor)
		}
		it := q.Run(c)
		for {
			key, err := it.Next(e)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "get key error %s", err.Error())
				return err
			}

			processor.Process(c, key)
			processed++
		}

		// did we process any?
		if processed > 0 {
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
		processFunc.Call(c, processor, cursor.String())
	}

	return nil
}
