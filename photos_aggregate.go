package main

import (
	"time"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	aggregatePhotos struct {
		From    time.Time
		To      time.Time

		// note non-exported member - we don't need it serialized between tasks
		counts map[int64]int64
	}
)

func init() {
	registerProcessor(newAggregatePhotos)
}

func newAggregatePhotos(params ParamAdapter) (Processor, error) {
	p := new(aggregatePhotos)
	if params == nil {
		return p, nil
	}

	fromStr := params.Get("from")
	toStr := params.Get("to")
	now := time.Now().UTC()
	var err error

	// default to previous day but allow any
	if toStr == "" {
		p.To = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		p.To, err = time.Parse(dateFormat, toStr)
		if err != nil {
			return p, err
		}
	}

	if fromStr == "" {
		p.From = p.To.Add(time.Duration(-24) * time.Hour)
	} else {
		p.From, err = time.Parse(dateFormat, fromStr)
		if err != nil {
			return p, err
		}
	}

	return p, nil
}

func (x *aggregatePhotos) Start(c context.Context) (*datastore.Query, interface{}) {
	x.counts = make(map[int64]int64)

	q := datastore.NewQuery("photo")
	q = q.Filter("taken >=", x.From)
	q = q.Filter("taken <", x.To)
	q = q.Order("taken")
	q = q.Limit(500)
	q = q.KeysOnly()

	// NOTE: we're going a keys_only query so we won't actually load the entity here ...
	return q, nil
}

func (x *aggregatePhotos) Process(c context.Context, key *datastore.Key) {
	// ... instead we have to load it ourselves (but now we can use memcache to reduce costs)
	photo := new(Photo)
	err := nds.Get(c, key, photo)
	if err != nil {
		log.Errorf(c, "get photo error %s", err.Error())
		return
	}
	photo.ID = key.IntID()

	// we could do other datastore lookups here (e.g. if processing orders and
	// looking up photo for line items to aggregate sales by photographer) in
	// which case we should look at creating more of a pipeline with go routines
	// so mutliple operations can overlap
	_, ok := x.counts[photo.Photographer.ID]
	if ok {
		x.counts[photo.Photographer.ID]++
	} else {
		x.counts[photo.Photographer.ID] = 1
	}
}

func (x *aggregatePhotos) Complete(c context.Context) {
	// schedule task to update aggregates etc ...
	for id, count := range x.counts {
		log.Debugf(c, "photographer %d took %d", id, count)
	}
}
