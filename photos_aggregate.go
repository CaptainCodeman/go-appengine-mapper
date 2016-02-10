package main

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

type (
	aggregatePhotoProcessor struct {
		Range  *photoProcessorRange
		// note non-exported member - we don't need it serialized between tasks
		counts map[int64]int64
	}
)

func init() {
	registerPhotoProcessor("aggregate", newAggregatePhotoProcessor)
}

func newAggregatePhotoProcessor(r *photoProcessorRange) PhotoProcessor {
	r.Size = 1000
	r.Timeout = time.Duration(5) * time.Minute
	return &aggregatePhotoProcessor{Range: r}
}

func (x *aggregatePhotoProcessor) Start(c context.Context) *photoProcessorRange {
	x.counts = make(map[int64]int64)
	return x.Range
}

func (x *aggregatePhotoProcessor) Process(c context.Context, photo *Photo) {
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

func (x *aggregatePhotoProcessor) Complete(c context.Context) {
	// schedule task to update aggregates etc ...
	for id, count := range x.counts {
		log.Debugf(c, "photographer %d took %d", id, count)
	}
}
