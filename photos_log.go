package main

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

type (
	logPhotoProcessor struct {
		Range *photoProcessorRange
		// no other properties required for this
	}
)

func init() {
	registerPhotoProcessor("log", newLogPhotoProcessor)
}

func newLogPhotoProcessor(r *photoProcessorRange) PhotoProcessor {
	r.Size = 20
	r.Timeout = time.Duration(2) * time.Second
	return &logPhotoProcessor{r}
}

func (x *logPhotoProcessor) Start(c context.Context) *photoProcessorRange {
	return x.Range
}

func (x *logPhotoProcessor) Process(c context.Context, photo *Photo) {
	// just log
	log.Debugf(c, "photo %d taken %s by %d %s", photo.ID, photo.Taken.String(), photo.Photographer.ID, photo.Photographer.Name)
}

func (x *logPhotoProcessor) Complete(c context.Context) {
	// nothing to do for this processor
}
