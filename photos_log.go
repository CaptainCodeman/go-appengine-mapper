package main

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	logPhotos struct {
		From    time.Time
		To      time.Time

		// note non-exported member - we don't need it serialized between tasks
		photo  *Photo
	}
)

func init() {
	registerProcessor(newLogPhotos)
}

func newLogPhotos(params ParamAdapter) (Processor, error) {
	p := new(logPhotos)
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

func (x *logPhotos) Start(c context.Context) (*datastore.Query, interface{}) {
	// entity instance to be loaded
	x.photo = new(Photo)

	q := datastore.NewQuery("photo")
	q = q.Filter("taken >=", x.From)
	q = q.Filter("taken <", x.To)
	q = q.Order("taken")
	q = q.Limit(100)

	// NOTE: we're doing a full entity query - we need to pass the pointer to our struct to load
	return q, x.photo
}

func (x *logPhotos) Process(c context.Context, key *datastore.Key) {
	// just log it
	log.Debugf(c, "photo %d taken %s by %d %s", key.IntID(), x.photo.Taken.String(), x.photo.Photographer.ID, x.photo.Photographer.Name)
}

func (x *logPhotos) Complete(c context.Context) {
	// nothing to do for this processor
}
