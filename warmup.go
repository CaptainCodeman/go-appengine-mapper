package main

import (
	"time"

	"net/http"
	"math/rand"

	"github.com/labstack/echo"
	"github.com/qedus/nds"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func init() {
	ah.Get("/warmup", warmupHandler)
}

func warmupHandler(c *echo.Context) error {
	if appengine.IsDevAppServer() {
		k := datastore.NewKey(c, "photo", "", 1, nil)
		p := new(Photo)
		err := nds.Get(c, k, p)
		if err != datastore.ErrNoSuchEntity {
			return c.NoContent(http.StatusOK)
		}

		photographers := []Photographer {
			{1, "Mr Canon"},
			{2, "Miss Nikon"},
			{3, "Mrs Pentax"},
			{4, "Ms Sony"},
		}

		// create some dummy data
		var id int64
		for m := 1; m <= 12; m++ {
			for d := 1; d < 28; d++ {
				taken := time.Date(2015, time.Month(m), d, 12, 0, 0, 0, time.UTC)
				photographer := photographers[rand.Int31n(4)]
				p = &Photo{
					Photographer: photographer,
					Uploaded    : time.Now().UTC(),
					Width       : 8000,
					Height      : 6000,
					Taken       : taken,
				}
				id++
				k = datastore.NewKey(c, "photo", "", id, nil)
				nds.Put(c, k, p)
			}
		}
	}
	return c.NoContent(http.StatusOK)
}
