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
		photographers := []Photographer {
			{1, "Mr Canon"},
			{2, "Miss Nikon"},
			{3, "Mrs Pentax"},
			{4, "Ms Sony"},
		}

		// create some dummy data
		for m := 1; m <= 12; m++ {
			for d := 1; d < 28; d++ {
				taken := time.Date(2015, time.Month(m), d, 12, 0, 0, 0, time.UTC)
				id := rand.Int31n(4)
				photographer := photographers[id]
				p := Photo{
					Photographer: photographer,
					Uploaded    : time.Now().UTC(),
					Width       : 8000,
					Height      : 6000,
					Taken       : taken,
				}
				k := datastore.NewIncompleteKey(c, "photo", nil)
				nds.Put(c, k, &p)
			}
		}
	}
	return c.NoContent(http.StatusOK)
}
