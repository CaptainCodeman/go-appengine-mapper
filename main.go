package main

import (
	"net/http"

	"github.com/labstack/echo"
	"google.golang.org/appengine"
)

var (
	web        = createMux()
	ah         = createAh()
	cron       = createCron()
	dateFormat = "2006-01-02"	// yyyy-mm-dd
)

func init() {
	http.Handle("/", web)
}

func createMux() *echo.Echo {
	e := echo.New()
	e.Use(appengineContext())
	return e
}

func createAh() *echo.Group {
	g := web.Group("/_ah")
	return g
}

func createCron() *echo.Group {
	g := ah.Group("/cron")
	// this prevents public access in case app.yaml doesn't have 'admin' restriction
	// g.Use(ensureCronRequest())
	return g
}

func ensureCronRequest() echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if c.Request().Header.Get("X-Appengine-Cron") == "" {
				return echo.NewHTTPError(http.StatusForbidden)
			}
			return h(c)
		}
	}
}

func appengineContext() echo.MiddlewareFunc {
	return func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			c.Context = appengine.NewContext(c.Request())
			return h(c)
		}
	}
}
