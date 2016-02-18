# Go AppEngine Mapper

Example of using Go (Golang) to iterate over entities to process or aggregate them.

Iteration handling is reused so new processor just involves implementing a struct to match the interface.
See aggregate and log for examples.

Could do with some tidfying up around the range / parameter settings and error handling but it works.

## Strategies

The basic idea is that the a query is defined for the collection and slices as required (in this case from / to date range)

The processor defines the batch size to use and the timeout. The timeout should allow for the last batch to be handled.

Entities are processed in batches using a keys only query for efficiency. Only one entity is ever loaded at a time. This
could be speeded up by having a pipeline using goroutines and channels.

Also for performance and atomicity, it could use named tasks to process set batch sizes and schedule a continuation before
processing the entities in a batch. See talks by Brett Slatkin for details of doing that.

## Running

Install go app dependencies:

    go get ./...

Run with dev server:

    goapp serve

### Example requests

Log all entities from 2015 on ...

    http://localhost:8080/_ah/cron/process/logPhotos?from=2015-01-01

Log all entities for Jan 2015 only ...

    http://localhost:8080/_ah/cron/process/logPhotos?from=2015-01-01&to=2015-02-01

Aggregate all entities from 2015 on ...

    http://localhost:8080/_ah/cron/process/aggregatePhotos?from=2015-01-01

## Notes for demo

Default cron task without params is designed to process previous days entries only

Remember to change the app id in app.yaml if deploying

Cron tasks can be restructed for production

Warmup task creates dummy data for demo