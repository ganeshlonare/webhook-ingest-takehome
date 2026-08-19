# Solution

## What I found and fixed

The service checked whether an event already existed and then inserted it in separate database operations. Two deliveries of the same webhook could both pass that check, so duplicate events and extra account counts were possible. I added a unique constraint on `events.event_id` and moved the event insert, call update, and account-stat update into one transaction.

Recording work was started with the HTTP request context. The request finishes as soon as we return 200, so the background work could be cancelled before it marked the recording as processed. I gave that work its own timeout context and added an error log. A restart also lost any pending recording work, so on startup the service finds unprocessed recordings and starts them again.

## Why PostgreSQL for deduplication

I considered Redis with `SETNX` and an expiry. It can be fast, but choosing the expiry and handling Redis failures would add another place where duplicate behaviour can be wrong. PostgreSQL already stores the event, call, and account totals. A unique database constraint is durable and works even when two requests arrive at the same time. `ON CONFLICT DO NOTHING` tells the service whether the delivery is new.

## If this handled 10,000 webhooks per second

I would keep the webhook response quick, but put recording processing into a durable queue with a limited number of workers, retries, and dead-letter handling. I would load-test and tune the Postgres connection pool, batch database work where useful, and add metrics for failures and queue backlog. I would also move account stats to a shared cache or rebuild the cache from Postgres after restart, so multiple service instances return the same totals.
