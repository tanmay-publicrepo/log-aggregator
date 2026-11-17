Building a high throughput log aggregator which can handle more than millions of requests per second with fail safe mechanism in place. This project I believe will help me explore more of current messaging and queuing system.

## Quick start             prerequisites; make up; make gen; make verify; the four UIs with ports
## Pipeline stages         generator, shipper (health, batching), processor (parse, enrich, filter, sample, DLQ)
## Sinks                   a table: sink · topic · connector name · config file · how to verify (OpenSearch count, make pg-count, make s3-ls)
## The custom connector    design: Kafka-coordinate idempotency key, one transaction per put, retry classification, backoff; tests; how to package and deploy
## Operations              make targets table; chaos drills (stop postgres, restart a task); DLQ inspection
## Development             tests, Testcontainers, CI, how to add a connector