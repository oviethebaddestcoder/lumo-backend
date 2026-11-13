# lumo-contracts

The wire contract between services: Kafka topic names, the event envelope,
event types and payloads (`events`), and WebSocket message types (`realtime`).
No dependencies, no logic.

Rules:
- Additive changes only. Never rename or retype a field.
- A breaking change is a new event type (or `Version`), published alongside the old one until every consumer moves.
- Every event is keyed so events about one entity stay ordered (see `events/topics.go`).

Topics: see `events/topics.go` (and `lumo-infra/kafka/topics.yaml`, generated from it).
