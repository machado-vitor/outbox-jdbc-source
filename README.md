# outbox-jdbc-source

Transactional outbox drained by the Confluent JDBC source connector — and why not to.
`orders` row and `outbox` row are written in one statement; the connector polls the table
every second with `mode=incrementing` on `id`, keeping the highest id it has seen as its
offset. SMTs shape the record (`connector.json`): `ValueToKey` + `ExtractField` make
`aggregate_id` the key, `HeaderFrom` moves `event_id` / `event_type` to headers,
`ExtractField` leaves `payload` as the value. `uuid` and `jsonb` are cast to `text` because
the connector does not map them; `SELECT * FROM (...) o` is needed because the connector
appends `WHERE "id" > ? ORDER BY "id"` to the query string.

```sh
mvn test
```

That is the whole demo. `JdbcSourceOutboxTest` (infrastructure in `Env`) downloads the plugin once (into
`kafka-connect-jdbc/`, gitignored), starts a real Postgres, Kafka and Kafka Connect
(Testcontainers), registers `connector.json`, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | the easy case works: key = `aggregate_id`, headers `event_id` / `event_type`, value = payload. `published_at` stays NULL, the connector never writes |
| `aRowThatBecomesVisibleLateIsLostForever` | tx A takes id *n* but commits after tx B took *n+1*. B is published and `connect_offsets` reads `{"incrementing": n+1}`. A commits; five polls later it is still not on the topic, and nothing will ever ask for it again |

## The bug

`bigserial` hands out ids at `INSERT` time, not at `COMMIT`, so a lower id can become
visible after a higher one. A cursor on `id` assumes otherwise. `mode=timestamp` has the
same hole: `now()` is the transaction *start* time. The connector never writes
`published_at`, so it cannot ask the one question that works (`WHERE published_at IS NULL`).

The suite itself had to work around it: truncating with `RESTART IDENTITY` between tests
recycled id 1 under an offset of 2, and the next test's order silently vanished.

Compared side by side in [outbox-compared](https://github.com/machado-vitor/outbox-compared).
Siblings that get it right: [outbox-relay](https://github.com/machado-vitor/outbox-relay) (no cursor, marks rows) and
[outbox-debezium](https://github.com/machado-vitor/outbox-debezium) (reads the WAL in commit order).
