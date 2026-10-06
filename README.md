# outbox-jdbc-source

Transactional outbox drained by the Confluent JDBC source connector — and why not to.
`orders` row and `outbox` row are written in one statement; the connector polls the table
every second with `mode=incrementing` on `id`, keeping the highest id it has seen as its
offset. SMTs shape the record: `ValueToKey` + `ExtractField` make `aggregate_id` the key,
`HeaderFrom` moves `event_id` / `event_type` to headers, `ExtractField` leaves `payload`
as the value. `uuid` and `jsonb` are cast to `text` because the connector does not map
them; `SELECT * FROM (...) o` is needed because the connector appends
`WHERE "id" > ? ORDER BY "id"` to the query string.

```sh
make up            # postgres + kafka + connect, schema, topic
make connector     # downloads the Confluent plugin (once), restarts connect, registers connector.json
make connector-status
make order N=5
make consume
make race          # <-- the bug
```

## The bug

`make race`: transaction A inserts an outbox row and takes `id = n`, then sleeps.
Transaction B inserts, takes `n+1`, commits. A commits 4 s later.

Only `B` reaches the topic. The connector saw `n+1`, stored it as its offset
(`connect_offsets` topic: `{"incrementing": n+1}`), and never asks for `id <= n+1` again.
Row `n` sits in the table with `published_at = NULL` forever; nothing will ever publish it.

`bigserial` hands out ids at `INSERT` time, not at `COMMIT`, so a lower id can become
visible after a higher one. A cursor on `id` assumes otherwise. `mode=timestamp` has the
same hole: `now()` is the transaction *start* time. The connector never writes
`published_at`, so it cannot use the one question that works (`WHERE published_at IS NULL`).

Siblings that get it right: [outbox-relay](../outbox-relay) (no cursor, marks rows) and
[outbox-debezium](../outbox-debezium) (reads the WAL in commit order).
