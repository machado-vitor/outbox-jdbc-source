import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

/// Each test writes rows the way the application would (orders + outbox in one
/// statement) and reads the topic back. Infrastructure lives in Env.
class JdbcSourceOutboxTest extends Env {

    /// The easy case works, which is what makes this connector tempting.
    @Test
    void publishesTheOrderEventWithKeyHeadersAndPayload() throws Exception {
        var orderId = insertOrder(db, "cust-1");

        var records = consume(1);
        assertEquals(1, records.size());
        var record = records.getFirst();
        assertEquals(orderId, record.key(), "key is the aggregate id");
        assertEquals("order.created", header(record, "event_type"));
        assertEquals(scalar("SELECT event_id::text FROM outbox"), header(record, "event_id"));
        assertTrue(record.value().contains("\"customer_id\": \"cust-1\""), "value is the payload as written");
        assertNull(scalar("SELECT published_at FROM outbox"), "the connector never touches the table");
    }

    /// Tx A takes id n but commits after tx B took id n+1. The connector sees n+1 first,
    /// stores it as its offset, and from then on only asks for id > n+1. Row n is never published.
    @Test
    void aRowThatBecomesVisibleLateIsLostForever() throws Exception {
        try (var a = connect()) {
            a.setAutoCommit(false);
            var idA = insertOrder(a, "A");   // lower id, not committed: invisible to the connector's SELECT
            var idB = insertOrder(db, "B");  // higher id, committed

            assertEquals(List.of(idB), keys(consume(1)));
            var offset = scalar("SELECT id FROM outbox WHERE aggregate_id = '" + idB + "'");
            assertTrue(storedOffsets().contains("\"incrementing\":" + offset), "offset moved past A's id");

            a.commit();                      // A is visible now, but below the offset

            assertEquals(List.of(idB), keys(drain(Duration.ofSeconds(5))), "five polls later, still no A");
            assertEquals(idA, scalar("SELECT aggregate_id FROM outbox ORDER BY id LIMIT 1"), "A has the lower id");
            assertEquals("1", scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL AND id < " + offset),
                    "A sits in the table, unpublished, with nothing left that will ever ask for it");
        }
    }
}
