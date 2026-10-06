import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// Real Postgres, real Kafka, real Kafka Connect with the Confluent JDBC source
/// plugin mounted and connector.json registered. Each test writes rows the way
/// the application would (orders + outbox in one statement) and reads the topic back.
class JdbcSourceOutboxTest {

    static final String TOPIC = "order.events";
    static final String PLUGIN_VERSION = "10.9.9";
    static final Path PLUGIN = Path.of("kafka-connect-jdbc"); // downloaded once, gitignored
    static final Network NET = Network.newNetwork();

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withNetwork(NET).withNetworkAliases("postgres")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(NET).withNetworkAliases("kafka").withListener("kafka:19092");

    // The Debezium image is just a convenient Kafka Connect; the JDBC plugin is mounted in.
    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.3.2.Final")
            .withNetwork(NET).dependsOn(POSTGRES, KAFKA)
            .withEnv(Map.of(
                    "BOOTSTRAP_SERVERS", "kafka:19092",
                    "GROUP_ID", "connect",
                    "CONFIG_STORAGE_TOPIC", "connect_configs",
                    "OFFSET_STORAGE_TOPIC", "connect_offsets",
                    "STATUS_STORAGE_TOPIC", "connect_statuses",
                    "OFFSET_FLUSH_INTERVAL_MS", "1000")) // default 60 s; the test wants to read the stored offset
            .withFileSystemBind(PLUGIN.toAbsolutePath().toString(), "/kafka/connect/kafka-connect-jdbc")
            .withExposedPorts(8083)
            .waitingFor(Wait.forHttp("/connectors").forPort(8083));

    static Connection db;
    static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void start() throws Exception {
        downloadPluginIfMissing();
        Startables.deepStart(CONNECT).join();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
        db = connect();
        registerConnector();
    }

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
            long idA = insertOutbox(a, "A");   // lower id, not committed: invisible to the connector's SELECT
            long idB = insertOutbox(db, "B");  // higher id, committed
            assertTrue(idA < idB);

            assertEquals(Set.of("B"), keys(consume(1)));
            assertTrue(storedOffsets().contains("\"incrementing\":" + idB), "offset moved past A's id");

            a.commit();                        // A is visible now, but below the offset

            assertEquals(Set.of("B"), keys(drain(Duration.ofSeconds(5))), "five polls later, still no A");
            assertEquals("A", scalar("SELECT aggregate_id FROM outbox WHERE id = " + idA + " AND published_at IS NULL"),
                    "A sits in the table, unpublished, with nothing left that will ever ask for it");
        }
    }

    // --- kafka connect ------------------------------------------------------------

    static void downloadPluginIfMissing() throws Exception {
        if (Files.exists(PLUGIN.resolve("manifest.json"))) return;
        var url = "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/"
                + PLUGIN_VERSION + "/confluentinc-kafka-connect-jdbc-" + PLUGIN_VERSION + ".zip";
        System.out.println("  downloading " + url);
        var res = http.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, res.statusCode(), "plugin download");
        try (var zip = new ZipInputStream(res.body())) {
            for (var e = zip.getNextEntry(); e != null; e = zip.getNextEntry()) {
                var rel = e.getName().substring(e.getName().indexOf('/') + 1); // strip the versioned top dir
                if (e.isDirectory() || rel.isEmpty()) continue;
                Files.createDirectories(PLUGIN.resolve(rel).getParent());
                Files.copy(zip, PLUGIN.resolve(rel));
            }
        }
    }

    static String connector;

    static void registerConnector() throws Exception {
        connector = "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + "/connectors/jdbc-connector";
        var put = HttpRequest.newBuilder(URI.create(connector + "/config"))
                .header("content-type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(Files.readString(Path.of("connector.json")))).build();
        var res = http.send(put, HttpResponse.BodyHandlers.ofString());
        assertTrue(res.statusCode() / 100 == 2, "register: " + res.body());
        assertTrue(awaitBody(connector + "/status", "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"").contains("RUNNING"),
                "task never started");
    }

    /// The connector's memory, as committed to connect_offsets (flushed every second, see above).
    static String storedOffsets() throws Exception {
        return awaitBody(connector + "/offsets", "incrementing");
    }

    /// GET until the body contains `until` (up to 30 s); returns the last body either way.
    static String awaitBody(String url, String until) throws Exception {
        var get = HttpRequest.newBuilder(URI.create(url)).GET().build();
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String body;
        do {
            Thread.sleep(500);
            body = http.send(get, HttpResponse.BodyHandlers.ofString()).body();
        } while (!body.contains(until) && System.nanoTime() < deadline);
        System.out.println("  connect <- " + body);
        return body;
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        var rs = c.createStatement().executeQuery("""
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
        rs.next();
        return rs.getString(1);
    }

    static long insertOutbox(Connection c, String key) throws Exception {
        var rs = c.createStatement().executeQuery("""
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                VALUES (gen_random_uuid(), '%s', 'order.created', '{}') RETURNING id""".formatted(key));
        rs.next();
        return rs.getLong(1);
    }

    // --- reading both sides ----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        // No RESTART IDENTITY: the connector remembers the highest id it saw, and a fresh
        // row with a recycled lower id would be skipped. (Yes, that is the bug, biting the suite.)
        db.createStatement().execute("SET lock_timeout = '5s'; TRUNCATE orders, outbox");
        received.clear();
        var props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topic = new KafkaConsumer<>(props);
        var partitions = topic.partitionsFor(TOPIC).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        topic.assign(partitions);
        topic.seekToEnd(partitions);
        partitions.forEach(topic::position); // seekToEnd is lazy; force it before the test writes anything
    }

    @AfterEach
    void closeConsumer() {
        topic.close();
    }

    /// Everything published since the test started. Waits up to 15 s for at least
    /// `atLeast` records, then a second more so anything unexpected shows up too.
    static List<ConsumerRecord<String, String>> consume(int atLeast) {
        var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (received.size() < atLeast && System.nanoTime() < deadline) {
            poll();
        }
        return drain(Duration.ofSeconds(1));
    }

    /// Keep reading for a fixed time; used to show that something does NOT arrive.
    static List<ConsumerRecord<String, String>> drain(Duration d) {
        var until = System.nanoTime() + d.toNanos();
        while (System.nanoTime() < until) {
            poll();
        }
        return received;
    }

    static void poll() {
        topic.poll(Duration.ofMillis(200)).forEach(r -> {
            received.add(r);
            System.out.printf("  topic <- key=%s event_type=%s event_id=%s value=%s%n",
                    r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static Set<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    static String scalar(String sql) throws Exception {
        var rs = db.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }
}
