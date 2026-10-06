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
import java.util.zip.ZipInputStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// The plumbing: real Postgres, real Kafka, real Kafka Connect with the Confluent JDBC
/// source plugin mounted, a consumer parked at the end of the topic. Nothing here is
/// the pattern; the pattern (and its hole) is in JdbcSourceOutboxTest.
class Env {

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

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }

    // --- kafka connect ------------------------------------------------------------

    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final String CONNECTOR = "/connectors/jdbc-connector";

    static void downloadPluginIfMissing() throws Exception {
        if (Files.exists(PLUGIN.resolve("manifest.json"))) return;
        var url = "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/"
                + PLUGIN_VERSION + "/confluentinc-kafka-connect-jdbc-" + PLUGIN_VERSION + ".zip";
        System.out.println("  downloading " + url);
        var res = HTTP.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofInputStream());
        if (res.statusCode() != 200) throw new IllegalStateException("plugin download: HTTP " + res.statusCode());
        try (var zip = new ZipInputStream(res.body())) {
            for (var e = zip.getNextEntry(); e != null; e = zip.getNextEntry()) {
                var rel = e.getName().substring(e.getName().indexOf('/') + 1); // strip the versioned top dir
                if (e.isDirectory() || rel.isEmpty()) continue;
                Files.createDirectories(PLUGIN.resolve(rel).getParent());
                Files.copy(zip, PLUGIN.resolve(rel));
            }
        }
    }

    static void registerConnector() throws Exception {
        var put = request(CONNECTOR + "/config")
                .header("content-type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(Files.readString(Path.of("connector.json")))).build();
        var res = HTTP.send(put, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("register: " + res.body());
        var running = "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"";
        var body = awaitBody(CONNECTOR + "/status", running);
        if (!body.contains(running)) throw new IllegalStateException("task never started: " + body);
    }

    /// The connector's memory, as committed to connect_offsets (flushed every second, see above).
    static String storedOffsets() throws Exception {
        return awaitBody(CONNECTOR + "/offsets", "incrementing");
    }

    /// GET until the body contains `until` (up to 30 s); returns the last body either way.
    static String awaitBody(String path, String until) throws Exception {
        var get = request(path).GET().build();
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String body;
        do {
            Thread.sleep(500);
            body = HTTP.send(get, HttpResponse.BodyHandlers.ofString()).body();
        } while (!body.contains(until) && System.nanoTime() < deadline);
        System.out.println("  connect <- " + body);
        return body;
    }

    static HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path));
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        return scalar(c, """
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
    }

    static String scalar(String sql) throws Exception {
        return scalar(db, sql);
    }

    static String scalar(Connection c, String sql) throws Exception {
        var rs = c.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    // --- reading the topic -----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        // No RESTART IDENTITY: the connector remembers the highest id it saw, and a fresh
        // row with a recycled lower id would be skipped. (Yes, that is the bug, biting the suite.)
        db.createStatement().execute("TRUNCATE orders, outbox");
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
            System.out.printf("  topic <- partition=%d key=%s event_type=%s event_id=%s value=%s%n",
                    r.partition(), r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static List<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).toList();
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
