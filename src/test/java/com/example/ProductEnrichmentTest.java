package com.example;

import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Product;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Integration test for the ProductEnrichmentProcessor using testcontainers for MySQL and Kafka. */
public class ProductEnrichmentTest {
  private static final Logger LOG = LoggerFactory.getLogger(ProductEnrichmentTest.class);

  private static final Network NETWORK = Network.newNetwork();
  private static final String MYSQL_IMAGE = "mysql:8.4";
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.0";
  private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.0";

  private static MySQLContainer<?> MYSQL;
  private static KafkaContainer KAFKA;
  private static GenericContainer<?> SCHEMA_REGISTRY;

  private static final String INPUT_TOPIC = "products";
  private static final String OUTPUT_TOPIC = "enriched_products";

  @BeforeAll
  public static void setUp() {
    LOG.info("Starting test containers");

    // Start MySQL container
    MYSQL =
        new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("mysql")
            .withExposedPorts(3306)
            .withDatabaseName("product_db")
            .withUsername("root")
            .withPassword("root")
            .waitingFor(Wait.forListeningPort());

    // Start MySQL first
    MYSQL.start();

    // Start Kafka container
    KAFKA =
        new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withExposedPorts(9092, 9093)
            .waitingFor(Wait.forListeningPort());

    // Start Kafka
    KAFKA.start();

    // Start Schema Registry container
    SCHEMA_REGISTRY =
        new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            // Disable Schema Registry compatibility check due to
            // https://issues.apache.org/jira/browse/FLINK-33045 and
            // https://issues.apache.org/jira/browse/FLINK-36650
            // Appears to be because the namespace + name doesn't match with what Flink would use
            .withEnv("SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL", "NONE")
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    // Start Schema Registry
    SCHEMA_REGISTRY.start();

    // Create the products table in MySQL
    try {
      MYSQL.execInContainer(
          "mysql",
          "-u",
          "root",
          "-proot",
          "product_db",
          "-e",
          "CREATE TABLE products (product_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), brand VARCHAR(100), vendor VARCHAR(100), department VARCHAR(100))");
    } catch (Exception e) {
      LOG.error("Failed to create products table", e);
      throw new RuntimeException("Failed to create products table", e);
    }

    // Insert test data into MySQL
    try {
      MYSQL.execInContainer(
          "mysql",
          "-u",
          "root",
          "-proot",
          "product_db",
          "-e",
          "INSERT INTO products VALUES ('PROD001', 'Test Product 1', 'Test Brand', 'Test Vendor', 'Test Department')");
      MYSQL.execInContainer(
          "mysql",
          "-u",
          "root",
          "-proot",
          "product_db",
          "-e",
          "INSERT INTO products VALUES ('PROD002', 'Test Product 2', 'Test Brand', 'Test Vendor', 'Test Department')");
      MYSQL.execInContainer(
          "mysql",
          "-u",
          "root",
          "-proot",
          "product_db",
          "-e",
          "INSERT INTO products VALUES ('PROD003', 'Test Product 3', 'Test Brand', 'Test Vendor', 'Test Department')");
    } catch (Exception e) {
      LOG.error("Failed to insert test data", e);
      throw new RuntimeException("Failed to insert test data", e);
    }

    LOG.info("Test containers setup completed");
  }

  @AfterAll
  public static void tearDown() {
    LOG.info("Stopping test containers");

    if (SCHEMA_REGISTRY != null) {
      SCHEMA_REGISTRY.stop();
    }
    if (KAFKA != null) {
      KAFKA.stop();
    }
    if (MYSQL != null) {
      MYSQL.stop();
    }

    NETWORK.close();
    LOG.info("Test containers torn down");
  }

  @Test
  public void testProductEnrichment() throws Exception {
    // Create test products
    List<Product> inputProducts = createTestProducts();
    LOG.info("Created {} test products", inputProducts.size());

    // Send products to Kafka
    sendProductsToKafka(inputProducts);
    LOG.info("Sent products to Kafka");

    // Set up Table Environment
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    // Create a ProductEnrichmentProcessor instance
    ProductEnrichmentProcessor processor =
        new ProductEnrichmentProcessor(
            KAFKA.getBootstrapServers(),
            "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081),
            INPUT_TOPIC,
            OUTPUT_TOPIC,
            MYSQL.getHost(),
            MYSQL.getMappedPort(3306),
            MYSQL.getDatabaseName(),
            MYSQL.getUsername(),
            MYSQL.getPassword());

    // Use the protected methods directly
    processor.createSourceTable(tableEnv);
    processor.createSinkTable(tableEnv);
    processor.registerLookupFunction(tableEnv);

    // Store the TableResult from processProducts
    TableResult tableResult = processor.processProducts(tableEnv);

    LOG.info("Test setup completed - tables and transformations created");

    // Wait a moment for processing to complete, then cancel the job
    try {
      LOG.info("Waiting 5 seconds before cancelling the job");
      Thread.sleep(5000);

      // Cancel the job
      tableResult
          .getJobClient()
          .ifPresent(
              jobClient -> {
                try {
                  LOG.info("Cancelling Flink job");
                  jobClient.cancel().get();
                  LOG.info("Flink job cancelled successfully");
                } catch (Exception e) {
                  LOG.error("Error cancelling Flink job", e);
                }
              });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting to cancel job", e);
    }

    // Query the results from the output table with a timeout
    List<Row> results = queryResults(tableEnv);
    LOG.info("Retrieved {} results from output table", results.size());

    // Verify we have the expected number of results
    assertEquals(inputProducts.size(), results.size(), "Should have processed all input products");

    // Create a map of input products by ID for easier lookup
    Map<String, Product> inputProductsById = new HashMap<>();
    for (Product product : inputProducts) {
      inputProductsById.put(product.getProductId(), product);
    }

    // Verify each result row
    for (Row result : results) {
      String productId = result.getFieldAs("product_id");
      Product inputProduct = inputProductsById.get(productId);
      assertNotNull(inputProduct, "Should find matching input product for ID: " + productId);

      // Verify the enriched fields
      assertEquals("Test Product " + productId.substring(4), result.getFieldAs("name"));
      assertEquals("Test Brand", result.getFieldAs("brand"));
      assertEquals("Test Vendor", result.getFieldAs("vendor"));
      assertEquals("Test Department", result.getFieldAs("department"));
    }

    LOG.info("Test completed successfully");
  }

  private List<Product> createTestProducts() {
    List<Product> products = new ArrayList<>();
    products.add(
        Product.newBuilder()
            .setProductId("PROD001")
            .setPrice(ByteBuffer.wrap(new BigDecimal("29.99").unscaledValue().toByteArray()))
            .setQuantity(100)
            .setTimestamp(Instant.now())
            .build());
    products.add(
        Product.newBuilder()
            .setProductId("PROD002")
            .setPrice(ByteBuffer.wrap(new BigDecimal("49.99").unscaledValue().toByteArray()))
            .setQuantity(50)
            .setTimestamp(Instant.now())
            .build());
    products.add(
        Product.newBuilder()
            .setProductId("PROD003")
            .setPrice(ByteBuffer.wrap(new BigDecimal("99.99").unscaledValue().toByteArray()))
            .setQuantity(25)
            .setTimestamp(Instant.now())
            .build());
    return products;
  }

  private void sendProductsToKafka(List<Product> products) {
    ProductProducerTest producer =
        new ProductProducerTest(
            KAFKA.getBootstrapServers(),
            "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081),
            INPUT_TOPIC);

    producer.sendProducts(products);
  }

  private List<Row> queryResults(TableEnvironment tableEnv) {
    // Query the results with a bounded query that will complete
    TableResult tableResult =
        tableEnv.executeSql(
            "SELECT * FROM enriched_products /*+ OPTIONS('scan.startup.mode'='earliest-offset', 'scan.bounded.mode'='latest-offset') */");

    // Set a timeout for collecting results to prevent hanging
    long timeoutMs = 10000; // 10 seconds timeout
    long startTime = System.currentTimeMillis();

    // Collect the results
    List<Row> results = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tableResult.collect()) {
      while (iterator.hasNext() && (System.currentTimeMillis() - startTime < timeoutMs)) {
        results.add(iterator.next());
      }

      // If we timed out, log a message
      if (System.currentTimeMillis() - startTime >= timeoutMs) {
        LOG.info(
            "Query collection timed out after {} ms, collected {} results",
            timeoutMs,
            results.size());
      }
    } catch (Exception e) {
      LOG.error("Error collecting results", e);
    }

    return results;
  }
}
