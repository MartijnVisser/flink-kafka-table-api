package com.example;

import static org.apache.flink.table.api.Expressions.*;

import com.example.lookup.MySqlAsyncLookupFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Table API application that processes product data from Kafka, enriches it with MySQL data,
 * and writes the enriched data back to Kafka.
 */
public class ProductEnrichmentProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ProductEnrichmentProcessor.class);

  // Kafka and Schema Registry configuration
  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final String inputTopic;
  private final String outputTopic;

  // MySQL configuration
  private final String mysqlHost;
  private final int mysqlPort;
  private final String mysqlDatabase;
  private final String mysqlUsername;
  private final String mysqlPassword;

  // Paths to Avro schema files
  private static final String PRODUCT_SCHEMA_PATH = "src/main/avro/Product.avsc";
  private static final String ENRICHED_PRODUCT_SCHEMA_PATH = "src/main/avro/EnrichedProduct.avsc";

  public ProductEnrichmentProcessor(
      String bootstrapServers,
      String schemaRegistryUrl,
      String inputTopic,
      String outputTopic,
      String mysqlHost,
      int mysqlPort,
      String mysqlDatabase,
      String mysqlUsername,
      String mysqlPassword) {
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.mysqlHost = mysqlHost;
    this.mysqlPort = mysqlPort;
    this.mysqlDatabase = mysqlDatabase;
    this.mysqlUsername = mysqlUsername;
    this.mysqlPassword = mysqlPassword;
  }

  /**
   * Sets up and executes the Flink processing pipeline.
   *
   * @return TableResult from the execution
   */
  public TableResult execute() throws Exception {
    LOG.info("Starting Product Enrichment Processor");

    // Set up Table Environment
    TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    // Register source table
    createSourceTable(tableEnv);

    // Register sink table
    createSinkTable(tableEnv);

    // Register the lookup function
    registerLookupFunction(tableEnv);

    // Create and execute the transformation using Table API
    TableResult result = processProducts(tableEnv);

    LOG.info("Executing Flink job");

    return result;
  }

  /**
   * Reads the content of an Avro schema file and formats it for Flink's Avro connector.
   *
   * @param schemaPath Path to the Avro schema file
   * @return The schema as a properly formatted string
   * @throws IOException If the file cannot be read or parsed
   */
  private String readAvroSchema(String schemaPath) throws IOException {
    // Read the raw schema file
    String rawSchema = new String(Files.readAllBytes(Paths.get(schemaPath)));

    // Parse it as a JSON object to validate and normalize it
    ObjectMapper objectMapper = new ObjectMapper();
    Object jsonSchema = objectMapper.readValue(rawSchema, Object.class);

    // Convert back to a compact string representation
    return objectMapper.writeValueAsString(jsonSchema);
  }

  /** Creates the source table that reads from Kafka. */
  protected void createSourceTable(TableEnvironment tableEnv) {
    try {
      String productSchema = readAvroSchema(PRODUCT_SCHEMA_PATH);

      tableEnv.createTable(
          "products",
          TableDescriptor.forConnector("kafka")
              .schema(
                  Schema.newBuilder()
                      .column("product_id", DataTypes.STRING().notNull())
                      .column("price", DataTypes.DECIMAL(10, 2).notNull())
                      .column("quantity", DataTypes.INT().notNull())
                      .column("timestamp", DataTypes.TIMESTAMP(3).notNull())
                      .watermark("timestamp", "`timestamp` - INTERVAL '5' SECOND")
                      .build())
              .option("topic", inputTopic)
              .option("properties.bootstrap.servers", bootstrapServers)
              .option("properties.group.id", "product-enrichment-processor")
              .option("scan.startup.mode", "earliest-offset")
              .option("format", "avro-confluent")
              .option("avro-confluent.url", schemaRegistryUrl)
              .option("avro-confluent.subject", inputTopic + "-value")
              .option("avro-confluent.schema", productSchema)
              .build());

      LOG.info("Source table 'products' created");
    } catch (IOException e) {
      LOG.error("Failed to read Product Avro schema", e);
      throw new RuntimeException("Failed to create source table", e);
    }
  }

  /** Creates the sink table that writes to Kafka. */
  protected void createSinkTable(TableEnvironment tableEnv) {
    try {
      String enrichedProductSchema = readAvroSchema(ENRICHED_PRODUCT_SCHEMA_PATH);

      tableEnv.createTable(
          "enriched_products",
          TableDescriptor.forConnector("kafka")
              .schema(
                  Schema.newBuilder()
                      .column("product_id", DataTypes.STRING().notNull())
                      .column("price", DataTypes.DECIMAL(10, 2).notNull())
                      .column("quantity", DataTypes.INT().notNull())
                      .column("timestamp", DataTypes.TIMESTAMP(3).notNull())
                      .column("name", DataTypes.STRING().notNull())
                      .column("brand", DataTypes.STRING().notNull())
                      .column("vendor", DataTypes.STRING().notNull())
                      .column("department", DataTypes.STRING().notNull())
                      .build())
              .option("topic", outputTopic)
              .option("properties.bootstrap.servers", bootstrapServers)
              .option("properties.group.id", "enriched-product-processor")
              .option("format", "avro-confluent")
              .option("avro-confluent.url", schemaRegistryUrl)
              .option("avro-confluent.subject", outputTopic + "-value")
              .option("avro-confluent.schema", enrichedProductSchema)
              .build());

      LOG.info("Sink table 'enriched_products' created");
    } catch (IOException e) {
      LOG.error("Failed to read EnrichedProduct Avro schema", e);
      throw new RuntimeException("Failed to create sink table", e);
    }
  }

  /** Registers the async lookup function for MySQL lookups. */
  protected void registerLookupFunction(TableEnvironment tableEnv) {
    AsyncTableFunction<Row> lookupFunction =
        new MySqlAsyncLookupFunction(
            mysqlHost,
            mysqlPort,
            mysqlDatabase,
            mysqlUsername,
            mysqlPassword,
            "products",
            "product_id",
            new String[] {"name", "brand", "vendor", "department"});

    tableEnv.createTemporaryFunction("lookup_product", lookupFunction);
    LOG.info("Registered async lookup function 'lookup_product'");
  }

  /**
   * Processes products data using Table API with async lookups. - Performs async lookups to MySQL -
   * Enriches the data with product details - Inserts into the sink table
   *
   * @return TableResult from the execution
   */
  protected TableResult processProducts(TableEnvironment tableEnv) {
    // Get the source table
    Table products = tableEnv.from("products");

    // Process using Table API with async lookup
    Table enrichedProducts =
        products
            .renameColumns($("product_id").as("source_product_id"))
            .joinLateral(call("lookup_product", $("source_product_id")).as("lookup"))
            .select(
                $("source_product_id").as("product_id"),
                $("price"),
                $("quantity"),
                $("timestamp"),
                $("lookup.name"),
                $("lookup.brand"),
                $("lookup.vendor"),
                $("lookup.department"));

    // Insert into sink table and return the TableResult
    TableResult result = enrichedProducts.executeInsert("enriched_products");

    LOG.info("Product enrichment transformation created");

    return result;
  }

  /** Application entry point. */
  public static void main(String[] args) throws Exception {
    String bootstrapServers = "localhost:9092";
    String schemaRegistryUrl = "http://localhost:8081";
    String inputTopic = "products";
    String outputTopic = "enriched_products";
    String mysqlHost = "localhost";
    int mysqlPort = 3306;
    String mysqlDatabase = "product_db";
    String mysqlUsername = "root";
    String mysqlPassword = "root";

    if (args.length >= 9) {
      bootstrapServers = args[0];
      schemaRegistryUrl = args[1];
      inputTopic = args[2];
      outputTopic = args[3];
      mysqlHost = args[4];
      mysqlPort = Integer.parseInt(args[5]);
      mysqlDatabase = args[6];
      mysqlUsername = args[7];
      mysqlPassword = args[8];
    }

    ProductEnrichmentProcessor processor =
        new ProductEnrichmentProcessor(
            bootstrapServers,
            schemaRegistryUrl,
            inputTopic,
            outputTopic,
            mysqlHost,
            mysqlPort,
            mysqlDatabase,
            mysqlUsername,
            mysqlPassword);
    TableResult result = processor.execute();

    // Wait for the job to finish (this will block indefinitely for streaming jobs)
    // In production, you might want to handle this differently
    try {
      result.await();
    } catch (Exception e) {
      LOG.error("Error while waiting for job completion", e);
    }
  }
}
