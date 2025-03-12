package com.example;

import static org.apache.flink.table.api.Expressions.*;

import com.example.model.TransactionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.flink.table.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Table API application that processes transaction data from Kafka. Filters out cancelled
 * transactions, transforms data, and writes approved transactions to another Kafka topic.
 */
public class TransactionProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

  // Kafka and Schema Registry configuration
  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final String inputTopic;
  private final String outputTopic;

  // Paths to Avro schema files
  private static final String TRANSACTION_SCHEMA_PATH = "src/main/avro/Transaction.avsc";
  private static final String APPROVED_TRANSACTION_SCHEMA_PATH =
      "src/main/avro/ApprovedTransaction.avsc";

  public TransactionProcessor(
      String bootstrapServers, String schemaRegistryUrl, String inputTopic, String outputTopic) {
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
  }

  /**
   * Sets up and executes the Flink processing pipeline.
   *
   * @return TableResult from the execution
   */
  public TableResult execute() throws Exception {
    LOG.info("Starting Transaction Processor");

    // Set up Table Environment
    TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    // Register source table
    createSourceTable(tableEnv);

    // Register sink table
    createSinkTable(tableEnv);

    // Create and execute the transformation using Table API
    TableResult result = processTransactions(tableEnv);

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
      String transactionSchema = readAvroSchema(TRANSACTION_SCHEMA_PATH);

      tableEnv.createTable(
          "transactions",
          TableDescriptor.forConnector("kafka")
              .schema(
                  Schema.newBuilder()
                      .column("id", DataTypes.STRING().notNull())
                      .column("amount", DataTypes.DOUBLE().notNull())
                      .column("currency", DataTypes.STRING().notNull())
                      .column("timestamp", DataTypes.TIMESTAMP(3).notNull())
                      .column("description", DataTypes.STRING())
                      .column("merchant", DataTypes.STRING().notNull())
                      .column("category", DataTypes.STRING())
                      .column("status", DataTypes.STRING().notNull())
                      .column("userId", DataTypes.STRING().notNull())
                      .column(
                          "metadata",
                          DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING().notNull()))
                      .watermark("timestamp", "`timestamp` - INTERVAL '5' SECOND")
                      .build())
              .option("topic", inputTopic)
              .option("properties.bootstrap.servers", bootstrapServers)
              .option("properties.group.id", "transaction-processor")
              .option("scan.startup.mode", "earliest-offset")
              .option("format", "avro-confluent")
              .option("avro-confluent.url", schemaRegistryUrl)
              .option("avro-confluent.subject", inputTopic + "-value")
              .option("avro-confluent.schema", transactionSchema)
              .build());

      LOG.info("Source table 'transactions' created");
    } catch (IOException e) {
      LOG.error("Failed to read Transaction Avro schema", e);
      throw new RuntimeException("Failed to create source table", e);
    }
  }

  /** Creates the sink table that writes to Kafka. */
  protected void createSinkTable(TableEnvironment tableEnv) {
    try {
      String approvedTransactionSchema = readAvroSchema(APPROVED_TRANSACTION_SCHEMA_PATH);

      tableEnv.createTable(
          "approved_transactions",
          TableDescriptor.forConnector("kafka")
              .schema(
                  Schema.newBuilder()
                      .column("id", DataTypes.STRING().notNull())
                      .column("amount", DataTypes.DOUBLE().notNull())
                      .column("currency", DataTypes.STRING().notNull())
                      .column("timestamp", DataTypes.TIMESTAMP(3).notNull())
                      .column("merchant", DataTypes.STRING().notNull())
                      .column("userId", DataTypes.STRING().notNull())
                      .column("amountInUsd", DataTypes.DOUBLE().notNull())
                      .column("processingTimestamp", DataTypes.TIMESTAMP(3).notNull())
                      .build())
              .option("topic", outputTopic)
              .option("properties.bootstrap.servers", bootstrapServers)
              .option("properties.group.id", "approved-transaction-processor")
              .option("format", "avro-confluent")
              .option("avro-confluent.url", schemaRegistryUrl)
              .option("avro-confluent.subject", outputTopic + "-value")
              .option("avro-confluent.schema", approvedTransactionSchema)
              .build());

      LOG.info("Sink table 'approved_transactions' created");
    } catch (IOException e) {
      LOG.error("Failed to read ApprovedTransaction Avro schema", e);
      throw new RuntimeException("Failed to create sink table", e);
    }
  }

  /**
   * Processes transactions data using Table API. - Filters out cancelled transactions - Selects
   * only specified fields - Performs transformations - Inserts into the sink table
   *
   * @return TableResult from the execution
   */
  protected TableResult processTransactions(TableEnvironment tableEnv) {
    // Get the source table
    Table transactions = tableEnv.from("transactions");

    // Process using Table API
    Table approvedTransactions =
        transactions
            // Filter out cancelled transactions
            .filter($("status").isNotEqual(TransactionStatus.CANCELLED))
            // Select and transform columns
            .select(
                $("id"),
                $("amount"),
                $("currency").as("currency"),
                $("timestamp"),
                $("merchant"),
                $("userId"),
                // Cast/transform data - simple currency conversion example
                ifThenElse(
                        $("currency").isEqual("EUR"),
                        $("amount").times(1.1),
                        ifThenElse(
                                $("currency").isEqual("GBP"),
                                $("amount").times(1.3),
                                $("amount")
                        )
                ).as("amountInUsd"),
                // Add processing timestamp
                currentTimestamp().as("processingTimestamp"));

    // Insert into sink table and return the TableResult
    TableResult result = approvedTransactions.executeInsert("approved_transactions");

    LOG.info("Transaction processing transformation created");

    return result;
  }

  /** Application entry point. */
  public static void main(String[] args) throws Exception {
    String bootstrapServers = "localhost:9092";
    String schemaRegistryUrl = "http://localhost:8081";
    String inputTopic = "transactions";
    String outputTopic = "approved_transactions";

    if (args.length >= 4) {
      bootstrapServers = args[0];
      schemaRegistryUrl = args[1];
      inputTopic = args[2];
      outputTopic = args[3];
    }

    TransactionProcessor processor =
        new TransactionProcessor(bootstrapServers, schemaRegistryUrl, inputTopic, outputTopic);
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
