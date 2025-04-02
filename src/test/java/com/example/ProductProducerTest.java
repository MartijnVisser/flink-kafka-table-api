package com.example;

import com.example.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for producing test products to Kafka. This class is used by the main test class to
 * send test data to Kafka.
 */
public class ProductProducerTest {
  private static final Logger LOG = LoggerFactory.getLogger(ProductProducerTest.class);

  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final String topic;
  private final ObjectMapper objectMapper;
  private final Schema avroSchema;
  private final Conversions.DecimalConversion decimalConversion;

  public ProductProducerTest(String bootstrapServers, String schemaRegistryUrl, String topic) {
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.topic = topic;

    // Configure ObjectMapper
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    // Load Avro schema
    this.avroSchema = loadAvroSchema();
    this.decimalConversion = new Conversions.DecimalConversion();
  }

  private Schema loadAvroSchema() {
    try {
      String schemaPath = "src/main/avro/Product.avsc";
      String schemaJson = new String(Files.readAllBytes(Paths.get(schemaPath)));
      return new Schema.Parser().parse(schemaJson);
    } catch (IOException e) {
      LOG.error("Failed to load Avro schema", e);
      throw new RuntimeException("Failed to load Avro schema", e);
    }
  }

  /**
   * Sends a list of products to Kafka.
   *
   * @param products List of products to send
   */
  public void sendProducts(List<Product> products) {
    LOG.info("Sending {} products to Kafka topic: {}", products.size(), topic);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", schemaRegistryUrl);

    try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
      for (Product product : products) {
        try {
          // Create Avro record
          GenericRecord record = createAvroRecord(product);

          // Create producer record
          ProducerRecord<String, GenericRecord> producerRecord =
              new ProducerRecord<>(topic, product.getProductId(), record);

          // Send record
          producer.send(
              producerRecord,
              (metadata, exception) -> {
                if (exception != null) {
                  LOG.error(
                      "Error sending product {}: {}",
                      product.getProductId(),
                      exception.getMessage());
                } else {
                  LOG.info(
                      "Successfully sent product {} to topic {} partition {} offset {}",
                      product.getProductId(),
                      metadata.topic(),
                      metadata.partition(),
                      metadata.offset());
                }
              });
        } catch (Exception e) {
          LOG.error("Error creating Avro record for product: {}", product, e);
        }
      }
    }
  }

  /**
   * Creates an Avro record from a Product object.
   *
   * @param product Product to convert
   * @return Avro record
   */
  private GenericRecord createAvroRecord(Product product) {
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set("product_id", product.getProductId());
    builder.set("price", product.getPrice());
    builder.set("quantity", product.getQuantity());
    builder.set("timestamp", product.getTimestamp().toEpochMilli());
    return builder.build();
  }
}
