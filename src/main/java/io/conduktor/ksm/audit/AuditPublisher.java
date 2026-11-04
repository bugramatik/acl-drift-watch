package io.conduktor.ksm.audit;

import com.typesafe.config.Config;
import io.conduktor.ksm.ReconciliationPlan;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes one JSON event per applied ACL change to a Kafka topic.
 *
 * <p>The publisher is only ever called after the Admin API call returned, so an event means the
 * change reached the cluster. Dry runs and failed cycles never reach this class. A publish failure
 * is logged and swallowed: the ACLs are already applied and the next cycle recomputes from Kafka.
 */
public final class AuditPublisher implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AuditPublisher.class);

  private final Producer<String, String> producer;
  private final String topic;
  private final Clock clock;
  private final long deliveryTimeoutMs;

  AuditPublisher(Producer<String, String> producer, String topic, Clock clock, long timeoutMs) {
    this.producer = producer;
    this.topic = topic;
    this.clock = clock;
    this.deliveryTimeoutMs = timeoutMs;
  }

  /** Builds a publisher from the {@code audit.*} section of the configuration. */
  public static AuditPublisher fromConfig(Config audit) {
    if (!audit.getBoolean("enabled")) return null;
    String topic = audit.getString("topic");
    if (topic.isBlank()) throw new IllegalArgumentException("audit.topic must not be blank");
    Map<String, Object> client = new HashMap<>();
    if (audit.hasPath("producer")) client.putAll(audit.getConfig("producer").root().unwrapped());
    Properties properties = new Properties();
    client.forEach((k, v) -> properties.put(k, String.valueOf(v)));
    if (!properties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String servers = audit.getString("bootstrap.servers");
      if (servers.isBlank())
        throw new IllegalArgumentException(
            "Set audit.bootstrap.servers or audit.producer.\"bootstrap.servers\"");
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    }
    properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    properties.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    properties.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "5");
    properties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "acl-drift-watch-audit");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    long timeout = audit.getLong("delivery.timeout.ms");
    if (timeout < 1) throw new IllegalArgumentException("audit.delivery.timeout.ms must be positive");
    properties.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(timeout));
    return new AuditPublisher(
        new KafkaProducer<>(properties), topic, Clock.systemUTC(), timeout);
  }

  /**
   * Publishes one event per added and removed ACL of an applied plan.
   *
   * @return the number of events handed to the producer
   */
  public int publish(ReconciliationPlan plan, String cycleId) {
    if (plan == null || plan.isEmpty()) return 0;
    Instant at = clock.instant();
    List<AclChangeEvent> events = new ArrayList<>(plan.added().size() + plan.removed().size());
    for (AclBinding binding : plan.added()) events.add(AclChangeEvent.added(binding, cycleId, at));
    for (AclBinding binding : plan.removed())
      events.add(AclChangeEvent.removed(binding, cycleId, at));
    int sent = 0;
    for (AclChangeEvent event : events) {
      try {
        producer
            .send(new ProducerRecord<>(topic, event.key(), event.toJson()))
            .get(deliveryTimeoutMs, TimeUnit.MILLISECONDS);
        sent++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Audit publish interrupted after {} of {} events", sent, events.size());
        return sent;
      } catch (Exception e) {
        LOG.warn(
            "Audit event for {} could not be published ({}); ACLs remain applied",
            event,
            e.getClass().getSimpleName());
      }
    }
    LOG.info("Published {} audit events for cycle {}", sent, cycleId);
    return sent;
  }

  /** A fresh identifier for one reconciliation cycle. */
  public static String newCycleId() {
    return UUID.randomUUID().toString();
  }

  @Override
  public void close() {
    try {
      producer.flush();
    } catch (Exception e) {
      LOG.warn("Audit producer flush failed ({})", e.getClass().getSimpleName());
    } finally {
      producer.close(java.time.Duration.ofSeconds(5));
    }
  }
}
