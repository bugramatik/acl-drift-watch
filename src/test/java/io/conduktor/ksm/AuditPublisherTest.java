package io.conduktor.ksm;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import io.conduktor.ksm.audit.AclChangeEvent;
import io.conduktor.ksm.audit.AuditPublisher;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;

class AuditPublisherTest {
  static final Instant NOW = Instant.parse("2026-10-12T09:30:00Z");

  static final class FakeProducer implements Producer<String, String> {
    final List<ProducerRecord<String, String>> sent = new ArrayList<>();
    boolean fail;
    boolean closed;

    public CompletableFuture<RecordMetadata> send(ProducerRecord<String, String> record) {
      if (fail) return CompletableFuture.failedFuture(new IllegalStateException("broker down"));
      sent.add(record);
      return CompletableFuture.completedFuture(
          new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0));
    }

    public void flush() {}

    public void close() {
      closed = true;
    }

    public void close(java.time.Duration timeout) {
      closed = true;
    }

    public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic) {
      return List.of();
    }

    public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>
        metrics() {
      return Map.of();
    }
  }

  static AuditPublisher publisher(FakeProducer producer) {
    return new AuditPublisher(producer, "acl-audit", Clock.fixed(NOW, ZoneOffset.UTC), 2000);
  }

  @Test
  void eventCarriesEveryFieldAndIsKeyedByPrincipal() {
    AclBinding binding = ParserTest.read("orders");
    AclChangeEvent event = AclChangeEvent.added(binding, "cycle-1", NOW);
    assertEquals("User:alice", event.key());
    assertEquals("ADDED", event.action());
    assertEquals("TOPIC", event.resourceType());
    assertEquals("LITERAL", event.patternType());
    assertEquals("orders", event.resourceName());
    assertEquals("READ", event.operation());
    assertEquals("ALLOW", event.permission());
    assertEquals("*", event.host());
    assertEquals("2026-10-12T09:30:00Z", event.timestamp());
    assertEquals("cycle-1", event.cycleId());
    String json = event.toJson();
    assertTrue(json.startsWith("{\"action\":\"ADDED\",\"principal\":\"User:alice\""));
    assertTrue(json.contains("\"cycleId\":\"cycle-1\""));
    assertFalse(json.contains("\n"));
  }

  @Test
  void appliedPlanPublishesOneEventPerChange() {
    var producer = new FakeProducer();
    var plan =
        ReconciliationPlan.between(
            Set.of(ParserTest.read("orders"), ParserTest.read("invoices")),
            Set.of(ParserTest.read("legacy")));
    assertEquals(3, publisher(producer).publish(plan, "cycle-7"));
    assertEquals(3, producer.sent.size());
    assertTrue(producer.sent.stream().allMatch(r -> r.topic().equals("acl-audit")));
    assertTrue(producer.sent.stream().allMatch(r -> r.key().equals("User:alice")));
    assertEquals(
        1, producer.sent.stream().filter(r -> r.value().contains("\"REMOVED\"")).count());
  }

  @Test
  void emptyPlanPublishesNothing() {
    var producer = new FakeProducer();
    assertEquals(0, publisher(producer).publish(ReconciliationPlan.between(Set.of(), Set.of()), "c"));
    assertTrue(producer.sent.isEmpty());
  }

  @Test
  void producerFailureDoesNotThrowAndCloseFlushes() {
    var producer = new FakeProducer();
    producer.fail = true;
    var publisher = publisher(producer);
    assertEquals(0, publisher.publish(ReconciliationPlan.between(Set.of(ParserTest.read("a")), Set.of()), "c"));
    publisher.close();
    assertTrue(producer.closed);
  }

  @Test
  void disabledConfigBuildsNoPublisher() {
    var config =
        ConfigFactory.parseString("enabled=false\ntopic=\"acl-audit\"\nbootstrap.servers=\"\"");
    assertNull(AuditPublisher.fromConfig(config));
  }
}
