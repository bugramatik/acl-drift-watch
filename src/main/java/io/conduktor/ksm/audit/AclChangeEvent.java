package io.conduktor.ksm.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.acl.AclBinding;

/**
 * One ACL change that was actually applied to the cluster.
 *
 * <p>The record is immutable and serializes to a flat JSON object so that any audit consumer can
 * read it without sharing this codebase. Field order is fixed by {@link #toJson()} so that the
 * same change always produces the same bytes.
 */
public record AclChangeEvent(
    String action,
    String principal,
    String resourceType,
    String patternType,
    String resourceName,
    String operation,
    String permission,
    String host,
    String timestamp,
    String cycleId) {

  public static final String ADDED = "ADDED";
  public static final String REMOVED = "REMOVED";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Builds an event for an ACL that the reconciler created. */
  public static AclChangeEvent added(AclBinding binding, String cycleId, Instant at) {
    return of(ADDED, binding, cycleId, at);
  }

  /** Builds an event for an ACL that the reconciler deleted. */
  public static AclChangeEvent removed(AclBinding binding, String cycleId, Instant at) {
    return of(REMOVED, binding, cycleId, at);
  }

  private static AclChangeEvent of(String action, AclBinding binding, String cycleId, Instant at) {
    if (binding == null) throw new IllegalArgumentException("binding is required");
    if (cycleId == null || cycleId.isBlank())
      throw new IllegalArgumentException("cycleId is required");
    if (at == null) throw new IllegalArgumentException("timestamp is required");
    return new AclChangeEvent(
        action,
        binding.entry().principal(),
        binding.pattern().resourceType().name(),
        binding.pattern().patternType().name(),
        binding.pattern().name(),
        binding.entry().operation().name(),
        binding.entry().permissionType().name(),
        binding.entry().host(),
        at.toString(),
        cycleId);
  }

  /** The Kafka record key: every change of one principal lands on the same partition. */
  public String key() {
    return principal;
  }

  /** The event as a single-line JSON document. */
  public String toJson() {
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("action", action);
    fields.put("principal", principal);
    fields.put("resourceType", resourceType);
    fields.put("patternType", patternType);
    fields.put("resourceName", resourceName);
    fields.put("operation", operation);
    fields.put("permission", permission);
    fields.put("host", host);
    fields.put("timestamp", timestamp);
    fields.put("cycleId", cycleId);
    try {
      return MAPPER.writeValueAsString(fields);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("ACL change event could not be serialized", e);
    }
  }

  @Override
  public String toString() {
    return action + " " + principal + " " + resourceType + ":" + resourceName + " " + operation;
  }
}
