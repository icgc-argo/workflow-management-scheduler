/**
 * Autogenerated by Avro
 *
 * <p>DO NOT EDIT DIRECTLY
 */
package org.icgc.argo.workflow_management_scheduler.rabbitmq.schema;

@org.apache.avro.specific.AvroGenerated
public enum RunState implements org.apache.avro.generic.GenericEnumSymbol<RunState> {
  UNKNOWN,
  QUEUED,
  INITIALIZING,
  RUNNING,
  PAUSED,
  CANCELING,
  CANCELED,
  COMPLETE,
  EXECUTOR_ERROR,
  SYSTEM_ERROR;
  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"type\":\"enum\",\"name\":\"RunState\",\"namespace\":\"org.icgc.argo.workflow_management_scheduler.rabbitmq.schema\",\"symbols\":[\"UNKNOWN\",\"QUEUED\",\"INITIALIZING\",\"RUNNING\",\"PAUSED\",\"CANCELING\",\"CANCELED\",\"COMPLETE\",\"EXECUTOR_ERROR\",\"SYSTEM_ERROR\"]}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }
}
