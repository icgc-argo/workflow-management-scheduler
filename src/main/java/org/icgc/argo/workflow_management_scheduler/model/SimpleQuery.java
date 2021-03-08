package org.icgc.argo.workflow_management_scheduler.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SimpleQuery {
  @JsonProperty("query")
  private String query;

  @JsonProperty("variables")
  private Map<String, Object> variables;
}
