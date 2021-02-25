package org.icgc.argo.workflow_management_scheduler.model;

import static org.icgc.argo.workflow_management_scheduler.utils.JacksonUtils.putInJsonNode;
import static org.icgc.argo.workflow_management_scheduler.utils.JacksonUtils.readTree;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.*;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.RunState;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
public class Run {
  @NonNull private String runId;
  @NonNull private String workflowUrl;
  private String workflowType;
  private String workflowTypeVersion;
  private String workflowParamsJsonStr;
  @NonNull private RunState state;
  private EngineParams workflowEngineParams;
  private Long timestamp;

  public Boolean isQueued() {
    return state.equals(RunState.QUEUED);
  }

  public Boolean isNotQueued() {
    return !isQueued();
  }

  public Boolean hasWorkDir() {
    return this.workflowEngineParams.getWorkDir() != null;
  }

  public void putInToWfParamsJsonStr(List<JsonField> fields) {
    val jsonNode = readTree(workflowParamsJsonStr);
    val updatedJsonNode = putInJsonNode(jsonNode, fields);
    this.workflowParamsJsonStr = updatedJsonNode.toPrettyString();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class EngineParams {
    private String defaultContainer;
    private String revision;
    private String resume;
    private String launchDir;
    private String projectDir;
    private String workDir;
    private Boolean latest;
  }
}
