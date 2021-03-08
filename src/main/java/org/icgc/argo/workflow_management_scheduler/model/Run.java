package org.icgc.argo.workflow_management_scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
