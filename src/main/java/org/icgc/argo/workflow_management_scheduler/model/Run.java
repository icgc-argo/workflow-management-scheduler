package org.icgc.argo.workflow_management_scheduler.model;

import static org.icgc.argo.workflow_management_scheduler.utils.WesUtils.extractRepositoryFromUrl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Optional;
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

  public Boolean isActive() {
    return state.equals(RunState.INITIALIZING)
        || state.equals(RunState.RUNNING)
        || state.equals(RunState.CANCELING);
  }

  public Boolean isAnyDirParamMatched(@NonNull String startsWithStr) {
    return (workflowEngineParams.getWorkDir() != null
            && workflowEngineParams.getWorkDir().startsWith(startsWithStr))
        || (workflowEngineParams.getLaunchDir() != null
            && workflowEngineParams.getLaunchDir().startsWith(startsWithStr))
        || (workflowEngineParams.getProjectDir() != null
            && workflowEngineParams.getProjectDir().startsWith(startsWithStr));
  }

  public Optional<String> getRepository() {
    return extractRepositoryFromUrl(this.getWorkflowUrl());
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
