package org.icgc.argo.workflow_management_scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;
import java.util.function.Function;
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

  public void transformEngineParamDirs(Function<String, String> transformer) {
    if (Objects.nonNull(workflowEngineParams.getWorkDir())) {
      workflowEngineParams.setWorkDir(transformer.apply(workflowEngineParams.getWorkDir()));
    }
    if (Objects.nonNull(workflowEngineParams.getLaunchDir())) {
      workflowEngineParams.setLaunchDir(transformer.apply(workflowEngineParams.getLaunchDir()));
    }
    if (Objects.nonNull(workflowEngineParams.getProjectDir())) {
      workflowEngineParams.setProjectDir(transformer.apply(workflowEngineParams.getProjectDir()));
    }
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
