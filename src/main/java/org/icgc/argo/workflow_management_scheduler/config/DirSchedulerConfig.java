package org.icgc.argo.workflow_management_scheduler.config;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.icgc.argo.workflow_management_scheduler.model.WorkflowProps;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "scheduler")
@ToString
public class DirSchedulerConfig {
  String wfParamsTemplate;
  ImmutableList<String> dirValues;
  ImmutableList<WorkflowProps> workflows;

  public void setDirValues(List<String> dirValues) {
    this.dirValues = ImmutableList.copyOf(dirValues);
  }

  public void setWorkflows(List<WorkflowProps> workflows) {
      this.workflows = ImmutableList.copyOf(workflows);
  }
}
