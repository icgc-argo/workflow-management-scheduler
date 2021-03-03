package org.icgc.argo.workflow_management_scheduler.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowProps {
  String name;
  String url;
  Integer maxTotalRuns;
  Integer cost;
}
