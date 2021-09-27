package org.icgc.argo.workflow_management_scheduler.model;

import static org.icgc.argo.workflow_management_scheduler.utils.WesUtils.extractRepositoryFromUrl;

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

  public String getRepository() {
    return extractRepositoryFromUrl(url);
  }
}
