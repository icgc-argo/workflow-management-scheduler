package org.icgc.argo.workflow_management_scheduler.model;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JsonField {
  String path;
  Object value;
}
