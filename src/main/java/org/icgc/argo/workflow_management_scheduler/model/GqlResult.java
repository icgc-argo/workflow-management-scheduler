package org.icgc.argo.workflow_management_scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class GqlResult {
  public static final RunsSearchResult EMPTY_SEARCH_RESULT =
      new RunsSearchResult(List.of(), new Info(false, 0L, 0));

  GqlData data;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class GqlData {
    RunsSearchResult runs;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RunsSearchResult {
    List<Run> content;
    Info info;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Info {
    Boolean hasNextFrom;
    Long totalHits;
    Integer contentCount;
  }
}
