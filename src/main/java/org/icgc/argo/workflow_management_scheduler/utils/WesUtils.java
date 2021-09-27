package org.icgc.argo.workflow_management_scheduler.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class WesUtils {
  public static String extractRepositoryFromUrl(@NonNull String workflowUrl) {
    return workflowUrl
        .replaceAll("((http|https)://)?(www.)?", "")
        .replace("github.com/", "")
        .replace(".git", "");
  }
}
