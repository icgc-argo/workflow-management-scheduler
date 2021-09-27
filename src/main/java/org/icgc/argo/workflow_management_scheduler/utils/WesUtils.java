package org.icgc.argo.workflow_management_scheduler.utils;

import java.util.Optional;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class WesUtils {
  public static Optional<String> extractRepositoryFromUrl(@NonNull String workflowUrl) {
    return Optional.of(
            workflowUrl
                .replaceAll("((http|https)://)?(www.)?", "")
                .replace("github.com/", "")
                .replace(".git", ""))
        .flatMap(s -> s.isEmpty() ? Optional.empty() : Optional.of(s));
  }
}
