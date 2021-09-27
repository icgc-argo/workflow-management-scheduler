package org.icgc.argo.workflow_management_scheduler;

import static org.icgc.argo.workflow_management_scheduler.utils.WesUtils.extractRepositoryFromUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import lombok.val;
import org.junit.jupiter.api.Test;

public class WesUtilsTests {
  @Test
  void testGetRepositoryPart() {
    val URLS =
        List.of(
            "https://github.com/icgc-argo-workflows/open-access-variant-filtering.git",
            "https://github.com/icgc-argo-workflows/open-access-variant-filtering",
            "https://www.github.com/icgc-argo-workflows/open-access-variant-filtering.git",
            "http://www.github.com/icgc-argo-workflows/open-access-variant-filtering",
            "https://www.github.com/icgc-argo-workflows/open-access-variant-filtering.git",
            "http://www.github.com/icgc-argo-workflows/open-access-variant-filtering",
            "www.github.com/icgc-argo-workflows/open-access-variant-filtering.git",
            "www.github.com/icgc-argo-workflows/open-access-variant-filtering",
            "github.com/icgc-argo-workflows/open-access-variant-filtering.git",
            "github.com/icgc-argo-workflows/open-access-variant-filtering");

    val EXPECTED = "icgc-argo-workflows/open-access-variant-filtering";

    URLS.forEach(
        url -> {
          assertEquals(EXPECTED, extractRepositoryFromUrl(url));
        });
  }
}
