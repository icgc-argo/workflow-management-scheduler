package org.icgc.argo.workflow_management_scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.components.DirScheduler;
import org.icgc.argo.workflow_management_scheduler.config.DirSchedulerConfig;
import org.icgc.argo.workflow_management_scheduler.model.Run;
import org.icgc.argo.workflow_management_scheduler.model.WorkflowProps;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.RunState;
import org.junit.jupiter.api.Test;

public class DirSchedulerTests {
  private static final String ALIGN_NAME = "ALIGN";
  private static final String WGS_NAME = "WGS_NAME";
  private static final String ALIGN_WF_REPO = "org/ALIGN";
  private static final String ALIGN_WF_URL = "http://github.com/org/ALIGN";
  private static final String WGS_SANGER_WF_REPO = "org/WGS_SANGER";
  private static final String WGS_SANGER_WF_URL = "http://github.com/org/WGS_SANGER.git";
  private static final String HELLO_WF_URL = "http://www.github.com/org/HELLO";

  private static final String WORK_DIR_TEMPLATE = "<SCHEDULED_DIR>";
  private static final String WORK_DIR_0 = "/nfs/dir-0";
  private static final String WORK_DIR_1 = "/nfs/dir-1";
  private static final Integer MAX_COST_PER_DIR = 2;

  private static final DirSchedulerConfig config =
      new DirSchedulerConfig(
          "<SCHEDULED_DIR>",
          MAX_COST_PER_DIR,
          ImmutableList.of(WORK_DIR_0, WORK_DIR_1),
          ImmutableList.of(
              new WorkflowProps(ALIGN_NAME, ALIGN_WF_REPO, 2, 2),
              new WorkflowProps(WGS_NAME, WGS_SANGER_WF_REPO, 4, 1)));

  private final DirScheduler dirScheduler = new DirScheduler(config);

  @Test
  void testBasicScheduling() {
    val runs =
        List.of(
            createRun("run-1", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.QUEUED, ALIGN_WF_URL, WORK_DIR_TEMPLATE));
    val initializedRuns = dirScheduler.getNextInitializedRuns(ImmutableList.copyOf(runs));

    val expectedRuns = List.of(createRun("run-2", RunState.INITIALIZING, ALIGN_WF_URL, WORK_DIR_1));

    assertIterableEquals(expectedRuns, initializedRuns);
  }

  @Test
  void testCanScheduleRunsNotNeedingDirs() {
    // currently running leaves no available dirs
    val allRuns =
        ImmutableList.of(
            createRun("run-1", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_1),
            createRun("run-3", RunState.QUEUED, HELLO_WF_URL, "emptyDir"),
            createRun("run-4", RunState.QUEUED, HELLO_WF_URL, null));
    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    // runs that don't need dirs are still initialized
    val expectedInitializedRuns =
        List.of(
            createRun("run-3", RunState.INITIALIZING, HELLO_WF_URL, "emptyDir"),
            createRun("run-4", RunState.INITIALIZING, HELLO_WF_URL, null));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }

  @Test
  void testMaxCostPerDirIsMaintained() {
    // we have one workflow which takes cost of 1 running in a dir which has max cost of 2
    val allRuns =
        ImmutableList.of(
            createRun("run-1", RunState.RUNNING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-3", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-4", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-5", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE));
    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    // expect 4 to be init since queued are cost 1 and we have 1 cost in the first dir plus 2 in the
    // next
    val expectedInitializedRuns =
        List.of(
            createRun("run-3", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-4", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1),
            createRun("run-5", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }

  @Test
  void testSchedulingMultipleTypesOfWorkflow() {
    val allRuns =
        ImmutableList.of(
            createRun("run-1", RunState.RUNNING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.QUEUED, ALIGN_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-3", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-4", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-5", RunState.QUEUED, ALIGN_WF_URL, WORK_DIR_TEMPLATE));

    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    // there is enough room to schedule an align in work_dir_1 and one more sanger in work_dir_0
    val expectedInitializedRuns =
        List.of(
            createRun("run-5", RunState.INITIALIZING, ALIGN_WF_URL, WORK_DIR_1),
            createRun("run-4", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_0));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }

  @Test
  void testSchedulingWhenActiveRunsHaveUnknownDirsAndRepos() {
    val allRuns =
        ImmutableList.of(
            createRun(
                "run-unknown", RunState.RUNNING, "https://github.com/unknown/repo.git", WORK_DIR_1),
            createRun("run-other-sanger", RunState.RUNNING, WGS_SANGER_WF_URL, "/test"),
            createRun("run-1", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-2", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-3", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-4", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE));

    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    // `run-other-sanger` is a wgs_sanger and these workflows are config with max limit of 4
    // which means only 3 new runs should be init.
    // `run-other-unknown` is using WORK_DIR_1 but its not configured so no cost can be associated
    // which is why its is ignored in scheduling
    val expectedInitializedRuns =
        List.of(
            createRun("run-2", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-3", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1),
            createRun("run-4", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }

  Run createRun(String runId, RunState runState, String url, String baseDir) {
    return Run.builder()
        .runId(runId)
        .state(runState)
        .workflowUrl(url)
        .workflowParamsJsonStr("{\"baseDir\": \"" + baseDir + "\"}")
        .workflowEngineParams(
            Run.EngineParams.builder()
                .projectDir(baseDir + "/project/dir/path")
                .launchDir(baseDir + "/launch/dir/path")
                .workDir(baseDir + "/work/dir/path")
                .build())
        .build();
  }
}
