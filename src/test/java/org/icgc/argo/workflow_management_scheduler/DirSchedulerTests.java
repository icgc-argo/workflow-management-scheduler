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
  private static final String ALIGN_WF_URL = "http://ALIGN";
  private static final String WGS_SANGER_WF_URL = "http://WGS_SANGER";
  private static final String HELLO_WF_URL = "http://HELLO";

  private static final String WORK_DIR_TEMPLATE = "<SCHEDULED_DIR>";
  private static final String WORK_DIR_0 = "/nfs/dir-0";
  private static final String WORK_DIR_1 = "/nfs/dir-1";

  private static final DirSchedulerConfig config =
      new DirSchedulerConfig(
          "<SCHEDULED_DIR>",
          ImmutableList.of(WORK_DIR_0, WORK_DIR_1),
          ImmutableList.of(
              new WorkflowProps(ALIGN_NAME, ALIGN_WF_URL, 2, 1),
              new WorkflowProps(WGS_NAME, WGS_SANGER_WF_URL, 4, 2)));

  private final DirScheduler dirScheduler = new DirScheduler(config);

  @Test
  void testBasicSchedule() {
    val runs =
        List.of(
            createRun("run-1", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.QUEUED, ALIGN_WF_URL, WORK_DIR_TEMPLATE));
    val initializedRuns = dirScheduler.getNextInitializedRuns(ImmutableList.copyOf(runs));

    val expectedRuns = List.of(createRun("run-2", RunState.INITIALIZING, ALIGN_WF_URL, WORK_DIR_1));

    assertIterableEquals(initializedRuns, expectedRuns);
  }

  @Test
  void testMaxRunPerDir() {
    val allRuns =
        ImmutableList.of(
            createRun("run-1", RunState.RUNNING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-2", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-3", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE),
            createRun("run-4", RunState.QUEUED, WGS_SANGER_WF_URL, WORK_DIR_TEMPLATE));
    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    val expectedInitializedRuns =
        List.of(
            createRun("run-2", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_0),
            createRun("run-3", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1),
            createRun("run-4", RunState.INITIALIZING, WGS_SANGER_WF_URL, WORK_DIR_1));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }


  @Test
  void testScheduleRunsNotUsingTemplate() {
    val allRuns =
            ImmutableList.of(
                    createRun("run-1", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_0),
                    createRun("run-2", RunState.RUNNING, ALIGN_WF_URL, WORK_DIR_1),
                    createRun("run-3", RunState.QUEUED, HELLO_WF_URL, "emptyDir"),
                    createRun("run-4", RunState.QUEUED,  HELLO_WF_URL, null));
    val initializedRuns = dirScheduler.getNextInitializedRuns(allRuns);

    val expectedInitializedRuns =
            List.of(
                    createRun("run-3", RunState.INITIALIZING, HELLO_WF_URL, "emptyDir"),
                    createRun("run-4", RunState.INITIALIZING, HELLO_WF_URL, null));

    assertThat(initializedRuns).hasSameElementsAs(expectedInitializedRuns);
  }

  Run createRun(String runId, RunState runState, String url, String workDir) {
    return Run.builder()
        .runId(runId)
        .state(runState)
        .workflowUrl(url)
        .workflowParamsJsonStr("{\"workDir\": \"" + workDir + "\"}")
        .workflowEngineParams(Run.EngineParams.builder().workDir(workDir).build())
        .build();
  }
}
