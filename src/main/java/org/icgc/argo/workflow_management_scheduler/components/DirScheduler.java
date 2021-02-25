package org.icgc.argo.workflow_management_scheduler.components;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.*;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.config.DirSchedulerConfig;
import org.icgc.argo.workflow_management_scheduler.model.Run;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.RunState;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DirScheduler {
  private final DirSchedulerConfig config;

  public DirScheduler(DirSchedulerConfig config) {
    this.config = config;
    log.debug("DirScheduler component created with config: {}", config);
  }

  public ImmutableList<Run> getNextInitializedRuns(ImmutableList<Run> allRuns) {
    List<Run> runsWaitingForDir = new ArrayList<>();
    List<Run> initializedRuns = new ArrayList<>();
    allRuns.stream()
        .filter(Run::isQueued)
        .forEach(
            run -> {
              if (hasDirTemplate(run)) {
                runsWaitingForDir.add(run);
              } else {
                run.setState(RunState.INITIALIZING);
                initializedRuns.add(run);
              }
            });

    val activeRuns =
        allRuns.stream().filter(Run::isNotQueued).collect(toCollection(ArrayList::new));

    val scheduledRuns = runsWaitingForDir.size() > 0 ? getNextScheduledRuns(activeRuns, runsWaitingForDir) : List.<Run>of();

    initializedRuns.addAll(scheduledRuns);

    return ImmutableList.copyOf(initializedRuns);
  }

  private List<Run> getNextScheduledRuns(List<Run> activeRuns, List<Run> queuedRuns) {
    val allDirs = config.getDirValues();
    val workflowProps = config.getWorkflows();

    val wfUrlToQueuedRunsMap = queuedRuns.stream().collect(groupingBy(Run::getWorkflowUrl));
    val wfUrlToActiveRunsMap = activeRuns.stream().collect(groupingBy(Run::getWorkflowUrl));

    // construct map of dir value to runs
    val dirToRunsMap =
        activeRuns.stream()
                .filter(run -> allDirs.contains(run.getWorkflowEngineParams().getWorkDir()))
                .collect(groupingBy(run -> run.getWorkflowEngineParams().getWorkDir()));
    Map<String, List<Run>> updatingDirValuesToRunsMap = new HashMap<>(dirToRunsMap);
    // add any dir values that aren't in use to the map
    allDirs.forEach(
        dir -> {
          if (!updatingDirValuesToRunsMap.containsKey(dir)) {
            updatingDirValuesToRunsMap.put(dir, new ArrayList<>());
          }
        });

    List<Run> initializedRuns = new ArrayList<>();
    workflowProps.forEach(
        prop -> {
          val lookupKey = prop.getUrl();
          val queuedRunsForUrl = wfUrlToQueuedRunsMap.getOrDefault(lookupKey, List.of());
          val activeRunsForUrl = wfUrlToActiveRunsMap.getOrDefault(lookupKey, List.of());

          val numRunsCanBeInit = prop.getMaxTotalRuns() - activeRunsForUrl.size();
          val numRunsToInit = Math.min(numRunsCanBeInit, queuedRunsForUrl.size());
          if (numRunsToInit <= 0) {
            return;
          }

          val allocatedResourceValues =
              getScheduableDirs(
                  updatingDirValuesToRunsMap, prop.getMaxRunsPerDir(), numRunsToInit);

          allocatedResourceValues.forEach(
              value -> {
                val nextRunToInit = queuedRunsForUrl.remove(queuedRunsForUrl.size() - 1);
                // update template params
                nextRunToInit.setWorkflowParamsJsonStr(
                    templateWithValue(
                        nextRunToInit.getWorkflowParamsJsonStr(),
                        config.getWfParamsTemplate(),
                        value));
                // set workdir
                nextRunToInit.getWorkflowEngineParams().setWorkDir(value);
                // set to INIT
                nextRunToInit.setState(RunState.INITIALIZING);

                updatingDirValuesToRunsMap.get(value).add(nextRunToInit);
                initializedRuns.add(nextRunToInit);
              });
        });

    return initializedRuns;
  }

  private Boolean hasDirTemplate(Run run) {
    return run != null
        && run.getWorkflowEngineParams() != null
        && run.getWorkflowEngineParams().getWorkDir() != null
        && run.getWorkflowEngineParams().getWorkDir().equals(config.getWfParamsTemplate());
  }

  private String templateWithValue(String input, String templateRegex, String templateValue) {
    if (input.contains(templateRegex)) {
      return input.replaceAll(templateRegex, templateValue);
    }
    return input;
  }

  private Stream<String> getScheduableDirs(
      Map<String, List<Run>> newResourcesValuesToRunsPart,
      Integer maxPerPartition,
      Integer maxResourcesToGet) {
    return newResourcesValuesToRunsPart.entrySet().stream()
        .flatMap(
            entry -> {
              val numValuesToReturn = maxPerPartition - entry.getValue().size();
              if (numValuesToReturn > 0) {
                return nCopies(numValuesToReturn, entry.getKey()).stream();
              }
              return Stream.of();
            })
        .limit(maxResourcesToGet);
  }
}
