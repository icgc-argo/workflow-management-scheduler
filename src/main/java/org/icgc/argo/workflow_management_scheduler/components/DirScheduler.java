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
import org.icgc.argo.workflow_management_scheduler.model.WorkflowProps;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.RunState;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DirScheduler {
  private final DirSchedulerConfig config;
  private final Map<String, Integer> workflowUrlToCosts;

  public DirScheduler(DirSchedulerConfig config) {
    this.config = config;
    this.workflowUrlToCosts =
        config.getWorkflows().stream()
            .collect(toMap(WorkflowProps::getUrl, WorkflowProps::getCost));
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

    val scheduledRuns =
        runsWaitingForDir.size() > 0
            ? getNextScheduledRuns(activeRuns, runsWaitingForDir)
            : List.<Run>of();

    initializedRuns.addAll(scheduledRuns);

    return ImmutableList.copyOf(initializedRuns);
  }

  private List<Run> getNextScheduledRuns(List<Run> activeRuns, List<Run> queuedRuns) {
    val allDirs = config.getDirValues();
    val workflowProps = config.getWorkflows();

    val wfUrlToQueuedRunsMap = queuedRuns.stream().collect(groupingBy(Run::getWorkflowUrl));
    val wfUrlToActiveRunsMap = activeRuns.stream().collect(groupingBy(Run::getWorkflowUrl));

    // map of current dir values in use to runs
    val dirValueToRunsMap =
        activeRuns.stream()
            .filter(run -> allDirs.contains(run.getWorkflowEngineParams().getWorkDir()))
            .collect(groupingBy(run -> run.getWorkflowEngineParams().getWorkDir()));

    // construct map that will be updated as runs are scheduled on dirs
    Map<String, List<Run>> updatedDirValueToRunsMap = new HashMap<>(dirValueToRunsMap);
    allDirs.forEach(
        dir -> {
          // add any dir values that aren't in use to the map
          if (!updatedDirValueToRunsMap.containsKey(dir)) {
            updatedDirValueToRunsMap.put(dir, new ArrayList<>());
          }
        });

    List<Run> initializedRuns = new ArrayList<>();
    workflowProps.forEach(
        wfProp -> {
          val wfUrl = wfProp.getUrl();
          val maxRunsForWfUrl = wfProp.getMaxTotalRuns();

          val queuedRunsWithWfUrl = wfUrlToQueuedRunsMap.getOrDefault(wfUrl, List.of());
          val activeRunsWithWfUrl = wfUrlToActiveRunsMap.getOrDefault(wfUrl, List.of());

          // numRuns to Init for url is either fill up to max or all the queued
          val numRunsToInit =
              Math.min(maxRunsForWfUrl - activeRunsWithWfUrl.size(), queuedRunsWithWfUrl.size());
          if (numRunsToInit <= 0) {
            return;
          }

          val allocatedWorkDirValues =
              getStreamOfNextSchedulableDirs(
                  updatedDirValueToRunsMap, wfProp.getCost(), numRunsToInit);

          allocatedWorkDirValues.forEach(
              value -> {
                val nextRunToInit = queuedRunsWithWfUrl.remove(queuedRunsWithWfUrl.size() - 1);
                // update template params
                nextRunToInit.setWorkflowParamsJsonStr(
                    replaceTemplateWithValue(
                        nextRunToInit.getWorkflowParamsJsonStr(),
                        config.getWorkDirTemplate(),
                        value));
                // set workdir
                nextRunToInit.getWorkflowEngineParams().setWorkDir(value);
                // set to INIT
                nextRunToInit.setState(RunState.INITIALIZING);

                updatedDirValueToRunsMap.get(value).add(nextRunToInit);
                initializedRuns.add(nextRunToInit);
              });
        });

    return initializedRuns;
  }

  private Boolean hasDirTemplate(Run run) {
    return run != null
        && run.getWorkflowEngineParams() != null
        && run.getWorkflowEngineParams().getWorkDir() != null
        && run.getWorkflowEngineParams().getWorkDir().equals(config.getWorkDirTemplate());
  }

  private String replaceTemplateWithValue(
      String input, String templateRegex, String templateValue) {
    if (input.contains(templateRegex)) {
      return input.replaceAll(templateRegex, templateValue);
    }
    return input;
  }

  private Stream<String> getStreamOfNextSchedulableDirs(
      Map<String, List<Run>> workDirToRunsMap, Integer cost, Integer maxWorkDirsRequested) {
    return workDirToRunsMap.entrySet().stream()
        .flatMap(
            entry -> {
              val dir = entry.getKey();
              val runsInDir = entry.getValue();

              val costInDir =
                  runsInDir.stream()
                      .map(Run::getWorkflowUrl)
                      .map(workflowUrlToCosts::get)
                      .reduce(0, Integer::sum);

              val availableCost = config.getMaxCostPerDir() - costInDir;
              if (availableCost >= cost) {
                val runsInAvailableCost = availableCost / cost;
                return nCopies(runsInAvailableCost, dir).stream();
              }
              return Stream.of();
            })
        .limit(maxWorkDirsRequested);
  }
}
