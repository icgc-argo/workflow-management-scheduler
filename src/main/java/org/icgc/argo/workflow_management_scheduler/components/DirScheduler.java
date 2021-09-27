package org.icgc.argo.workflow_management_scheduler.components;

import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.*;

import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.function.Function;
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
  private final Map<String, Integer> workflowNameToCosts;

  public DirScheduler(DirSchedulerConfig config) {
    this.config = config;
    this.workflowNameToCosts =
        config.getWorkflows().stream()
            .collect(toMap(WorkflowProps::getName, WorkflowProps::getCost));
    log.debug("DirScheduler component created with config: {}", config);
  }

  public ImmutableList<Run> getNextInitializedRuns(ImmutableList<Run> allRuns) {
    val RUN_WAITING_FOR_DIR = "RUN_WAITING_FOR_DIR";
    val ACTIVE_RUN = "ACTIVE_RUN";
    val RUN_READY_FOR_INIT = "RUN_READY_FOR_INIT";

    Function<Run, String> matchRunToSchedulingType =
        (run) -> {
          if (run.isActive()) {
            return ACTIVE_RUN;
          } else if (canBeTemplated(run)) {
            return RUN_WAITING_FOR_DIR;
          } else {
            return RUN_READY_FOR_INIT;
          }
        };
    val runsBySchedulingType = allRuns.stream().collect(groupingBy(matchRunToSchedulingType));

    val runsWaitingForDir = runsBySchedulingType.getOrDefault(RUN_WAITING_FOR_DIR, new ArrayList<>());
    val runReadyForInit = runsBySchedulingType.getOrDefault(RUN_READY_FOR_INIT, new ArrayList<>());
    val activeRuns = runsBySchedulingType.getOrDefault(ACTIVE_RUN, new ArrayList<>());

    if (!runsWaitingForDir.isEmpty()) {
      val scheduledRuns = getNextScheduledRuns(activeRuns, runsWaitingForDir);
      runReadyForInit.addAll(scheduledRuns);
    }

    runReadyForInit.forEach(r -> r.setState(RunState.INITIALIZING));

    return ImmutableList.copyOf(runReadyForInit);
  }

  private List<Run> getNextScheduledRuns(List<Run> activeRuns, List<Run> queuedRuns) {
    val wfNameToQueuedRuns =
        queuedRuns.stream().collect(groupingBy(this::matchRunToKnownWorkflowName));
    val wfNameToActiveRuns =
        activeRuns.stream().collect(groupingBy(this::matchRunToKnownWorkflowName));

    val dirValueToActiveRuns =
        activeRuns.stream().collect(groupingBy(this::matchRunToKnownDirValues));

    // map representing all runs on scheduler dirs
    val dirValueToRuns = new HashMap<>(dirValueToActiveRuns);
    // make sure all dir values are present
    config
        .getDirValues()
        .forEach(dir -> dirValueToRuns.computeIfAbsent(dir, k -> new ArrayList<>()));

    // collect next scheduled runs
    List<Run> scheduledRuns = new ArrayList<>();
    config
        .getWorkflows()
        .forEach(
            wfProp -> {
              val wfName = wfProp.getName();
              val maxRunsAllowedForWf = wfProp.getMaxTotalRuns();

              val queuedRunsForWf = wfNameToQueuedRuns.getOrDefault(wfName, emptyList());
              val activeRunsForWf = wfNameToActiveRuns.getOrDefault(wfName, emptyList());

              // numRuns to Init for wf is either fill up to max or all the queued
              val numRunsToInit =
                  Math.min(maxRunsAllowedForWf - activeRunsForWf.size(), queuedRunsForWf.size());
              if (numRunsToInit <= 0) {
                return;
              }

              val allocatedWorkDirValues =
                  getStreamOfNextSchedulableDirs(dirValueToRuns, wfProp.getCost(), numRunsToInit);

              allocatedWorkDirValues.forEach(
                  value -> {
                    val nextRunToInit = queuedRunsForWf.remove(queuedRunsForWf.size() - 1);
                    // update template params
                    val templatedJson =
                        replaceTemplateWithValue(
                            nextRunToInit.getWorkflowParamsJsonStr(),
                            config.getWorkDirTemplate(),
                            value);
                    nextRunToInit.setWorkflowParamsJsonStr(templatedJson);
                    // set dirs
                    val newWorkDir =
                        replaceTemplateWithValue(
                            nextRunToInit.getWorkflowEngineParams().getWorkDir(),
                            config.getWorkDirTemplate(),
                            value);
                    val newProjectDir =
                        replaceTemplateWithValue(
                            nextRunToInit.getWorkflowEngineParams().getProjectDir(),
                            config.getWorkDirTemplate(),
                            value);
                    val newLaunchDir =
                        replaceTemplateWithValue(
                            nextRunToInit.getWorkflowEngineParams().getLaunchDir(),
                            config.getWorkDirTemplate(),
                            value);
                    nextRunToInit.getWorkflowEngineParams().setWorkDir(newWorkDir);
                    nextRunToInit.getWorkflowEngineParams().setProjectDir(newProjectDir);
                    nextRunToInit.getWorkflowEngineParams().setLaunchDir(newLaunchDir);

                    dirValueToRuns.get(value).add(nextRunToInit);
                    scheduledRuns.add(nextRunToInit);
                  });
            });

    return scheduledRuns;
  }

  private Boolean canBeTemplated(Run run) {
    return run != null
        && run.getWorkflowEngineParams() != null
        && run.isAnyDirParamMatched(config.getWorkDirTemplate())
        && !matchRunToKnownWorkflowName(run).isEmpty();
  }

  private String matchRunToKnownWorkflowName(Run run) {
    return config.getWorkflows().stream()
        .filter(wf -> wf.getRepository().equals(run.getRepository()))
        .map(WorkflowProps::getName)
        .findFirst()
        .orElse("");
  }

  private String matchRunToKnownDirValues(Run run) {
    return config.getDirValues().stream().filter(run::isAnyDirParamMatched).findFirst().orElse("");
  }

  private String replaceTemplateWithValue(
      String input, String templateRegex, String templateValue) {
    if (input != null && input.contains(templateRegex)) {
      return input.replaceAll(templateRegex, templateValue);
    }
    return input;
  }

  private Stream<String> getStreamOfNextSchedulableDirs(
      Map<String, List<Run>> workDirToRunsMap, Integer cost, Integer maxWorkDirsRequested) {
    return workDirToRunsMap.entrySet().stream()
        .flatMap(
            entry -> {
              val dirValue = entry.getKey();
              val runsInDir = entry.getValue();

              val costInDir =
                  runsInDir.stream()
                      .map(this::matchRunToKnownWorkflowName)
                      .map(wfName -> workflowNameToCosts.getOrDefault(wfName, 0))
                      .reduce(0, Integer::sum);

              val availableCost = config.getMaxCostPerDir() - costInDir;
              if (availableCost >= cost) {
                val numOfRunsForWfNameToFitInAvailableCost = availableCost / cost;
                return nCopies(numOfRunsForWfNameToFitInAvailableCost, dirValue).stream();
              }
              return Stream.empty();
            })
        .limit(maxWorkDirsRequested);
  }
}
