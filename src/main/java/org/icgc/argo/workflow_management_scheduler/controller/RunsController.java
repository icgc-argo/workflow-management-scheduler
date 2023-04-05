package org.icgc.argo.workflow_management_scheduler.controller;

import org.icgc.argo.workflow_management_scheduler.rabbitmq.SchedulerStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/trigger")
public class RunsController {

  @Autowired
  SchedulerStreams schedulerStreams;

  @PostMapping(value = "/reschedule")
  public String initializeRuns() {
    schedulerStreams.initializeRuns();
    return "Runs Triggered Successfully!";
  }
}
