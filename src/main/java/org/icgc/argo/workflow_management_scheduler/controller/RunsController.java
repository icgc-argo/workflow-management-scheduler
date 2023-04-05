package org.icgc.argo.workflow_management_scheduler.controller;

import com.pivotal.rabbitmq.RabbitEndpointService;
import org.icgc.argo.workflow_management_scheduler.components.DirScheduler;
import org.icgc.argo.workflow_management_scheduler.components.GatekeeperClient;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.SchedulerStreams;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/trigger")
public class RunsController extends SchedulerStreams {

  public RunsController(
      RabbitEndpointService rabbit, GatekeeperClient gatekeeperClient, DirScheduler dirScheduler) {
    super(rabbit, gatekeeperClient, dirScheduler);
  }

  @PostMapping(value = "/reschedule")
  public String triggerSchedulerManually() {
    triggerScheduler();
    return "Runs Triggered Successfully!";
  }
}
