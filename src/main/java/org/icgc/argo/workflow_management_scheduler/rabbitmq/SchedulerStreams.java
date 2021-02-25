package org.icgc.argo.workflow_management_scheduler.rabbitmq;

import static org.icgc.argo.workflow_management_scheduler.utils.RabbitmqUtils.createTransConsumerStream;
import static org.icgc.argo.workflow_management_scheduler.utils.RabbitmqUtils.createTransProducerStream;

import com.google.common.collect.ImmutableList;
import com.pivotal.rabbitmq.RabbitEndpointService;
import com.pivotal.rabbitmq.source.OnDemandSource;
import com.pivotal.rabbitmq.stream.Transaction;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.components.DirScheduler;
import org.icgc.argo.workflow_management_scheduler.components.GatekeeperClient;
import org.icgc.argo.workflow_management_scheduler.model.Run;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.EngineParams;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.RunState;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.schema.WfMgmtRunMsg;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SchedulerStreams {
  @Value("${scheduler.producer.topology.queueName}")
  private String producerQueueName;

  @Value("${scheduler.producer.topology.topicExchangeName}")
  private String producerTopicExchangeName;

  @Value("${scheduler.consumer.topology.queueName}")
  private String consumerQueueName;

  @Value("${scheduler.consumer.topology.topicExchangeName}")
  private String consumerTopicExchangeName;

  @Value("${scheduler.consumer.topology.bufferDurationSec}")
  private Long bufferDurationSec;

  private final OnDemandSource<WfMgmtRunMsg> sourceSink = new OnDemandSource<>("sourceSink");

  private final RabbitEndpointService rabbit;
  private final GatekeeperClient gatekeeperClient;
  private final DirScheduler dirScheduler;

  private static final Set<RunState> ACTION_ON_STATES =
      Set.of(
          RunState.QUEUED,
          RunState.COMPLETE,
          RunState.CANCELED,
          RunState.SYSTEM_ERROR,
          RunState.EXECUTOR_ERROR);

  @Bean
  public Disposable schedulerProducer() {
    val routingKeys = "#";
    return createTransProducerStream(
            rabbit, producerTopicExchangeName, producerQueueName, routingKeys)
        .send(sourceSink.source())
        .subscribe(
            tx -> {
              log.info("Sent: {}", tx.get());
              tx.commit();
            });
  }

  @Bean
  public Disposable consumerStream() {
    val routingKeys = ACTION_ON_STATES.stream().map(RunState::toString).toArray(String[]::new);
    return createTransConsumerStream(
            rabbit, consumerTopicExchangeName, consumerQueueName, routingKeys)
        .receive()
        .doOnNext(tx -> log.info("Recieved: " + tx.get()))
        .filter(
            tx -> {
              if (ACTION_ON_STATES.contains(tx.get().getState())) {
                return true;
              }
              tx.reject();
              return false;
            })
        // Buffer events in last n minutes so work dirs becoming available as runs end close
        // to each other will all be rescheduled together rather than going through the same
        // computation multiple times in sequence
        .buffer(Duration.ofSeconds(bufferDurationSec))
        // We only need one event from the window to trigger the next schedule
        .map(
            bufferedTransactions -> {
              bufferedTransactions.stream().skip(1).forEach(Transaction::commit);
              return bufferedTransactions.get(0);
            })
        .doOnNext(tx -> log.info("Triggering scheduleOn: {}", tx.get()))
        // Ask dir scheduler to schedule next batch of runs and stream them to the sourceSink
        .flatMap(
            tx ->
                gatekeeperClient
                    .getAllRuns()
                    .map(runs -> dirScheduler.getNextInitializedRuns(ImmutableList.copyOf(runs)))
                    .flatMapMany(Flux::fromIterable)
                    .map(this::toWfMgmtRunMsg)
                    .doOnNext(sourceSink::send)
                    .then(Mono.just(tx)))
        .subscribe(Transaction::commit);
  }

  private WfMgmtRunMsg toWfMgmtRunMsg(Run run) {
    val msgWep = run.getWorkflowEngineParams();
    val runWep =
        EngineParams.newBuilder()
            .setLatest(msgWep.getLatest())
            .setDefaultContainer(msgWep.getDefaultContainer())
            .setLaunchDir(msgWep.getLaunchDir())
            .setRevision(msgWep.getRevision())
            .setProjectDir(msgWep.getProjectDir())
            .setWorkDir(msgWep.getWorkDir())
            .setResume(msgWep.getResume())
            .build();

    return WfMgmtRunMsg.newBuilder()
        .setRunId(run.getRunId())
        .setState(run.getState())
        .setWorkflowUrl(run.getWorkflowUrl())
        .setWorkflowParamsJsonStr(run.getWorkflowParamsJsonStr())
        .setWorkflowEngineParams(runWep)
        .setTimestamp(Instant.now().toEpochMilli())
        .build();
  }
}
