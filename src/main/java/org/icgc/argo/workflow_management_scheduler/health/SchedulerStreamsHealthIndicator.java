package org.icgc.argo.workflow_management_scheduler.health;

import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.rabbitmq.SchedulerStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SchedulerStreamsHealthIndicator implements ReactiveHealthIndicator {
    private final SchedulerStreams schedulerStreams;

    @Override
    public Mono<Health> health() {
        val healthBuilder = Health.up()
                .withDetail("schedulerConsumer", schedulerStreams.getSchedulerConsumer().isDisposed())
                .withDetail("schedulerProducer", schedulerStreams.getSchedulerConsumer().isDisposed());

        if (schedulerStreams.getSchedulerConsumer().isDisposed()
                    || schedulerStreams.getSchedulerProducer().isDisposed()) {
            healthBuilder.status(Status.DOWN);
        }
        return Mono.just(healthBuilder.build());
    }
}
