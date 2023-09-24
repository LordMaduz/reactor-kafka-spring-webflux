package com.reactive.kafka.service;

import com.reactive.kafka.model.Event;
import com.reactive.kafka.util.CloudEventUtil;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumerService {

    private final ReactiveKafkaConsumerTemplate<String, CloudEvent> reactiveKafkaConsumerTemplate;
    private final CloudEventUtil<Event> cloudEventUtils;

    private final LogAccessor logAccessor = new LogAccessor(LogFactory.getLog(getClass()));

    @PostConstruct
    public void initialize() {
        onNEventReceived();
    }


    public Disposable onNEventReceived() {
        return reactiveKafkaConsumerTemplate
                .receive()
                .doOnError(error -> {
                    log.error("Error receiving event, Will retry", error);
                })
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ZERO.withSeconds(5)))
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.offset())
                )
                .concatMap(this::handleEvent)
                .doOnNext(event -> {
                    log.info("successfully consumed {}={}", Event.class.getSimpleName(), event);
                }).subscribe(record-> {
                    record.receiverOffset().commit();
                });
    }

    public Mono<ReceiverRecord<String, CloudEvent>> handleEvent(ReceiverRecord<String, CloudEvent> record) {
        return Mono.just(record).<CloudEvent>handle((result, sink) -> {
                    List<DeserializationException> exceptionList = getExceptionList(result);
                    if (!CollectionUtils.isEmpty(exceptionList)) {
                        logError(exceptionList);
                    } else {
                        if (Objects.nonNull(result.value())) {
                            sink.next(result.value());
                        }
                    }
                }).flatMap(this::processRecord)
                .doOnError(error -> {
                    log.error("Error Processing event: key {}", record.key(), error);
                }).onErrorResume(error-> Mono.empty())
                .then(Mono.just(record));
    }

    private Mono<CloudEvent> processRecord(CloudEvent cloudEvent) {
        Event event = cloudEventUtils.toObject(cloudEvent, Event.class);
        log.info("Event Record Processed: {}", event);
        return Mono.just(cloudEvent);
    }

    private void logError(final List<DeserializationException> exceptionList) {
        exceptionList.forEach(error -> {
            log.error("Deserialization Error: ", error);
        });
    }

    private List<DeserializationException> getExceptionList(ConsumerRecord<String, CloudEvent> record) {
        final List<DeserializationException> exceptionList = new ArrayList<>();

        DeserializationException valueException = ListenerUtils.getExceptionFromHeader(record, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, logAccessor);
        DeserializationException keyException = ListenerUtils.getExceptionFromHeader(record, SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, logAccessor);

        if (!ObjectUtils.isEmpty(valueException)) {
            exceptionList.add(valueException);
        }

        if (!ObjectUtils.isEmpty(keyException)) {
            exceptionList.add(keyException);
        }

        return exceptionList;
    }
}
