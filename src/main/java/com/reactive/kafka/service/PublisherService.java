package com.reactive.kafka.service;

import com.reactive.kafka.model.Event;
import com.reactive.kafka.util.CloudEventUtil;
import io.cloudevents.CloudEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.kafka.sender.SenderRecord;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PublisherService {

    @Value("${KAFKA_TOPIC_NAME}")
    public String kafkaTopicName;
    private final CloudEventUtil<Event> cloudEventUtil;
    private final ReactiveKafkaProducerTemplate<String, CloudEvent> kafkaProducerTemplate;

    public void publish(String key) {

        final Event event = Event.builder().eventName("Kafka Event").build();
        final CloudEvent cloudEvent = cloudEventUtil.pojoCloudEvent(event, UUID.randomUUID().toString());

        kafkaProducerTemplate.send(SenderRecord.create(new ProducerRecord<>(
                        kafkaTopicName,
                        key, cloudEvent), new Object()))
                .doOnError(error -> log.info("unable to send message due to: {}", error.getMessage())).subscribe(record -> {
                    RecordMetadata metadata = record.recordMetadata();
                    log.info("send message with partition: {} offset: {}", metadata.partition(), metadata.offset());
                });
    }
}
