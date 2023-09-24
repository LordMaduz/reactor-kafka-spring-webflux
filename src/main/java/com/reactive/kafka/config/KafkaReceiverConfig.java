package com.reactive.kafka.config;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Map;

@Configuration
public class KafkaReceiverConfig extends KafkaBasicConfig {

    @Value("${KAFKA_GROUP_ID}")
    public String KafkaGroupId;

    @Value("${KAFKA_TOPIC_NAME}")
    public String kafkaTopicName;


    public ReceiverOptions<String, CloudEvent> getReceiverOptions() {
        Map<String, Object> basicConfig = getBasicConfig();
        basicConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        basicConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);

        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, CloudEventDeserializer.class);

        ReceiverOptions<String, CloudEvent> basicReceiverOptions = ReceiverOptions.create(basicConfig);

        return basicReceiverOptions
                .subscription(Collections.singletonList(kafkaTopicName));
    }

    @Bean
    public KafkaReceiver<String, CloudEvent> kafkaReceiver() {
        return KafkaReceiver.create(getReceiverOptions());
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, CloudEvent> reactiveKafkaConsumerTemplate() {
        return new ReactiveKafkaConsumerTemplate<>(getReceiverOptions());
    }

}
