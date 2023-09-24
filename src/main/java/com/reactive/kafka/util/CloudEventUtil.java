package com.reactive.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import io.cloudevents.spring.http.CloudEventHttpUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Objects;

import static io.cloudevents.core.CloudEventUtils.mapData;

@Component
public class CloudEventUtil<T> {

    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String PARTITIONING_KEY_EXTENSION = "partitionkey";
    private final String URL = "/api";

    public T toObject(CloudEvent cloudEvent, Class<T> type) {
        try {
            if (cloudEvent.getData() instanceof JsonCloudEventData) {
                return OBJECT_MAPPER.treeToValue(((JsonCloudEventData) cloudEvent.getData()).getNode(), type);
            } else {
                return Objects.requireNonNull(mapData(
                        cloudEvent,
                        PojoCloudEventDataMapper.from(OBJECT_MAPPER, type)
                )).getValue();
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private CloudEventData pojoCloudEventData(T object) {
        return PojoCloudEventData.wrap(object, OBJECT_MAPPER::writeValueAsBytes);
    }

    private CloudEventData jsonCloudEventData(T object) {
        return JsonCloudEventData.wrap(OBJECT_MAPPER.valueToTree(object));
    }

    public CloudEvent pojoCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(pojoCloudEventData(object)).build();
    }

    public CloudEvent pojoCloudEvent(final CloudEvent cloudEvent, final T object, final String id) {
        return cloudEventBuilder(cloudEvent, object, id).withData(pojoCloudEventData(object)).build();
    }

    public CloudEvent pojoCloudEvent(final HttpHeaders httpHeaders, final T object, final String id) {
        return cloudEventBuilder(httpHeaders, object, id).withData(pojoCloudEventData(object)).build();
    }

    public CloudEvent jsonCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(jsonCloudEventData(object)).build();
    }

    public CloudEvent jsonCloudEvent(final CloudEvent cloudEvent, final T object, final String id) {
        return cloudEventBuilder(object, id).withData(jsonCloudEventData(object)).build();
    }

    public CloudEvent jsonCloudEvent(final HttpHeaders httpHeaders, final T object, final String id) {
        return cloudEventBuilder(httpHeaders, object, id).withData(jsonCloudEventData(object)).build();
    }

    private CloudEventBuilder cloudEventBuilder(final T object, final String id) {
        return CloudEventBuilder.v1()
                .withSource(URI.create(URL))
                .withType(object.getClass().getName())
                .withId(id)
                .withExtension(PARTITIONING_KEY_EXTENSION, id)
                .withTime(ZonedDateTime.now().toOffsetDateTime());
    }

    private CloudEventBuilder cloudEventBuilder(final CloudEvent cloudEvent, final T object, String id) {
        return CloudEventBuilder.from(cloudEvent)
                .withSource(URI.create(URL))
                .withId(id)
                .withType(object.getClass().getName());


    }

    private CloudEventBuilder cloudEventBuilder(final HttpHeaders httpHeaders, final T object, String id) {
        return CloudEventHttpUtils.fromHttp(httpHeaders)
                .withSource(URI.create(URL))
                .withId(id)
                .withType(object.getClass().getName());
    }
}
