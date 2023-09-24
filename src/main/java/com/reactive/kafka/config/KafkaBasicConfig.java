package com.reactive.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaBasicConfig {
    @Value("${CONFLUENT.URL}")
    private String bootstrapServer;
    @Value("${CONFLUENT.USERNAME}")
    private String userName;
    @Value("${CONFLUENT.PASSWORD}")
    private String password;
    @Value("${CONFLUENT.SCHEMA_REG_URL}")
    private String schemaRegistryUrl;
    @Value("${CONFLUENT.SCHEMA_REG_USER}")
    private String schemaRegistryUser;

    @Value("${CONFLUENT.KEY_STORE}")
    private String keyStore;

    protected Map<String, Object> getBasicConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", this.bootstrapServer);
        config.put("ssl.endpoint.identification.algorithm", "https");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", this.userName, this.password));
        config.put("basic.auth.credentials.source", "USER_INFO");
        config.put("schema.registry.basic.auth.user.info", this.schemaRegistryUser);
        config.put("schema.registry.url", this.schemaRegistryUrl);
        return config;
    }

    protected Properties getBasicConfigProperty() {
        Properties config = new Properties();
        config.put("bootstrap.servers", this.bootstrapServer);
        config.put("ssl.endpoint.identification.algorithm", "https");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", this.userName, this.password));
        config.put("basic.auth.credentials.source", "USER_INFO");
        config.put("schema.registry.basic.auth.user.info", this.schemaRegistryUser);
        config.put("schema.registry.url", this.schemaRegistryUrl);
        return config;
    }
}
