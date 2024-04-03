package com.kafka.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfiguration {

    @Value("${spring.kafka.topic}")
    private String topic;
    @Value("${topic.partitions}")
    private Integer partition;

    @Value("${topic.replicas}")
    private Short replicationFactor;


    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(topic).partitions(partition).replicas(replicationFactor).build();
    }
}
