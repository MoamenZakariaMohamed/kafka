package com.kafka.consumer.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
@RequiredArgsConstructor
public class ConsumerConfiguration {
    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;
    @Autowired
    private  KafkaTemplate kafkaTemplate;

    private final KafkaProperties properties;

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            kafkaTemplate.send(retryTopic, record.partition(), record.key(), record.value());

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);
            kafkaTemplate.send(deadLetterTopic, record.partition(), record.key(), record.value());

        }
    };


    public DefaultErrorHandler errorHandler() {

        var fixedBackOff = new FixedBackOff(1000L, 2);

        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L);
        exponentialBackOff.setMultiplier(2L);
        exponentialBackOff.setMaxInterval(4000L);

        var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);

        var errorHandler = new DefaultErrorHandler(
                //consumerRecordRecoverer,
                publishingRecoverer(),
                //fixedBackOff
                exponentialBackOff
        );

        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler.setRetryListeners((consumerRecord, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {}",
                    ex.getMessage(), deliveryAttempt);
        });

        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
