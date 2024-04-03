package com.kafka.consumer.service.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InventoryConsumer {

    @Autowired
    private InventoryService inventoryEventService;

    @KafkaListener(topics = {"library-events"}, groupId = "inventory-consumer-group-1")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        inventoryEventService.processLibraryEvent(consumerRecord);
        log.info("Consumer Record: {}", consumerRecord);
    }

    /**
     *  ----- consumer group and partition with intial offset  ----
     *
     @KafkaListener(groupId = "inventory-consumer-group-1",
     topicPartitions = @TopicPartition(topic = "inventory-events",
     partitionOffsets = {
     @PartitionOffset(partition = "0", initialOffset = "0"),
     @PartitionOffset(partition = "2", initialOffset = "0")}))
     */
    public void onMessage_PartitionIntialOffset(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("Consumer Record: {}", consumerRecord);
    }


    /**
     * ----- consumer group and partition with no intial offset  ----
     *
     @KafkaListener(topicPartitions = @TopicPartition(topic = "inventory-events", partitions = { "0", "1" }))
     */
    public void onMessage_PartitionNoOffset(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("Consumer Record: {}", consumerRecord);
    }
}