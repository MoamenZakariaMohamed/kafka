package com.kafka.consumer.service.consumers;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.data.InventoryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryService {


 private final KafkaTemplate<Integer, Object> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String value = consumerRecord.value();
        log.info("value : {} ", value);
        InventoryEvent inventoryEvent = objectMapper.readValue(value, InventoryEvent.class);
        log.info("libraryEvent : {} ", inventoryEvent);

        if(inventoryEvent.getInventoryId() != null && ( inventoryEvent.getInventoryId() == 999 )){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (inventoryEvent.getInventoryEventType()) {
            case NEW -> save(inventoryEvent);
            case UPDATE -> {
                validate(inventoryEvent);
                save(inventoryEvent);
            }
            default -> log.info("Invalid Library Event Type");
        }

    }

    private void save(InventoryEvent inventoryEvent) {
        log.info("Successfully Persisted the inventory Event {} ", inventoryEvent);
    }

    private void validate(InventoryEvent inventoryEvent) {

        if (inventoryEvent.getInventoryId() == null || inventoryEvent.getProduct().getProductId() == null) {
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        log.info("Validation is successful for the library Event : {} ", inventoryEvent);
    }



}