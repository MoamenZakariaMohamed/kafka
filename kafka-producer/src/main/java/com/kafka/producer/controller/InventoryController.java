package com.kafka.producer.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.producer.data.InventoryEvent;
import com.kafka.producer.data.InventoryEventType;
import com.kafka.producer.service.InventoryProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryProducer inventoryEventProducer;

    @PostMapping("/v1/inventoryEvent")
    public ResponseEntity<?> postLibraryEvent(@RequestBody InventoryEvent inventoryEvent) throws JsonProcessingException {

        if (InventoryEventType.NEW != inventoryEvent.getInventoryEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }
        //invoke kafka producer
        inventoryEventProducer.sendInventoryEvent_ProducerRecord(inventoryEvent);
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(inventoryEvent);
    }

    //PUT
    @PutMapping("/v1/inventoryEvent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody InventoryEvent inventoryEvent) throws JsonProcessingException {


        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(inventoryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        inventoryEventProducer.sendInventoryEvent_ProducerRecord(inventoryEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(inventoryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(InventoryEvent inventoryEvent) {
        if (inventoryEvent.getInventoryId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!InventoryEventType.UPDATE.equals(inventoryEvent.getInventoryEventType())) {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }
}

