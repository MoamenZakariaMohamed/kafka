package com.kafka.producer.data;

import lombok.Data;

@Data
public class InventoryEvent {
    private Integer inventoryId;
    private InventoryEventType inventoryEventType;
    private Product product;
}
