package net.task.coordinator.service.config;

public enum TCSTransportMode {

    RABBITMQ("rabbitmq"), ZEROMQ("zeromq"), MQTT("mqtt");

    private String description;

    private TCSTransportMode(String descprtion) {
        this.description = descprtion;
    }

    private String getDescription() {
        return this.description;
    }

}
