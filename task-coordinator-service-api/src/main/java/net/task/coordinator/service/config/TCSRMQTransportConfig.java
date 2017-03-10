package net.task.coordinator.service.config;

public class TCSRMQTransportConfig {

    @Override
    public String toString() {
        return "RMQConfig [brokerAddress=" + brokerAddress + "]";
    }

    public TCSRMQTransportConfig() {
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    private String brokerAddress;
}

