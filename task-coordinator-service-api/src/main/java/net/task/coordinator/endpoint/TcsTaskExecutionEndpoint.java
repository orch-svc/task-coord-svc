package net.task.coordinator.endpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.messaging.clusterbox.Address;

public class TcsTaskExecutionEndpoint extends TcsEndPoint {

    private String brokerAddress;

    @JsonCreator
    public TcsTaskExecutionEndpoint(@JsonProperty("clusterBoxName") String exchangeName,
            @JsonProperty("clusterBoxMailBoxName") String routingKey) {
        super(exchangeName, routingKey);
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    @JsonIgnore
    public String toEndpointURI() {
        return new Address(getExchangeName(), getRoutingKey()).toJson();
    }
}
