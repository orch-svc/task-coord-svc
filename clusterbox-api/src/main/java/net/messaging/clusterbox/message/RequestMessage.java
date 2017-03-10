package net.messaging.clusterbox.message;

import java.util.LinkedList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.messaging.clusterbox.Address;

public class RequestMessage<T> extends Message<T> {

    public RequestMessage() {
        super();
    }

    @JsonCreator
    public RequestMessage(@JsonProperty("payload") T payload) {
        super(payload);
    }

    public void pushToFromChain(Address address) {
        if (fromChain == null) {
            fromChain = new LinkedList<Address>();

        }
        fromChain.push(address);
    }

}
