package net.messaging.clusterbox.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResultMessage<T> extends Message<T> {

    @JsonCreator
    public ResultMessage(@JsonProperty("payload") T payload) {
        super(payload);
    }

}
