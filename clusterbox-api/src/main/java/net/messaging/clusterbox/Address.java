package net.messaging.clusterbox;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Address implements Serializable {

    private static final long serialVersionUID = -1268837008453727844L;

    @JsonProperty
    private final String clusterBoxName;

    @JsonProperty
    private final String clusterBoxMailBoxName;
    private static ObjectMapper mapper = new ObjectMapper();

    @JsonCreator
    public Address(@JsonProperty("clusterBoxName") String clusterBoxName,
            @JsonProperty("clusterBoxMailBoxName") String clusterBoxMailBoxName) {
        super();
        this.clusterBoxName = clusterBoxName;
        this.clusterBoxMailBoxName = clusterBoxMailBoxName;
    }

    public String getClusterBoxName() {
        return clusterBoxName;
    }

    public String getClusterBoxMailBoxName() {
        return clusterBoxMailBoxName;
    }

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return null;
        }
    }

    public static Address fromJson(String json) {
        try {
            return mapper.readValue(json, Address.class);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "Address [clusterBoxName=" + clusterBoxName + ", clusterBoxMailBoxName=" + clusterBoxMailBoxName + "]";
    }

}
