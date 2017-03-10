package net.tcs.messaging;

import net.messaging.clusterbox.Address;
import net.task.coordinator.endpoint.TcsTaskExecutionEndpoint;

public class AddressParser {
    /**
     *
     * @param uri
     *            of the form rmq://address/exchange/ROUTING_KEY
     * @return
     */
    public static TcsTaskExecutionEndpoint parseAddress(String uri) {
        Address address = Address.fromJson(uri);
        return new TcsTaskExecutionEndpoint(address.getClusterBoxName(), address.getClusterBoxMailBoxName());
    }
}
