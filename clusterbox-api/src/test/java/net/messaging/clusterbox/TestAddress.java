package net.messaging.clusterbox;

import org.testng.annotations.Test;

import net.messaging.clusterbox.message.RequestMessage;

public class TestAddress {

    @Test
    public void testMessageFromChain() {
        RequestMessage<String> requestMessage = new RequestMessage<String>("hello");
        Address address1 = new Address("1", "one");
        Address address2 = new Address("2", "two");
        Address address3 = new Address("3", "three");
        requestMessage.pushToFromChain(address1);
        requestMessage.pushToFromChain(address2);
        requestMessage.pushToFromChain(address3);
        System.out.println(requestMessage.peekFromChain());
        Address topAddress = requestMessage.popFromChain();
        System.out.println(topAddress);

    }

}
