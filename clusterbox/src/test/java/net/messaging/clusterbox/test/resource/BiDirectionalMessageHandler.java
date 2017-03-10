package net.messaging.clusterbox.test.resource;

import java.util.Queue;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

public class BiDirectionalMessageHandler<T> implements ClusterBoxMessageHandler<T> {

    private Queue<T> queue;

    public BiDirectionalMessageHandler(Queue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void handleMessage(RequestMessage<?> message, ClusterBoxDropBox dropBox) {
        T payload = (T) message.getPayload();
        System.out.println("Got payload in " + payload);
        Address toAddress = message.popFromChain();
        System.out.println("Return Address " + toAddress);
        ResultMessage<T> result = new ResultMessage<T>(payload);
        result.setTo(toAddress);
        queue.offer(payload);
        dropBox.drop(result);
    }

    @Override
    public String getRequestName() {
        // TODO Auto-generated method stub
        return null;
    }

}
