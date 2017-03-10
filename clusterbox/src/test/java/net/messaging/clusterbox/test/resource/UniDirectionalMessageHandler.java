package net.messaging.clusterbox.test.resource;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;

public class UniDirectionalMessageHandler<T> implements ClusterBoxMessageHandler<T> {

    private final Queue<T> queue;

    private static final Logger LOGGER = LoggerFactory.getLogger(UniDirectionalMessageHandler.class);

    public UniDirectionalMessageHandler(Queue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void handleMessage(RequestMessage<?> message, ClusterBoxDropBox dropBox) {
        T payload = (T) message.getPayload();
        LOGGER.info("Received Payload {}", payload);
        queue.offer(payload);
    }

    @Override
    public String getRequestName() {
        // TODO Auto-generated method stub
        return null;
    }

}
