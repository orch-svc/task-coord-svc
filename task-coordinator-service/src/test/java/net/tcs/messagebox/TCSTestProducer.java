package net.tcs.messagebox;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

public class TCSTestProducer implements ClusterBoxDropBox {

    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();

    public Message getMessage() {
        try {
            return queue.poll(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public void drop(Message<?> message) throws ClusterBoxMessageSendFailed {
        queue.offer(message);

    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

    @Override
    public Future<ResultMessage> dropAndReceive(RequestMessage<?> message) throws ClusterBoxMessageSendFailed {
        // TODO Auto-generated method stub
        return null;
    }
}
