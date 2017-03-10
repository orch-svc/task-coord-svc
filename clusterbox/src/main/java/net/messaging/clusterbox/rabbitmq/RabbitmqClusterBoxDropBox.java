package net.messaging.clusterbox.rabbitmq;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.FailureResponse;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

public class RabbitmqClusterBoxDropBox implements ClusterBoxDropBox {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqClusterBoxDropBox.class);
    private final Connection connection;
    private final ObjectMapper mapper;
    private final ExecutorService dropAndReceiveExecutor;
    private final ExecutorService dropMessageExecutor;
    private volatile boolean on = false;

    public RabbitmqClusterBoxDropBox(Connection connection) {
        this.connection = connection;
        mapper = new ObjectMapper();
        dropAndReceiveExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                String name = String.format("%s.%s", "DropAndReceiver-thread-", counter.incrementAndGet());
                return new Thread(r, name);
            }
        });

        dropMessageExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                String name = String.format("%s.%s", "DropMessageWorker-thread-", counter.incrementAndGet());
                return new Thread(r, name);
            }

        });
        on = true;
    }

    @Override
    public void drop(Message<?> message) throws ClusterBoxMessageSendFailed {
        if (!on) {
            throw new ClusterBoxMessageSendFailed("Dropbox is shutdown");
        }
        if (message == null) {
            LOGGER.error("Message is null");
            throw new ClusterBoxMessageSendFailed("transport message is null");
        }
        if (message.getTo() == null) {
            LOGGER.error("Send to address is null");
            throw new ClusterBoxMessageSendFailed("transport to address is null");
        }
        DropMessageWorker dropMessageWorker;
        try {
            dropMessageWorker = new DropMessageWorker(connection.createChannel(), message);
            dropMessageExecutor.submit(dropMessageWorker);
        } catch (IOException e) {
            LOGGER.error("Message send failed. Exception {}", e);
            throw new ClusterBoxMessageSendFailed(e.getMessage());
        }
    }

    @Override
    public Future<ResultMessage> dropAndReceive(RequestMessage<?> message) throws ClusterBoxMessageSendFailed {
        DropAndReceiver dropAndReceiver;
        if (!on) {
            throw new ClusterBoxMessageSendFailed("Dropbox is shutdown");
        }
        try {
            dropAndReceiver = new DropAndReceiver(connection.createChannel(), message);
            return dropAndReceiveExecutor.submit(dropAndReceiver);
        } catch (IOException e) {
            LOGGER.error("Message send failed. Exception {}", e);
            throw new ClusterBoxMessageSendFailed(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        on = false;
        if (dropAndReceiveExecutor != null) {
            dropAndReceiveExecutor.shutdown();
        }
        if (dropMessageExecutor != null) {
            dropMessageExecutor.shutdown();
        }
    }

    public void waitForShutdown() {
        if (on) {
            LOGGER.error("Needs to be shutdown before waiting");
            return;
        }
        try {
            dropAndReceiveExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (!dropAndReceiveExecutor.isTerminated()) {
                LOGGER.error("dropAndReceive message executor is not gracefully shutting down");
            }
            dropAndReceiveExecutor.shutdownNow();
            LOGGER.info("dropAndReceive Executor shutdown now");
        }
        try {
            dropMessageExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (!dropMessageExecutor.isTerminated()) {
                LOGGER.error("Drop message executor is not gracefully shutting down");
            }
            dropMessageExecutor.shutdownNow();
            LOGGER.info("Drop message executor is shutdown now");
        }
    }

    private void validateMessage(Message<?> message) {
        if (message.getTo() == null) {
            LOGGER.error("Message Transmisson failed. Reason - to address is null");
            throw new ClusterBoxMessageSendFailed("To Address is null");
        }
        if (message.peekFromChain() == null) {
            LOGGER.error("Message Transmisson failed. Reason - From address is null");
            throw new ClusterBoxMessageSendFailed("From Address is null");
        }
    }

    private class DropMessageWorker implements Runnable {
        private final Message<?> message;
        private final Channel channel;

        public DropMessageWorker(Channel channel, Message<?> message) {
            this.message = message;
            this.channel = channel;
        }

        @Override
        public void run() {
            String exchangeName = message.getTo().getClusterBoxName();
            String routingKey = message.getTo().getClusterBoxMailBoxName();
            LOGGER.info("Sending Message to exchange {} with routingKey {}", exchangeName, routingKey);
            try {
                channel.basicPublish(exchangeName, routingKey, null, mapper.writeValueAsBytes(message));
            } catch (Exception e) {
                LOGGER.error("Send FAILED Message {}. Exception {}", message, e);
            }
            closeChannel(channel);
        }
    }

    private class DropAndReceiver implements Callable<ResultMessage> {

        private final Channel channel;
        private final RequestMessage<?> message;
        private String responseQueueName;

        public DropAndReceiver(Channel channel, RequestMessage<?> message) {
            this.channel = channel;
            this.message = message;
        }

        @Override
        public ResultMessage call() throws Exception {
            try {
                responseQueueName = channel.queueDeclare().getQueue();
            } catch (Exception e) {
                throw new ClusterBoxMessageSendFailed(e.getMessage());
            }
            // Using default exchange to receive result message
            final Address fromAddress = new Address("", responseQueueName);
            String exchangeName = message.getTo().getClusterBoxName();
            String routingKey = message.getTo().getClusterBoxMailBoxName();
            message.pushToFromChain(fromAddress);
            String corrId = UUID.randomUUID().toString();
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().correlationId(corrId)
                    .replyTo(responseQueueName).build();
            try {
                validateMessage(message);
                LOGGER.warn("Sending message {}", message);
                channel.basicPublish(exchangeName, routingKey, properties, mapper.writeValueAsBytes(message));
            } catch (Exception e) {
                LOGGER.error("Dropping message {} failed. Exception {}", message, e);
                throw new ClusterBoxMessageSendFailed(e.getMessage());
            }
            final BlockingQueue<ResultMessage> resultHolder = new LinkedBlockingQueue<>();
            channel.basicConsume(responseQueueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                        byte[] body) {
                    try {
                        resultHolder.offer(mapper.readValue(body, ResultMessage.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                        FailureResponse response = new FailureResponse();
                        response.setDetailedMessage(e.getLocalizedMessage());
                        response.setErrorCode("INTERNAL_ERROR");
                        response.setErrorMessage(e.getMessage());
                        ResultMessage<FailureResponse> errorMessage = new ResultMessage<>(response);
                        resultHolder.offer(errorMessage);
                    }
                }
            });
            ResultMessage result = resultHolder.take();
            cleanUp();
            return result;
        }

        public void cleanUp() throws Exception {
            channel.queueDelete(responseQueueName);
            closeChannel(channel);
        }
    }

    private void closeChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
                LOGGER.warn("Channel close failed. Exception {}", e);
            }
        }
    }
}
