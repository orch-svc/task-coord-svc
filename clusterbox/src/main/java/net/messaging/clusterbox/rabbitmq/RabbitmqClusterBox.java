package net.messaging.clusterbox.rabbitmq;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.ClusterBoxConfig;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.exception.ClusterBoxCreateFailed;
import net.messaging.clusterbox.exception.ClusterBoxMessageBoxStartFailed;
import net.messaging.clusterbox.message.RequestMessage;

public class RabbitmqClusterBox implements ClusterBox {

    private final RabbitmqClusterBoxConfig clusterBoxConfig;
    private Connection consumerConnection;
    private RabbitmqClusterBoxDropBox clusterBoxDropBox;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqClusterBox.class);
    private volatile boolean on = false;
    private CountDownLatch latch = new CountDownLatch(1);
    private ConcurrentHashMap<String, ClusterBoxWorker> workers = new ConcurrentHashMap<>();

    private RabbitmqClusterBox(RabbitmqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
    }

    public static ClusterBox newClusterBox(RabbitmqClusterBoxConfig clusterBoxConfig) {
        return new RabbitmqClusterBox(clusterBoxConfig);
    }

    @Override
    public ClusterBoxConfig getClusterBoxConfig() {
        return clusterBoxConfig;
    }

    @Override
    public synchronized void start() {
        if (on) {
            return;
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(clusterBoxConfig.getBrokerConfig().getIpAddress());
        if (clusterBoxConfig.getBrokerConfig().getFrontendPort() != null) {
            factory.setPort(Integer.valueOf(clusterBoxConfig.getBrokerConfig().getFrontendPort()));
        }
        try {
            consumerConnection = factory.newConnection();
            clusterBoxDropBox = new RabbitmqClusterBoxDropBox(factory.newConnection());
        } catch (IOException | TimeoutException e) {
            throw new ClusterBoxCreateFailed(e.getMessage());
        }
        on = true;
        latch.countDown();
    }

    @Override
    public void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox) {
        if (!on) {
            throw new ClusterBoxMessageBoxStartFailed("Clusterbox is not yet started or shutdown");
        }
        if (clusterBoxMessageBox.getMessageHandler() == null) {
            throw new ClusterBoxMessageBoxStartFailed(clusterBoxMessageBox.getMessageBoxName()
                    + " MessageBox Registration failed. MessageHandler is Null");
        }
        ClusterBoxWorker worker = new ClusterBoxWorker(clusterBoxMessageBox, clusterBoxDropBox,
                clusterBoxConfig.getClusterBoxName());
        if (null == workers.putIfAbsent(clusterBoxMessageBox.getMessageBoxId(), worker)) {
            LOGGER.info("Starting clusterboxworker for messageBox {}", clusterBoxMessageBox.getMessageBoxId());
            worker.start(clusterBoxConfig.isAutoDeclare());
        } else {
            LOGGER.warn("ClusterBoxMessageBox already registered.");
        }
    }

    @Override
    public ClusterBoxDropBox getDropBox() {
        return clusterBoxDropBox;
    }

    @Override
    public void shutDown() {
        LOGGER.info("Shutting down clusterbox !!");
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        on = false;
        for (Entry<String, ClusterBoxWorker> entry : workers.entrySet()) {
            LOGGER.info("shutting down MessageBox {}", entry.getKey());
            entry.getValue().shutdown();
        }
        if (clusterBoxDropBox != null) {
            clusterBoxDropBox.shutdown();
            clusterBoxDropBox.waitForShutdown();
        }
        if (consumerConnection != null) {
            try {
                consumerConnection.close();
            } catch (Exception e) {
                LOGGER.error("ConsumerConnection close failed. Exception {}", e);
            }
        }
    }

    private class ClusterBoxWorker {
        private volatile boolean on = false;
        private Channel channel;
        private final ClusterBoxMessageBox clusterBoxMessageBox;
        private ObjectMapper mapper;
        private final String exchangeName;
        private final String queueName;
        private final String rtKey;
        private boolean autoDeclare = false;

        public ClusterBoxWorker(ClusterBoxMessageBox messageBox, RabbitmqClusterBoxDropBox dropBox,
                String exchangeName) {
            this.clusterBoxMessageBox = messageBox;
            this.exchangeName = exchangeName;
            this.queueName = clusterBoxMessageBox.getMessageBoxId();
            this.rtKey = clusterBoxMessageBox.getMessageBoxName();
        }

        public void start(boolean autoDeclare) {
            this.autoDeclare = autoDeclare;
            try {
                this.channel = consumerConnection.createChannel();
                mapper = new ObjectMapper();
                if (autoDeclare) {
                    LOGGER.warn("Declaring Exchange {}", exchangeName);
                    channel.exchangeDeclare(exchangeName, clusterBoxConfig.getExchangeType(), true);
                    LOGGER.warn("Declaring queue {}", queueName);
                    channel.queueDeclare(queueName, true, false, false, null);
                    LOGGER.warn("Binding queue {} with  routingkey {}", queueName, rtKey);
                    channel.queueBind(queueName, exchangeName, rtKey);
                }
                String consumerTag = clusterBoxMessageBox.getMessageBoxName();
                channel.basicConsume(clusterBoxMessageBox.getMessageBoxId(), false, consumerTag,
                        new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag, Envelope envelope,
                                    AMQP.BasicProperties properties, byte[] body) {
                                RequestMessage<?> message = null;
                                try {
                                    LOGGER.info("Got message {}", new String(body));
                                    message = mapper.readValue(body, RequestMessage.class);
                                    if (message != null) {
                                        clusterBoxMessageBox.getMessageHandler().handleMessage(message,
                                                clusterBoxDropBox);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOGGER.error("Error processing message {}. Cause - {}", message, e.getMessage());
                                } finally {
                                    try {

                                        channel.basicAck(envelope.getDeliveryTag(), false);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        LOGGER.error("Error during message ack. Cause - {}", e.getMessage());
                                    }
                                }
                            }

                        });
            } catch (IOException e) {
                e.printStackTrace();
                throw new ClusterBoxMessageBoxStartFailed(e.getMessage());
            }
            on = true;
        }

        public void shutdown() {
            if (!on) {
                LOGGER.error("ClusterBoxMessageBox not yet started");
                return;
            }
            if (channel != null) {
                try {
                    if (autoDeclare) {
                        channel.queueUnbind(queueName, exchangeName, rtKey);
                        channel.queueDelete(queueName);
                    }
                    channel.close();
                } catch (Exception e) {
                    LOGGER.error("ClusterBoxMessageBox shutdown failed. Exception {}", e);
                }
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((exchangeName == null) ? 0 : exchangeName.hashCode());
            result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
            result = prime * result + ((rtKey == null) ? 0 : rtKey.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ClusterBoxWorker other = (ClusterBoxWorker) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (exchangeName == null) {
                if (other.exchangeName != null)
                    return false;
            } else if (!exchangeName.equals(other.exchangeName))
                return false;
            if (queueName == null) {
                if (other.queueName != null)
                    return false;
            } else if (!queueName.equals(other.queueName))
                return false;
            if (rtKey == null) {
                if (other.rtKey != null)
                    return false;
            } else if (!rtKey.equals(other.rtKey))
                return false;
            return true;
        }

        private RabbitmqClusterBox getOuterType() {
            return RabbitmqClusterBox.this;
        }

    }

    @Override
    public boolean isOn() {
        return consumerConnection.isOpen();
    }

    @Override
    public void unregisterMessageBox(String clusterBoxMessageBoxId) {

    }

}
