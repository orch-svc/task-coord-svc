package net.messaging.clusterbox.zeromq;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

// TODO : Change implementation to be using a connection pool if possible

public class ZeromqClusterBoxDropBox implements ClusterBoxDropBox {

    private final ZeromqClusterBoxConfig clusterBoxConfig;
    private final ZMQ.Context dropBoxContext;
    private ZMQ.Socket frontendSocket;
    private ObjectMapper mapper;
    private static final String WORKER_PREFIX = "TxAndRx";
    private final ExecutorService dropAndReceiveExecutor;
    private final ExecutorService dropExecutor;
    private volatile boolean on = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeromqClusterBoxDropBox.class);

    public ZeromqClusterBoxDropBox(ZeromqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
        dropBoxContext = ZMQ.context(1);
        mapper = new ObjectMapper();
        dropExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                String name = String.format("%s.%s", "SendMessage-thread-", counter.incrementAndGet());
                return new Thread(r, name);
            }

        });
        dropAndReceiveExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                String name = String.format("%s.%s", "SendAndReceive-thread-", counter.incrementAndGet());
                return new Thread(r, name);
            }

        });
        on = true;
        LOGGER.info("Started zeromqClusterBoxDropBox");
    }

    @Override
    public void drop(Message<?> message) throws ClusterBoxMessageSendFailed {
        if (!on) {
            LOGGER.error("Drop box is shutdown");
            throw new ClusterBoxMessageSendFailed("Drop box is shutdown");
        }
        DropMessageTask task = new DropMessageTask(message);
        dropExecutor.submit(task);
    }

    @Override
    public void shutdown() {
        dropAndReceiveExecutor.shutdown();
        dropExecutor.shutdown();
        try {
            dropAndReceiveExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            dropExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("Exception while waiting for termination {}", e);
        } finally {
            if (!dropExecutor.isTerminated()) {
                LOGGER.warn("dropExecutor did not shutdown gracefully");
            }
            dropExecutor.shutdownNow();
            LOGGER.info("dropExecutor shutdown now");
            if (!dropAndReceiveExecutor.isTerminated()) {
                LOGGER.warn("dropAndReceiveExecutor did not shutdown gracefully");
            }
            dropAndReceiveExecutor.shutdownNow();
            LOGGER.info("dropAndReceiveExecutor shutdown now");
            dropBoxContext.term();
        }
    }

    @Override
    public Future<ResultMessage> dropAndReceive(RequestMessage<?> message) throws ClusterBoxMessageSendFailed {
        if (!on) {
            LOGGER.error("Drop box is shutdown");
            throw new ClusterBoxMessageSendFailed("Drop box is shutdown");
        }
        DropAndReceiveTask task = new DropAndReceiveTask(message);
        return dropAndReceiveExecutor.submit(task);
    }

    private class DropMessageTask implements Runnable {
        private final Message<?> message;
        private ZMQ.Socket frontendSocket;

        public DropMessageTask(Message<?> message) {
            this.message = message;
        }

        @Override
        public void run() {
            frontendSocket = dropBoxContext.socket(ZMQ.PUSH);
            // frontendSocket.setIdentity(message.peekFromChain().toJson().getBytes());
            frontendSocket.connect(clusterBoxConfig.getBrokerConfig().getFrontendProtocol() + "://"
                    + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                    + clusterBoxConfig.getBrokerConfig().getFrontendPort());
            try {
                ZMsg msg = new ZMsg();
                // msg.add(message.getTo().toJson());
                msg.add(mapper.writeValueAsString(message));
                msg.send(frontendSocket);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                throw new ClusterBoxMessageSendFailed(e.getMessage());
            } finally {
                frontendSocket.close();
            }
            LOGGER.info("Message {} Sent.....", message);
        }

    }

    private class DropAndReceiveTask implements Callable<ResultMessage> {

        private final RequestMessage message;
        private ZMQ.Socket dynamicWorkerSocket;
        private ZMQ.Socket frontendSocket;
        private final Address dynamicWorkerAddress;
        private final String workerIdentity;

        public DropAndReceiveTask(RequestMessage message) {
            this.message = message;
            this.workerIdentity = String.format("%s.%s", WORKER_PREFIX, UUID.randomUUID().toString());
            dynamicWorkerAddress = new Address("", workerIdentity);
        }

        @Override
        public ResultMessage call() throws Exception {
            frontendSocket = dropBoxContext.socket(ZMQ.PUSH);
            // frontendSocket.setIdentity(message.peekFromChain().toJson().getBytes());
            frontendSocket.connect(clusterBoxConfig.getBrokerConfig().getFrontendProtocol() + "://"
                    + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                    + clusterBoxConfig.getBrokerConfig().getFrontendPort());

            dynamicWorkerSocket = dropBoxContext.socket(ZMQ.DEALER);
            dynamicWorkerSocket.setIdentity(workerIdentity.getBytes());
            dynamicWorkerSocket.connect(clusterBoxConfig.getBrokerConfig().getFrontendProtocol() + "://"
                    + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                    + ZeromqClusterBoxConstants.DEFAILT_CB_BACKEND_PORT);
            try {
                message.pushToFromChain(dynamicWorkerAddress);
                ZMsg msg = new ZMsg();
                msg.add(mapper.writeValueAsString(message));
                msg.send(frontendSocket);
                LOGGER.info("Message {} Sent.....", message);
                String reply = dynamicWorkerSocket.recvStr();
                LOGGER.info("Reply {} received.....", reply);
                return mapper.readValue(reply, ResultMessage.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                throw new ClusterBoxMessageSendFailed(e.getMessage());
            } finally {
                frontendSocket.close();
                dynamicWorkerSocket.close();
            }
        }

    }

}
