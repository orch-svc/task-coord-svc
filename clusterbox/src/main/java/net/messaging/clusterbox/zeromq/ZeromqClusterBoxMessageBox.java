package net.messaging.clusterbox.zeromq;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;

public final class ZeromqClusterBoxMessageBox implements Runnable {
    private final ClusterBoxMessageBox clusterBoxMailBox;
    private final ZeromqClusterBoxConfig clusterBoxConfig;
    private ZContext clusterBoxWorkerContext;
    private Socket frontendSocket;
    private Thread messageBoxThread;
    private AtomicBoolean on = new AtomicBoolean(false);
    private String messageBoxId;
    private String messageBoxName;
    private ClusterBoxDropBox dropBox;
    private Poller pollItems;
    private ObjectMapper mapper = new ObjectMapper();
    private ExecutorService workerExecutorService;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static final Long POLL_TIMEOUT_INTERVAL = 1000L; // milli
    // Seconds

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeromqClusterBoxMessageBox.class);

    protected ZeromqClusterBoxMessageBox(ClusterBoxMessageBox clusterBoxMessageBox, ClusterBoxDropBox dropBox,
            ZeromqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxMailBox = clusterBoxMessageBox;
        this.clusterBoxConfig = clusterBoxConfig;
        clusterBoxWorkerContext = new ZContext(ZeromqClusterBoxConstants.DEFAULT_IO_THREAD_COUNT);
        this.messageBoxId = clusterBoxMailBox.getMessageBoxId();
        this.messageBoxName = clusterBoxMailBox.getMessageBoxName();
        this.dropBox = dropBox;
        this.pollItems = new Poller(1);
        this.workerExecutorService = Executors.newCachedThreadPool(new ThreadFactory() {
            private AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                        String.format("%s-%s", "MessageBox-message-handler-thread", counter.incrementAndGet()));
            }
        });
    }

    @Override
    public void run() {
        frontendSocket.connect(clusterBoxConfig.getBackendProtocol() + "://" + clusterBoxConfig.getBackendAddress());
        ZMsg bootstrapMsg = new ZMsg();
        bootstrapMsg.add(ZeromqClusterBoxConstants.REGISTER_WORKER);
        bootstrapMsg.add(messageBoxName);
        bootstrapMsg.send(frontendSocket);
        ZFrame readyFrame = new ZFrame(ZeromqClusterBoxConstants.WORKER_READY);
        readyFrame.send(frontendSocket, 0);
        pollItems.register(frontendSocket, Poller.POLLIN);
        LOGGER.info("Message box {} with id {} started and registered with clusterbox", messageBoxName, messageBoxId);
        while (on.get() && !messageBoxThread.isInterrupted()) {
            pollItems.poll(POLL_TIMEOUT_INTERVAL);
            if (pollItems.getItem(0).isReadable()) {
                ZMsg msg = ZMsg.recvMsg(frontendSocket);
                if (msg == null) {
                    break;
                }
                String message = new String(msg.getFirst().getData());
                LOGGER.info("Message Received from clusterbox {}", message);
                try {
                    RequestMessage request = mapper.readValue(message, RequestMessage.class);
                    WorkerTask task = new WorkerTask(clusterBoxMailBox.getMessageHandler(),
                            new Exception("Task submitted by " + String.format("%s-%s", messageBoxId, messageBoxName)),
                            request, dropBox, messageBoxId);
                    workerExecutorService.submit(task);
                } catch (Exception e) {
                    LOGGER.error("Exception while handling message {}. Cause {}", message, e);
                }
            }
        }
        if (frontendSocket != null) {
            ZMsg doneMsg = new ZMsg();
            doneMsg.add(messageBoxName);
            doneMsg.add(ZeromqClusterBoxConstants.WORKER_SHUTDOWN);
            doneMsg.send(frontendSocket);
        }
        shutdownLatch.countDown();
    }

    protected synchronized void start() {
        if (on.get()) {
            LOGGER.info("Message box Already active !!");
            return;
        }
        frontendSocket = clusterBoxWorkerContext.createSocket(ZMQ.DEALER);
        frontendSocket.setIdentity(messageBoxId.getBytes());
        on.set(true);
        messageBoxThread = new Thread(this, "cb-mb-io-" + messageBoxId + "-thread");
        messageBoxThread.start();
        LOGGER.info("Worker {} for work {} started", messageBoxId, messageBoxName);
    }

    protected synchronized void shutdown() {
        if (!on.get()) {
            LOGGER.warn("MessageBox Already Shutdown or shutting down.");
            return;
        }
        on.set(false);
        try {
            shutdownLatch.await(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("Waiting on shutdown latch got interrupted {}", e);
            Thread.currentThread().interrupt();
        } finally {
            if (frontendSocket != null) {
                clusterBoxWorkerContext.destroySocket(frontendSocket);
            }
            if (workerExecutorService != null) {
                workerExecutorService.shutdown();
            }
            if (clusterBoxWorkerContext != null) {
                clusterBoxWorkerContext.destroy();
            }
        }

    }

    protected void waitForShutdown() {
        if (on.get()) {
            LOGGER.warn("MessageBox needs to be shutdown before waitforshutdown");
            return;
        }
        if (workerExecutorService != null) {
            try {
                workerExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("workerExecutorService await termination interrupted");
                Thread.currentThread().interrupt();
            } finally {
                if (!workerExecutorService.isTerminated()) {
                    LOGGER.error("worker executorService for messagebox {} is not shutting down gracefully",
                            messageBoxName);
                }
                workerExecutorService.shutdownNow();
                LOGGER.info("Worker for messagebox {} is shutdown now", messageBoxName);
            }
        }
    }

    private class WorkerTask implements Runnable {

        private final ClusterBoxMessageHandler handler;
        private final Exception clientStackTrace;
        private final RequestMessage message;
        private final ClusterBoxDropBox dropBox;
        private final String messageBoxId;
        private String oldThreadName;

        public WorkerTask(ClusterBoxMessageHandler handler, final Exception clientStackTrace,
                final RequestMessage message, ClusterBoxDropBox dropBox, String messageBoxId) {
            this.handler = handler;
            this.clientStackTrace = clientStackTrace;
            this.message = message;
            this.dropBox = dropBox;
            this.messageBoxId = messageBoxId;
        }

        @Override
        public void run() {
            try {
                oldThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName(messageBoxId + "-message-handler-thread");
                handler.handleMessage(message, dropBox);
            } catch (Exception e) {
                LOGGER.error("Exception happened while handling message {}, {}", e, clientStackTrace);
                throw e;
            } finally {
                Thread.currentThread().setName(oldThreadName);
            }
        }
    }
}
