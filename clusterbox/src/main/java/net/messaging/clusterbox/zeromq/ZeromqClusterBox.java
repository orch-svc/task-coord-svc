package net.messaging.clusterbox.zeromq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMsg;

import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.ClusterBoxConfig;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.exception.ClusterBoxMessageBoxStartFailed;

public class ZeromqClusterBox implements ClusterBox, Runnable {

    private static final int MAX_PING_RETRY_COUNT = 3;
    private final AtomicInteger PING_RETRY_COUNT = new AtomicInteger(MAX_PING_RETRY_COUNT);
    private static final ZFrame PING_FRAME = new ZFrame(ZeromqClusterBoxConstants.PING_FRAME);
    private static final long PING_INTERVAL = 1000L; // Unit in ms

    private volatile long pingedAt;

    private AtomicBoolean on = new AtomicBoolean(false);
    private ZContext clusterBoxContext;
    private ZMQ.Socket frontendSocket;
    private ZMQ.Socket backendSocket;
    private Thread clusterBoxThread;
    private Poller pollItems;
    private ClusterBoxDropBox clusterBoxDropBox;
    private ConcurrentHashMap<String, ZeromqClusterBoxMessageBox> messageBoxes = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> discoveredWorkNameToIdMap = new ConcurrentHashMap<String, List<String>>();
    private AtomicBoolean pongPending = new AtomicBoolean(true);
    private Long pingInterval = PING_INTERVAL;
    private AtomicBoolean dirty = new AtomicBoolean(true);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final ZeromqClusterBoxConfig clusterBoxConfig;
    private boolean isAvailable = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeromqClusterBox.class);

    private ZeromqClusterBox(ZeromqClusterBoxConfig clusterBoxConfig) {
        this.clusterBoxConfig = clusterBoxConfig;
        pingInterval = clusterBoxConfig.getPingInterval() == null ? PING_INTERVAL : clusterBoxConfig.getPingInterval();
        this.pollItems = new Poller(2);
        clusterBoxContext = new ZContext(ZeromqClusterBoxConstants.DEFAULT_IO_THREAD_COUNT);
        clusterBoxDropBox = new ZeromqClusterBoxDropBox(clusterBoxConfig);
        if (clusterBoxConfig.isAutoStart()) {
            start();
        } else {
            LOGGER.info("Cluster box needs an explicit start");
        }
    }

    @Override
    public ClusterBoxConfig getClusterBoxConfig() {
        return clusterBoxConfig;
    }

    public static ClusterBox newClusterBox(ZeromqClusterBoxConfig clusterBoxConfig) {
        return new ZeromqClusterBox(clusterBoxConfig);
    }

    @Override
    public void start() {
        LOGGER.info("Staring clusterbox {} - {}", clusterBoxConfig.getClusterBoxName(),
                clusterBoxConfig.getClusterBoxId());
        startFrontEnd();
        startBackEnd();
        clusterBoxThread = new Thread(this, "cb-io-" + clusterBoxConfig.getClusterBoxId() + "-thread");
        on.set(true);
        clusterBoxThread.start();
    }

    @Override
    public void run() {
        while (on.get() && !clusterBoxThread.isInterrupted()) {
            try {
                if (dirty.get()) {
                    reInitPolling();
                }
                pollItems.poll(1000);
                if (!on.get()) {
                    break;
                }
                if (pollItems.getItem(0).isReadable()) {
                    ZMsg msg = ZMsg.recvMsg(frontendSocket, 0);
                    if (msg.isEmpty()) {
                        LOGGER.error("Got an Unexpected Empty message. ShuttingDown !!");
                        break;
                    }
                    // Check if Frame is HeartBeat
                    if (!PING_FRAME.equals(msg.peek())) {
                        JSONObject jsonFromClient = new JSONObject(new String(msg.peek().getData()));
                        JSONObject jsonAddress = jsonFromClient.getJSONObject("to");
                        String workName = jsonAddress.getString("clusterBoxMailBoxName");
                        if (discoveredWorkNameToIdMap.containsKey(workName)) {
                            Random random = new Random();
                            int randomIndex = random.nextInt(discoveredWorkNameToIdMap.get(workName).size());
                            String workerName = discoveredWorkNameToIdMap.get(workName).get(randomIndex);
                            msg.addFirst(workerName);
                            LOGGER.info("Sending work {} to messageBox worker {}", msg, workerName);
                            msg.send(backendSocket);
                        } else {
                            LOGGER.error("No worker with name {} Discovered yet in clusterbox {}. Skipping the message",
                                    workName);
                        }
                    }
                    resetRetryCounter();
                    pongPending.set(false);
                    isAvailable = true;
                } else if (pollItems.getItem(1).isReadable()) {
                    // Message on the Backend
                    ZMsg msg = ZMsg.recvMsg(backendSocket, 0);
                    if (msg == null) {
                        break;
                    }
                    String workerName = new String(msg.removeFirst().getData());
                    if (ZeromqClusterBoxConstants.WORKER_READY.equals(new String(msg.getLast().getData()))) {
                        LOGGER.info("MessageBox {} is ready !!", workerName);
                    } else if (ZeromqClusterBoxConstants.WORKER_SHUTDOWN.equals(new String(msg.getLast().getData()))) {
                        String workName = new String(msg.removeFirst().getData());
                        LOGGER.info("MessageBox {} shutdown request arrived for workname {}", workerName, workName);
                        if (discoveredWorkNameToIdMap.containsKey(workName)) {
                            discoveredWorkNameToIdMap.get(workName).remove(workerName);
                            LOGGER.info("Discovered Message box count for workname {} is {}", workName,
                                    discoveredWorkNameToIdMap.get(workName).size());
                            if (discoveredWorkNameToIdMap.get(workName).size() == 0) {
                                discoveredWorkNameToIdMap.remove(workName);
                            }
                        }
                        messageBoxes.remove(workerName);
                    } else if (ZeromqClusterBoxConstants.REGISTER_WORKER
                            .equals(new String(msg.peekFirst().getData()))) {
                        msg.removeFirst();
                        String workName = new String(msg.getFirst().getData());
                        LOGGER.info("Registering Worker {} for work {}", workerName, workName);
                        if (discoveredWorkNameToIdMap.containsKey(workName)) {
                            discoveredWorkNameToIdMap.get(workName).add(workerName);
                        } else {
                            discoveredWorkNameToIdMap.put(workName, new ArrayList<String>(Arrays.asList(workerName)));
                        }
                        LOGGER.info("Discovered Message box count for workname {} is {}", workName,
                                discoveredWorkNameToIdMap.get(workName).size());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Clusterbox Exception {}", e);
            }
            if (hasExpired() && pongPending.get() && PING_RETRY_COUNT.decrementAndGet() == 0) {
                LOGGER.error("Detected Dead broker. Reconnecting ...");
                clusterBoxContext.destroySocket(frontendSocket);
                startFrontEnd();
                resetRetryCounter();
                dirty.set(true);
                isAvailable = false;
            }
            // Check Expiry and Ping on expiry
            if (hasExpired()) {
                sendPing();
                pongPending.set(true);
                pingedAt = System.currentTimeMillis();
            }
        }
        shutdownLatch.countDown();
    }

    private boolean hasExpired() {
        return pingedAt + pingInterval < System.currentTimeMillis();
    }

    @Override
    public void shutDown() {
        for (Entry<String, ZeromqClusterBoxMessageBox> entry : messageBoxes.entrySet()) {
            LOGGER.info("Shutting down messagebox {}", entry.getKey());
            entry.getValue().shutdown();
            entry.getValue().waitForShutdown();
            LOGGER.info("Messagebox {} shutdown", entry.getKey());
        }
        on.set(false);
        try {
            shutdownLatch.await(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("Waiting on shutdown latch got interrupted {}", e);
            Thread.currentThread().interrupt();
        } finally {
            if (frontendSocket != null) {
                clusterBoxContext.destroySocket(frontendSocket);
            }
            if (backendSocket != null) {
                clusterBoxContext.destroySocket(backendSocket);
            }
            if (clusterBoxContext != null) {
                clusterBoxContext.destroy();
            }
        }
    }

    @Override
    public void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox) {
        if (!on.get()) {
            throw new ClusterBoxMessageBoxStartFailed("Clusterbox is not yet started or shutdown");
        }
        if (clusterBoxMessageBox.getMessageHandler() == null) {
            throw new ClusterBoxMessageBoxStartFailed(clusterBoxMessageBox.getMessageBoxName()
                    + " MessageBox Registration failed. MessageHandler is Null");
        }
        ZeromqClusterBoxMessageBox messageBox = new ZeromqClusterBoxMessageBox(clusterBoxMessageBox,
                clusterBoxDropBox, clusterBoxConfig);
        if (null == messageBoxes.putIfAbsent(clusterBoxMessageBox.getMessageBoxId(), messageBox)) {
            LOGGER.info("Starting Messagebox {} and Id {}", clusterBoxMessageBox.getMessageBoxName(),
                    clusterBoxMessageBox.getMessageBoxId());
            messageBox.start();
        } else {
            LOGGER.warn("MessageBox already registered and started");
        }
    }

    @Override
    public void unregisterMessageBox(String clusterBoxMessageBoxId) {
        ZeromqClusterBoxMessageBox messageBox = messageBoxes.get(clusterBoxMessageBoxId);
        try {
            if (messageBox != null) {
                messageBox.shutdown();
            }
        } finally {
            messageBoxes.remove(clusterBoxMessageBoxId);
        }
        LOGGER.info("unregisterMessageBox {} successfully", clusterBoxMessageBoxId);
    }

    @Override
    public ClusterBoxDropBox getDropBox() {
        return this.clusterBoxDropBox;
    }

    @Override
    public boolean isOn() {
        return (on.get() && clusterBoxThread.isAlive());
    }

    private void sendPing() {
        ZMsg pingMsg = new ZMsg();
        pingMsg.add("ping");
        pingMsg.send(frontendSocket);
    }

    private void resetRetryCounter() {
        PING_RETRY_COUNT.set(MAX_PING_RETRY_COUNT);
    }

    private void startFrontEnd() {
        frontendSocket = clusterBoxContext.createSocket(ZMQ.DEALER);
        frontendSocket.setIdentity(clusterBoxConfig.getClusterBoxId().getBytes());
        frontendSocket.connect(clusterBoxConfig.getBrokerConfig().getBackendProtocol() + "://"
                + clusterBoxConfig.getBrokerConfig().getIpAddress() + ":"
                + clusterBoxConfig.getBrokerConfig().getBackendPort());
        ZMsg bootstrapMsg = new ZMsg();
        bootstrapMsg.add(ZeromqClusterBoxConstants.REGISTER_CLUSTER_BOX);
        bootstrapMsg.add(clusterBoxConfig.getClusterBoxName());
        bootstrapMsg.add(String.valueOf(pingInterval * MAX_PING_RETRY_COUNT).getBytes());
        bootstrapMsg.send(frontendSocket);
        LOGGER.info("Started and registered clusterbox {} / {} with broker", clusterBoxConfig.getClusterBoxName(),
                clusterBoxConfig.getClusterBoxId());
        ZFrame readyFrame = new ZFrame(ZeromqClusterBoxConstants.CLUSTER_BOX_READY);
        readyFrame.send(frontendSocket, 0);
    }

    private void startBackEnd() {
        backendSocket = clusterBoxContext.createSocket(ZMQ.ROUTER);
        backendSocket.bind(clusterBoxConfig.getBackendProtocol() + "://" + clusterBoxConfig.getBackendAddress());
    }

    private void reInitPolling() {
        this.pollItems = new Poller(2);
        pollItems.register(frontendSocket, Poller.POLLIN);
        pollItems.register(backendSocket, Poller.POLLIN);
        dirty.set(false);
    }

    protected boolean isAvailable() {
        return isAvailable;
    }

}
