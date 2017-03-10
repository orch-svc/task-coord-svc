package net.messaging.clusterbox.litemq.broker.zeromq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.litemq.broker.LitemqBroker;
import net.messaging.clusterbox.zeromq.ZeromqClusterBoxConstants;

public class ZeromqBroker implements LitemqBroker, Runnable {

    private AtomicBoolean on = new AtomicBoolean(false);
    private Thread brokerThread;
    private ZContext brokerContext;
    private ZMQ.Socket frontendSocket;
    private ZMQ.Socket backendSocket;
    private ZMQ.Socket defaultCBBackendSocket;
    private ZeromqBrokerConfig config;
    private ConcurrentHashMap<String, Set<ClusterBox>> discoveredClusterBox = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> pingedAt = new ConcurrentHashMap<>();
    private Poller pollItems;
    private static final ZFrame PING_FRAME = new ZFrame(ZeromqClusterBoxConstants.PING_FRAME);

    private static final Integer DEFAULT_IO_THREAD_COUNT = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeromqBroker.class);

    private ZeromqBroker(ZeromqBrokerConfig config) {
        this.config = config;
        this.pollItems = new Poller(2);
        if (config.isAutoStart()) {
            startBroker();
        }
    }

    public static ZeromqBroker newBroker(ZeromqBrokerConfig config) {
        return new ZeromqBroker(config);
    }

    @Override
    public void run() {
        // TODO : Guard Against Exceptions
        while (on.get() && !brokerThread.isInterrupted()) {
            pollItems.poll(1000);
            if (pollItems.getItem(0).isReadable()) {
                ZMsg fromClient = ZMsg.recvMsg(frontendSocket, 0);
                if (fromClient == null) {
                    break;
                }
                String fromClientId = new String(fromClient.removeFirst().getData());
                JSONObject jsonFromClient = new JSONObject(new String(fromClient.peek().getData()));
                LOGGER.info("Message from client {}", jsonFromClient);
                JSONObject jsonAddress = jsonFromClient.getJSONObject("to");
                String clusterBoxName = jsonAddress.getString(ZeromqClusterBoxConstants.CLUSTER_BOX_NAME_KEY);
                if (StringUtils.isEmpty(clusterBoxName)) {
                    String mailBoxId = jsonAddress.getString(ZeromqClusterBoxConstants.CLUSTER_BOX_MAIL_BOX_NAME_KEY);
                    fromClient.addFirst(mailBoxId);
                    LOGGER.info("Message {} to be send to default ClusterBox", fromClient);
                    fromClient.send(defaultCBBackendSocket);
                } else {
                    if (discoveredClusterBox.containsKey(clusterBoxName)) {
                        List<ClusterBox> selectedClusterBoxes = refreshAndGetActiveClusterBox(clusterBoxName);
                        LOGGER.info("Selected ClusterBox  {}", selectedClusterBoxes);
                        if (CollectionUtils.isEmpty(selectedClusterBoxes)) {
                            LOGGER.error("No Active ClusterBox present. Message {} dropped", fromClient);
                            continue;
                        }
                        for (ClusterBox selectedClusterBox : selectedClusterBoxes) {
                            ZMsg message = fromClient.duplicate();
                            message.addFirst(selectedClusterBox.clusterBoxId);
                            LOGGER.info("Message {} send to clusterBoxId {}", fromClient,
                                    selectedClusterBox.clusterBoxId);
                            message.send(backendSocket);
                        }

                    } else {
                        LOGGER.error("No backend available skip request {}", fromClient);
                    }
                }
            } else if (pollItems.getItem(1).isReadable()) {
                ZMsg msg = ZMsg.recvMsg(backendSocket, 0);
                if (msg == null) {
                    break;
                }
                String clusterBoxId = new String(msg.removeFirst().getData());
                setPingedAt(clusterBoxId);
                if (ZeromqClusterBoxConstants.CLUSTER_BOX_READY.equals(new String(msg.getFirst().getData()))) {
                    LOGGER.info("ClusterBox {} is ready", clusterBoxId);
                } else if (ZeromqClusterBoxConstants.CLUSTER_BOX_SHUTDOWN
                        .equals(new String(msg.getFirst().getData()))) {

                } else if (ZeromqClusterBoxConstants.REGISTER_CLUSTER_BOX
                        .equals(new String(msg.peekFirst().getData()))) {
                    msg.removeFirst();
                    String clusterBoxName = new String(msg.getFirst().getData());
                    Long pingInterval = Long.valueOf(new String(msg.getLast().getData()));
                    ClusterBox clusterBox = new ClusterBox(clusterBoxName, clusterBoxId, pingInterval);
                    if (discoveredClusterBox.containsKey(clusterBoxName)) {
                        discoveredClusterBox.get(clusterBoxName).add(clusterBox);
                    } else {
                        discoveredClusterBox.put(clusterBoxName, new HashSet<ClusterBox>(Arrays.asList(clusterBox)));
                    }
                    LOGGER.info("Registers Cluster box {} with name {}", clusterBoxId, clusterBoxName);
                    LOGGER.info("ClusterBoxes {} count {}", clusterBoxName,
                            discoveredClusterBox.get(clusterBoxName).size());
                } else if (PING_FRAME.equals(msg.getFirst())) {
                    // Reply back to finish the pong
                    msg.addFirst(clusterBoxId);
                    msg.send(backendSocket);
                } else {
                    LOGGER.error("Un Supported Message {}", msg);
                }
            } else if (pollItems.getItem(2).isReadable()) {
                ZMsg msg = ZMsg.recvMsg(defaultCBBackendSocket, 0);
                if (msg == null) {
                    break;
                }
            }

        }
        if (frontendSocket != null) {
            brokerContext.destroySocket(frontendSocket);
        }
        if (backendSocket != null) {
            brokerContext.destroySocket(backendSocket);
        }
        if (defaultCBBackendSocket != null) {
            brokerContext.destroySocket(defaultCBBackendSocket);
        }
        if (brokerContext != null) {
            brokerContext.destroy();
        }
        LOGGER.info("zeromq Broker shutdown");
    }

    public void startBroker() {
        LOGGER.info("Starting zeromq Broker");
        brokerContext = new ZContext(DEFAULT_IO_THREAD_COUNT);
        frontendSocket = brokerContext.createSocket(ZMQ.ROUTER);
        backendSocket = brokerContext.createSocket(ZMQ.ROUTER);
        defaultCBBackendSocket = brokerContext.createSocket(ZMQ.ROUTER);

        frontendSocket
        .bind(config.getFrontendProtocol() + "://" + config.getIpAddress() + ":" + config.getFrontendPort());
        backendSocket.bind(config.getBackendProtocol() + "://" + config.getIpAddress() + ":" + config.getBackendPort());
        defaultCBBackendSocket.bind(config.getBackendProtocol() + "://" + config.getIpAddress() + ":"
                + ZeromqClusterBoxConstants.DEFAILT_CB_BACKEND_PORT);

        pollItems.register(frontendSocket, Poller.POLLIN);
        pollItems.register(backendSocket, Poller.POLLIN);
        pollItems.register(defaultCBBackendSocket, Poller.POLLIN);

        brokerThread = new Thread(this, "zeromq-broker");
        on = new AtomicBoolean(true);
        brokerThread.start();
    }

    public void stopBroker() {
        on.set(false);
    }

    public void dumpClusterBox() {
        try {
            LOGGER.info("Discovered ClusterBoxes - {}", new ObjectMapper().writeValueAsString(discoveredClusterBox));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void setPingedAt(String clusterBoxId) {
        pingedAt.put(clusterBoxId, System.currentTimeMillis());
    }

    private List<ClusterBox> refreshAndGetActiveClusterBox(String clusterBoxName) {
        if (discoveredClusterBox.get(clusterBoxName) == null) {
            return null;
        }
        Iterator<ClusterBox> iterator = discoveredClusterBox.get(clusterBoxName).iterator();
        List<ClusterBox> result = new ArrayList<>();
        while (iterator.hasNext()) {
            ClusterBox clusterBox = iterator.next();
            LOGGER.info("Verifying cluster box {}. It was pingedAt {}", clusterBox,
                    pingedAt.get(clusterBox.clusterBoxId));
            long timeOutAt = pingedAt.get(clusterBox.clusterBoxId) + clusterBox.pingInterval;
            if (System.currentTimeMillis() < timeOutAt) {
                result.add(clusterBox);
            } else {
                LOGGER.info("Removing Dead Cluster Box {} as no ping for {}", clusterBox,
                        System.currentTimeMillis() - timeOutAt);
                iterator.remove();
            }
        }
        if (discoveredClusterBox.get(clusterBoxName).size() == 0) {
            discoveredClusterBox.remove(clusterBoxName);
        }
        return result;

    }

    private static class ClusterBox {
        private String clusterBoxName;
        private String clusterBoxId;
        private Long pingInterval;

        private ClusterBox(String clusterBoxName, String clusterBoxId, Long pingInterval) {
            super();
            this.clusterBoxName = clusterBoxName;
            this.clusterBoxId = clusterBoxId;
            this.pingInterval = pingInterval;
        }

        @Override
        public String toString() {
            return "ClusterBox [clusterBoxName=" + clusterBoxName + ", clusterBoxId=" + clusterBoxId + ", pingInterval="
                    + pingInterval + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((clusterBoxId == null) ? 0 : clusterBoxId.hashCode());
            result = prime * result + ((clusterBoxName == null) ? 0 : clusterBoxName.hashCode());
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
            ClusterBox other = (ClusterBox) obj;
            if (clusterBoxId == null) {
                if (other.clusterBoxId != null)
                    return false;
            } else if (!clusterBoxId.equals(other.clusterBoxId))
                return false;
            if (clusterBoxName == null) {
                if (other.clusterBoxName != null)
                    return false;
            } else if (!clusterBoxName.equals(other.clusterBoxName))
                return false;
            return true;
        }

    }

}
