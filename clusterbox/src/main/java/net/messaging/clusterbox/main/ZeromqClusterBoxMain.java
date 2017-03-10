package net.messaging.clusterbox.main;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBroker;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBrokerConfig;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.zeromq.ZeromqClusterBox;
import net.messaging.clusterbox.zeromq.ZeromqClusterBoxConfig;

public class ZeromqClusterBoxMain {

    ConcurrentHashMap<String, ClusterBox> clusterBoxes = new ConcurrentHashMap<>();
    ZeromqBrokerConfig brokerConfig = new ZeromqBrokerConfig("localhost", "5555", "6666", "tcp", "tcp");
    private static final Logger LOGGER = LoggerFactory.getLogger(ZeromqClusterBoxMain.class);
    ZeromqBroker broker = null;

    @CliRunner(command = "startbroker")
    public void startBroker() {
        broker = ZeromqBroker.newBroker(brokerConfig);
        broker.startBroker();
    }

    @CliRunner(command = "stopbroker")
    public void stopBroker() {
        broker.stopBroker();
    }

    @CliRunner(command = "startclusterbox")
    public void startClusterbox(String clusterBoxName, String clusterBoxId) {
        ZeromqClusterBoxConfig clusterBoxConfig = new ZeromqClusterBoxConfig(brokerConfig, clusterBoxName,
                clusterBoxId);
        ClusterBox clusterBox = ZeromqClusterBox.newClusterBox(clusterBoxConfig);
        clusterBox.start();
        clusterBoxes.putIfAbsent(clusterBoxId, clusterBox);
    }

    @CliRunner(command = "stopclusterbox")
    public void stopClusterbox(String clusterBoxId) {
        if (clusterBoxes.get(clusterBoxId) != null) {
            if (clusterBoxes.get(clusterBoxId).isOn()) {
                clusterBoxes.get(clusterBoxId).shutDown();
            } else {
                LOGGER.error("Already shutdown");
            }
        } else {
            LOGGER.error("No ClusterBox found hence not able to register message box");
        }
    }

    @CliRunner(command = "startmessagebox")
    public void startMessageBox(String messageBoxName, String messageBoxId, String clusterBoxId) {
        if (clusterBoxes.get(clusterBoxId) != null) {
            DefaultClusterBoxMessageBox messageBox = new DefaultClusterBoxMessageBox(messageBoxId, messageBoxName);
            clusterBoxes.get(clusterBoxId).registerMessageBox(messageBox);
        } else {
            LOGGER.error("No ClusterBox found hence not able to register message box");
        }

    }

    @CliRunner(command = "sendmessage")
    public void sendMessage() {
        MyPayload payload = new MyPayload("firstName", 10, "LastName");
        RequestMessage<MyPayload> message = new RequestMessage<MyPayload>(payload);
        message.setCommand(MyPayload.class.getName());
        Address from = new Address("Ardenwood", "34173");
        Address to = new Address("misson", "1234");
        message.setTo(to);
        message.pushToFromChain(from);
        clusterBoxes.get("Ardenwood-1").getDropBox().drop(message);
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        ZeromqClusterBoxMain main = new ZeromqClusterBoxMain();
        new CommandRunner(main);
    }
}
