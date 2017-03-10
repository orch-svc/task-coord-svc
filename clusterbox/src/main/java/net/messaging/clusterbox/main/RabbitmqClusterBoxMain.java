package net.messaging.clusterbox.main;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.BasicConfigurator;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;
import net.messaging.clusterbox.rabbitmq.RabbitmqBrokerConfig;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBox;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBoxConfig;

public class RabbitmqClusterBoxMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        BasicConfigurator.configure();
        RabbitmqBrokerConfig brokerConfig = new RabbitmqBrokerConfig("localhost", null, null, null, true);
        RabbitmqClusterBoxConfig clusterBoxConfig = new RabbitmqClusterBoxConfig(brokerConfig, "Ardenwood",
                "Ardenwood-1", "direct");
        clusterBoxConfig.setAutoDeclare(true);
        ClusterBox clusterBox = RabbitmqClusterBox.newClusterBox(clusterBoxConfig);
        clusterBox.start();
        RabbitmqClusterBoxConfig clusterBoxConfig2 = new RabbitmqClusterBoxConfig(brokerConfig, "misson", "misson-2",
                "direct");
        clusterBoxConfig2.setAutoDeclare(true);
        ClusterBox clusterBox2 = RabbitmqClusterBox.newClusterBox(clusterBoxConfig2);
        clusterBox2.start();
        DefaultClusterBoxMessageBox messageBox = new DefaultClusterBoxMessageBox("1234-1", "1234");
        clusterBox2.registerMessageBox(messageBox);
        try {
            Thread.currentThread().sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MyPayload payload = new MyPayload("firstName", 10, "LastName");
        RequestMessage<MyPayload> message = new RequestMessage<MyPayload>(payload);
        message.setCommand(MyPayload.class.getName());
        Address from = new Address("Ardenwood", "34173");
        Address to = new Address("misson", "1234");
        message.setTo(to);
        message.pushToFromChain(from);
        clusterBox.getDropBox().drop(message);
        Future<ResultMessage> response = clusterBox.getDropBox().dropAndReceive(message);
        if (response != null) {
            System.out.println("Got the Result for drop and Receive " + response.get().getPayload());
        }
        clusterBox.shutDown();
        clusterBox2.shutDown();
    }
}
