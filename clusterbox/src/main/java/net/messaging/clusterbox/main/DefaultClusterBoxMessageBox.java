package net.messaging.clusterbox.main;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

public class DefaultClusterBoxMessageBox implements ClusterBoxMessageBox {

    /*
     * Queue name
     */
    private final String messageBoxId;
    /*
     * Routingkey
     */
    private final String messageBoxName;
    private ClusterBoxMessageHandler<?> messageHandler;

    public DefaultClusterBoxMessageBox(String messageBoxId, String messageBoxName) {
        super();
        this.messageBoxId = messageBoxId;
        this.messageBoxName = messageBoxName;
    }

    @Override
    public String getMessageBoxId() {
        return messageBoxId;
    }

    @Override
    public String getMessageBoxName() {
        return messageBoxName;
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        if (messageHandler == null) {
            messageHandler = new ClusterBoxMessageHandler<MyPayload>() {
                @Override
                public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {
                    MyPayload payload = (MyPayload) message.getPayload();
                    System.out.println("Got payload in " + payload + "in Message Box " + messageBoxName);
                    Address toAddress = message.popFromChain();
                    System.out.println("Return Address " + toAddress);
                    ResultMessage<MyPayload> result = new ResultMessage<MyPayload>(payload);
                    result.setTo(toAddress);
                    dropBox.drop(result);
                }

                @Override
                public String getRequestName() {
                    return MyPayload.class.getName();
                }

            };
        }
        return messageHandler;
    }

}
