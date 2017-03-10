package net.messaging.clusterbox.test.resource;

import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;

public class TestMessageBox implements ClusterBoxMessageBox {

    /*
     * Queue name
     */
    private final String messageBoxId;
    /*
     * Routingkey
     */
    private final String messageBoxName;
    private final ClusterBoxMessageHandler<?> messageHandler;

    public TestMessageBox(String messageBoxId, String messageBoxName, ClusterBoxMessageHandler<?> messageHandler) {
        super();
        this.messageBoxId = messageBoxId;
        this.messageBoxName = messageBoxName;
        this.messageHandler = messageHandler;
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
        return messageHandler;
    }

}
