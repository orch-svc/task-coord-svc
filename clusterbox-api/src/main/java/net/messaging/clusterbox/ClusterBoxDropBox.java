package net.messaging.clusterbox;

import java.util.concurrent.Future;

import net.messaging.clusterbox.exception.ClusterBoxMessageSendFailed;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;

/*
 * Producer Interface to produce the message to external or required destination
 * */
public interface ClusterBoxDropBox {

    void drop(Message<?> message) throws ClusterBoxMessageSendFailed;

    void shutdown();

    Future<ResultMessage> dropAndReceive(RequestMessage<?> message) throws ClusterBoxMessageSendFailed;
}
