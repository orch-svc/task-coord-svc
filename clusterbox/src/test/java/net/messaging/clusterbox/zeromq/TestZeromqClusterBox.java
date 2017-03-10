package net.messaging.clusterbox.zeromq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBroker;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBrokerConfig;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;
import net.messaging.clusterbox.test.resource.BiDirectionalMessageHandler;
import net.messaging.clusterbox.test.resource.TestMessageBox;
import net.messaging.clusterbox.test.resource.TestPayload;
import net.messaging.clusterbox.test.resource.UniDirectionalMessageHandler;

public class TestZeromqClusterBox {

    ZeromqBrokerConfig brokerConfig = new ZeromqBrokerConfig("localhost", "6551", "5661", "tcp", "tcp");
    ZeromqBroker broker = null;
    ClusterBox clusterBox = null;

    private static final String TEST_CLUSTER_BOX_NAME = "test-cluster-box";
    private static final String TEST_CLUSTER_BOX_ID = String.format("%s.%s", "test-cluster-box", "id");
    private static final Logger LOGGER = LoggerFactory.getLogger(TestZeromqClusterBox.class);
    private ArrayList<Holder> uniDirectionalHolders;
    private ArrayList<Holder> biDirectionalHolders;

    @BeforeTest
    public void init() {
        BasicConfigurator.configure();
        broker = ZeromqBroker.newBroker(brokerConfig);
        broker.startBroker();
        LOGGER.info("Started Broker  !!");
        ZeromqClusterBoxConfig clusterBoxConfig = new ZeromqClusterBoxConfig(brokerConfig, TEST_CLUSTER_BOX_NAME,
                TEST_CLUSTER_BOX_ID);
        clusterBox = ZeromqClusterBox.newClusterBox(clusterBoxConfig);
        clusterBox.start();
        LOGGER.info("Started ClusterBox  !!");
        initCBRuntime();
    }

    @AfterClass
    public void cleanup() {
        LOGGER.info("Cleanning up clusterBox runtime");
        if (clusterBox != null) {
            clusterBox.shutDown();
            LOGGER.info("ClusterBox shutdown");
        }
        if (broker != null) {
            broker.stopBroker();
            LOGGER.info("Broker shutdown");
        }
    }

    @Test
    public void testClusterBoxStabilityAtScale() {
        int i = 0;
        while (i <= 10) {
            try {
                Thread.currentThread().sleep(5000);
            } catch (InterruptedException e) {

            }
            Assert.assertEquals(((ZeromqClusterBox) clusterBox).isAvailable(), true, "Failed at Iteration " + i);
            i++;
        }
    }

    @Test
    public void testBasicMessageDropAndUnregister() throws IOException, InterruptedException, ExecutionException {
        SynchronousQueue<TestPayload> queue = new SynchronousQueue<>();
        UniDirectionalMessageHandler<TestPayload> handler = new UniDirectionalMessageHandler<TestPayload>(queue);
        TestMessageBox messageBox = new TestMessageBox(String.format("testcase-%s-id", 1),
                String.format("testcase-%s-name", 1), handler);
        clusterBox.registerMessageBox(messageBox);
        TestPayload payload = TestPayload.getSamplePaylod();
        RequestMessage<TestPayload> message = new RequestMessage<TestPayload>(payload);
        message.setTo(new Address(TEST_CLUSTER_BOX_NAME, String.format("testcase-%s-name", 1)));
        clusterBox.getDropBox().drop(message);
        try {
            TestPayload result = queue.poll(1000, TimeUnit.MILLISECONDS);
            Assert.assertEquals(payload, result);
        } catch (Exception e) {
            Assert.fail("Did not receive sent message");
        }
        clusterBox.unregisterMessageBox(String.format("testcase-%s-id", 1));
        clusterBox.getDropBox().drop(message);
        try {
            TestPayload result = queue.poll(100, TimeUnit.MILLISECONDS);
            Assert.assertEquals(null, result);
        } catch (Exception e) {
            Assert.fail("Did not receive sent message");
        }
    }

    @Test
    public void testBasicMessageDropAndReceiveAndUnregister()
            throws IOException, InterruptedException, ExecutionException {
        BlockingQueue<TestPayload> queue = new LinkedBlockingQueue<>();
        BiDirectionalMessageHandler<TestPayload> handler = new BiDirectionalMessageHandler<TestPayload>(queue);
        TestMessageBox messageBox = new TestMessageBox(String.format("testcase-%s-id", 2),
                String.format("testcase-%s-name", 2), handler);
        clusterBox.registerMessageBox(messageBox);
        TestPayload payload = TestPayload.getSamplePaylod();
        RequestMessage<TestPayload> message = new RequestMessage<TestPayload>(payload);
        message.setTo(new Address(TEST_CLUSTER_BOX_NAME, String.format("testcase-%s-name", 2)));
        Future<ResultMessage> reply = clusterBox.getDropBox().dropAndReceive(message);
        try {
            ResultMessage resultMessage = reply.get(5000, TimeUnit.MILLISECONDS);
            Assert.assertEquals(resultMessage.getPayload(), payload);
        } catch (TimeoutException e) {
            Assert.fail("Excepting in getting the value from future");
        } finally {
            clusterBox.unregisterMessageBox(String.format("testcase-%s-id", 2));
        }
    }

    @Test
    public void testMultipleMessageDrop() throws IOException {

        for (Holder holder : uniDirectionalHolders) {
            clusterBox.getDropBox().drop(holder.message);
        }
        for (Holder holder : uniDirectionalHolders) {
            try {
                TestPayload result = holder.queue.poll(5000, TimeUnit.MILLISECONDS);
                Assert.assertEquals(result, holder.message.getPayload());
            } catch (Exception e) {
                Assert.fail("Did not receive sent message");
            }
        }
    }

    @Test
    public void testMultipleMessageDropAndReceive() throws IOException {
        for (Holder holder : biDirectionalHolders) {
            Future<ResultMessage> future = clusterBox.getDropBox().dropAndReceive((RequestMessage) holder.message);
            holder.future = future;
        }
        for (Holder holder : biDirectionalHolders) {
            try {
                TestPayload actual = (TestPayload) holder.future.get(5000, TimeUnit.MILLISECONDS).getPayload();
                TestPayload expected = holder.queue.poll(5000, TimeUnit.MILLISECONDS);
                Assert.assertEquals(actual, expected);
            } catch (Exception e) {
                Assert.fail("Did not receive sent message");
            }
        }
    }

    private void initCBRuntime() {
        uniDirectionalHolders = new ArrayList<>();
        biDirectionalHolders = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            BlockingQueue<TestPayload> queue = new LinkedBlockingQueue<>();
            UniDirectionalMessageHandler<TestPayload> handler = new UniDirectionalMessageHandler<>(queue);
            String messageBoxName = String.format("message-box-%s-name", i);
            String messageBoxId = String.format("message-box-%s-id", i);
            TestMessageBox messageBox = new TestMessageBox(messageBoxId, messageBoxName, handler);
            clusterBox.registerMessageBox(messageBox);
            TestPayload payload = TestPayload.getSamplePaylod();
            RequestMessage<TestPayload> message = new RequestMessage<TestPayload>(payload);
            message.setTo(new Address(TEST_CLUSTER_BOX_NAME, messageBoxName));
            uniDirectionalHolders.add(new Holder(queue, message, messageBox));
        }

        for (int i = 21; i <= 40; i++) {
            BlockingQueue<TestPayload> queue = new LinkedBlockingQueue<>();
            BiDirectionalMessageHandler<TestPayload> handler = new BiDirectionalMessageHandler<>(queue);
            String messageBoxName = String.format("message-box-%s-name", i);
            String messageBoxId = String.format("message-box-%s-id", i);
            TestMessageBox messageBox = new TestMessageBox(messageBoxId, messageBoxName, handler);
            clusterBox.registerMessageBox(messageBox);
            TestPayload payload = TestPayload.getSamplePaylod();
            RequestMessage<TestPayload> message = new RequestMessage<TestPayload>(payload);
            message.setTo(new Address(TEST_CLUSTER_BOX_NAME, messageBoxName));
            biDirectionalHolders.add(new Holder(queue, message, messageBox));
        }
    }

    private class Holder {
        private BlockingQueue<TestPayload> queue;
        private Message message;
        private TestMessageBox messageBox;
        private Future<ResultMessage> future;

        protected Holder(BlockingQueue queue, Message message, TestMessageBox messageBox) {
            this.queue = queue;
            this.message = message;
            this.messageBox = messageBox;
        }
    }
}
