package net.tcs.client.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.messaging.clusterbox.rabbitmq.RabbitmqBrokerConfig;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBox;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBoxConfig;
import net.task.coordinator.base.message.TCSConstants;

public final class TCSRmqBasedClientRuntime extends TCSCBBasedClientRuntime {

    private volatile boolean initialized = false;
    private RabbitmqBrokerConfig brokerConfig;
    private RabbitmqClusterBoxConfig clusterBoxConfig;

    private static final Logger LOGGER = LoggerFactory.getLogger(TCSRmqBasedClientRuntime.class);

    public TCSRmqBasedClientRuntime(int numPartitions, String rmqBrokerAddress) {
        super(numPartitions, rmqBrokerAddress);
        brokerConfig = new RabbitmqBrokerConfig(rmqBrokerAddress, null, null, null, true);
        clusterBoxConfig = new RabbitmqClusterBoxConfig(brokerConfig, TCSConstants.TCS_EXECUTOR_EXCHANGE,
                String.format("%s.%s", TCSConstants.TCS_EXECUTOR_EXCHANGE, TCSConstants.TCS_EXECUTOR_EXCHANGE),
                "topic");
    }

    @Override
    public void initialize() {
        if (!initialized) {
            synchronized (this) {
                if (initialized) {
                    return;
                }
                clusterBoxConfig.setAutoDeclare(true);
                clusterBox = RabbitmqClusterBox.newClusterBox(clusterBoxConfig);
                clusterBox.start();
                initialized = true;
            }
            LOGGER.info("Client Runtime Initialized");
        } else {
            LOGGER.info("Client Runtime all ready Initialized");
        }
    }

    @Override
    public void cleanup() {
        if (initialized && clusterBox != null) {
            clusterBox.shutDown();
        }
        LOGGER.info("Shut down Client Runtime");
    }

}
