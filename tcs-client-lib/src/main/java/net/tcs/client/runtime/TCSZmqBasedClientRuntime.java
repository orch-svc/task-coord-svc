package net.tcs.client.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBrokerConfig;
import net.messaging.clusterbox.zeromq.ZeromqClusterBox;
import net.messaging.clusterbox.zeromq.ZeromqClusterBoxConfig;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.service.config.TCSZeromqTransportConfig;

public final class TCSZmqBasedClientRuntime extends TCSCBBasedClientRuntime {

    private volatile boolean initialized = false;
    private TCSZeromqTransportConfig tcsZeromqTransportConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSZmqBasedClientRuntime.class);

    public TCSZmqBasedClientRuntime(int numPartitions, TCSZeromqTransportConfig tcszeromqTransportConfig) {
        super(numPartitions, tcszeromqTransportConfig.getIpAddress());
        this.tcsZeromqTransportConfig = tcszeromqTransportConfig;
    }

    @Override
    public void initialize() {
        if (initialized) {
            LOGGER.info("Client ZMQ Based runtime already intialized");
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            ZeromqBrokerConfig brokerConfig = new ZeromqBrokerConfig(tcsZeromqTransportConfig.getIpAddress(),
                    tcsZeromqTransportConfig.getFrontendPort(), tcsZeromqTransportConfig.getBackendPort(),
                    tcsZeromqTransportConfig.getFrontendProtocol(), tcsZeromqTransportConfig.getBackendProtocol());
            ZeromqClusterBoxConfig clusterBoxConfig = new ZeromqClusterBoxConfig(brokerConfig,
                    TCSConstants.TCS_EXECUTOR_EXCHANGE,
                    String.format("%s.%s", TCSConstants.TCS_EXECUTOR_EXCHANGE, TCSConstants.TCS_EXECUTOR_EXCHANGE));
            clusterBox = ZeromqClusterBox.newClusterBox(clusterBoxConfig);
            clusterBox.start();
            initialized = true;
            LOGGER.info("Client ZMQ Based runtime intialized");
        }
    }

    @Override
    public void cleanup() {

        if (initialized && clusterBox != null) {
            LOGGER.info("Shutting down Clusterbox {} / {}", clusterBox.getClusterBoxConfig().getClusterBoxName(),
                    clusterBox.getClusterBoxConfig().getClusterBoxId());
            clusterBox.shutDown();
            LOGGER.info("Clusterbox shut down");
        }
    }

}
