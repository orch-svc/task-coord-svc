package net.tcs.drivers;

import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBroker;
import net.messaging.clusterbox.litemq.broker.zeromq.ZeromqBrokerConfig;
import net.messaging.clusterbox.rabbitmq.RabbitmqBrokerConfig;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBox;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBoxConfig;
import net.messaging.clusterbox.zeromq.ZeromqClusterBox;
import net.messaging.clusterbox.zeromq.ZeromqClusterBoxConfig;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.service.config.TCSTransportMode;
import net.task.coordinator.service.config.TCSZeromqTransportConfig;
import net.tcs.config.TCSClusterConfigHolder;
import net.tcs.config.TCSConfig;
import net.tcs.db.ActiveJDBCAdapter;
import net.tcs.messagebox.TcsJobQueryMessageBox;
import net.tcs.messagebox.TcsJobRegisterMessageBox;
import net.tcs.shard.TCSShardLock;

public abstract class TCSDriverBase {
    private ClusterBox clusterBox = null;

    private ZeromqBroker broker = null;

    public abstract void initialize(String instanceName) throws Exception;

    public void cleanup() {
        if (clusterBox != null) {
            clusterBox.shutDown();
        }

        if (TCSDriver.getDbAdapter() != null) {
            TCSDriver.getDbAdapter().cleanup();
        }

        if (broker != null) {
            broker.stopBroker();
        }
    }

    protected void initializeRMQ() {
        // TODO : Move it to a different place after testing.
        TCSConfig config = TCSClusterConfigHolder.getConfig();
        TCSTransportMode tcsTransportMode = config.getTcsTransportMode();
        if (TCSTransportMode.RABBITMQ == tcsTransportMode) {
            RabbitmqBrokerConfig brokerConfig = new RabbitmqBrokerConfig(config.getRabbitConfig().getBrokerAddress(), null,
                    null, null, true);
            RabbitmqClusterBoxConfig clusterBoxConfig = new RabbitmqClusterBoxConfig(brokerConfig,
                    TCSConstants.TCS_EXCHANGE, String.format("%s.%s", TCSConstants.TCS_EXCHANGE, TCSConstants.TCS_EXCHANGE),
                    "topic");
            clusterBox = RabbitmqClusterBox.newClusterBox(clusterBoxConfig);
            System.out.println(
                    "Connected to RabbitMQ Broker: " + TCSDriver.getConfig().getRabbitConfig().getBrokerAddress());
        } else if (TCSTransportMode.ZEROMQ == tcsTransportMode) {
            TCSZeromqTransportConfig transportConfig = config.getZeromqConfig();
            ZeromqBrokerConfig brokerConfig = new ZeromqBrokerConfig(transportConfig.getIpAddress(),
                    transportConfig.getFrontendPort(), transportConfig.getBackendPort(),
                    transportConfig.getFrontendProtocol(), transportConfig.getBackendProtocol());
            broker = ZeromqBroker.newBroker(brokerConfig);
            broker.startBroker();
            ZeromqClusterBoxConfig clusterBoxConfig = new ZeromqClusterBoxConfig(brokerConfig,
                    TCSConstants.TCS_EXCHANGE,
                    String.format("%s.%s", TCSConstants.TCS_EXCHANGE, TCSConstants.TCS_EXCHANGE));
            clusterBox = ZeromqClusterBox.newClusterBox(clusterBoxConfig);
            System.out.println("Connected to ZerrorMq Broker: " + transportConfig.getIpAddress());

        } else {
            System.out.println("Unsupported Transport mode");
            throw new IllegalArgumentException("Unsupported Transport mode");
        }
        clusterBox.start();
        clusterBox.registerMessageBox(new TcsJobQueryMessageBox());
        clusterBox.registerMessageBox(new TcsJobRegisterMessageBox());

    }

    protected void initializeDB() {
        final ActiveJDBCAdapter dbAdapter = new ActiveJDBCAdapter();

        dbAdapter.initialize(TCSDriver.getConfig().getDbConfig().getDbConnectString(),
                TCSDriver.getConfig().getDbConfig().getUserName(), TCSDriver.getConfig().getDbConfig().getPassword());
        TCSDriver.setDbAdapter(dbAdapter);

        System.out.println("Connected to Database: " + TCSDriver.getConfig().getDbConfig().getDbConnectString());
    }

    protected void initializePartitions() {
        for (int i = 0; i < TCSDriver.getConfig().getClusterConfig().getNumPartitions(); i++) {
            final String shardName = String.format("%s_%d",
                    TCSDriver.getConfig().getClusterConfig().getShardGroupName(), i);
            final TCSShardLock shardLock = new TCSShardLock(shardName);
            shardLock.initializeShard(false);
        }
    }
}
