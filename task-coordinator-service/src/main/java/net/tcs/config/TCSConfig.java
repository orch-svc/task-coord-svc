package net.tcs.config;

import java.util.Arrays;

import net.task.coordinator.service.config.TCSClusterConfig;
import net.task.coordinator.service.config.TCSDataSourceConfig;
import net.task.coordinator.service.config.TCSRMQTransportConfig;
import net.task.coordinator.service.config.TCSTransportMode;
import net.task.coordinator.service.config.TCSZeromqTransportConfig;
import net.tcs.drivers.TCSDeploymentMode;

public class TCSConfig {

    public TCSConfig() {
    }

    public String getZookeeperConnectString() {
        return zookeeperConnectString;
    }

    public void setZookeeperConnectString(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
    }

    public TCSClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public void setClusterConfig(TCSClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public TCSRMQTransportConfig getRabbitConfig() {
        return rabbitConfig;
    }

    public void setRabbitConfig(TCSRMQTransportConfig rabbitConfig) {
        this.rabbitConfig = rabbitConfig;
    }

    public TCSDataSourceConfig getDbConfig() {
        return dbConfig;
    }

    public void setDbConfig(TCSDataSourceConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public TCSDeploymentMode getTcsDeploymentMode() {
        return tcsDeploymentMode;
    }

    public void setTcsDeploymentMode(String tcsMode) {
        try {
            this.tcsDeploymentMode = TCSDeploymentMode.valueOf(TCSDeploymentMode.class, tcsMode);
        } catch (final IllegalArgumentException ex) {
            System.out.println(
                    "Permissible Values for TCS DeploymentMode are: " + Arrays.asList(TCSDeploymentMode.values()));
            throw ex;
        }
    }

    public TCSTransportMode getTcsTransportMode() {
        return tcsTransportMode;
    }

    public void setTcsTransportMode(String tcsTransportMode) {
        try {
            this.tcsTransportMode = TCSTransportMode.valueOf(TCSTransportMode.class, tcsTransportMode);
        } catch (IllegalArgumentException e) {
            System.out.println(
                    "Permissible Values for TCS TransportMode are: " + Arrays.asList(TCSTransportMode.values()));
            throw e;
        }
    }

    public TCSZeromqTransportConfig getZeromqConfig() {
        return zeromqConfig;
    }

    public void setzeromqConfig(TCSZeromqTransportConfig zeromqConfig) {
        this.zeromqConfig = zeromqConfig;
    }

    private TCSDeploymentMode tcsDeploymentMode = TCSDeploymentMode.MULTI_INSTANCE;
    private String zookeeperConnectString;
    private TCSClusterConfig clusterConfig;
    private TCSRMQTransportConfig rabbitConfig;
    private TCSDataSourceConfig dbConfig;
    private TCSTransportMode tcsTransportMode = TCSTransportMode.RABBITMQ;
    private TCSZeromqTransportConfig zeromqConfig;
}

