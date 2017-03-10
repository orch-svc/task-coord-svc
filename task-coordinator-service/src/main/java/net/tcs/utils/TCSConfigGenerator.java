package net.tcs.utils;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.service.config.TCSRMQTransportConfig;
import net.task.coordinator.service.config.TCSClusterConfig;
import net.task.coordinator.service.config.TCSDataSourceConfig;
import net.tcs.config.TCSConfig;;

public class TCSConfigGenerator {

    public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException {
        final TCSConfig config = new TCSConfig();

        // config.setZookeeperConnectString("172.24.100.253:2181,172.24.100.230:2181,172.24.100.92:2181");
        config.setZookeeperConnectString("localhost:2181");
        final TCSDataSourceConfig dbConfig = new TCSDataSourceConfig();
        dbConfig.setDbConnectString("jdbc:mysql://localhost:3306/tcsdb");
        dbConfig.setUserName("root");
        dbConfig.setPassword("root");
        config.setDbConfig(dbConfig);

        final TCSClusterConfig clusterConfig = new TCSClusterConfig();
        clusterConfig.setClusterName("APIC-TCS");
        clusterConfig.setShardGroupName(TCSConstants.TCS_SHARD_GROUP_NAME);
        clusterConfig.setNumPartitions(8);
        config.setClusterConfig(clusterConfig);

        final TCSRMQTransportConfig rmqConfig = new TCSRMQTransportConfig();
        rmqConfig.setBrokerAddress("172.24.100.253");
        config.setRabbitConfig(rmqConfig);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File("config.json"), config);
    }
}
