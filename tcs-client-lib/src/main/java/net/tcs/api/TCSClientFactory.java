package net.tcs.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.task.coordinator.service.config.TCSClusterConfig;
import net.task.coordinator.service.config.TCSRMQTransportConfig;
import net.task.coordinator.service.config.TCSTransportMode;
import net.task.coordinator.service.config.TCSZeromqTransportConfig;
import net.tcs.client.runtime.TCSRmqBasedClientRuntime;
import net.tcs.client.runtime.TCSZmqBasedClientRuntime;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;

public class TCSClientFactory {
    /**
     * Create a Job Spec
     *
     * @param jobName
     * @return
     */
    public static JobSpec createJobSpec(String jobName) {
        return new JobDefinition(jobName);
    }

    /**
     * Create a TCSClientRuntime
     *
     * @return
     */
    public static TCSClient createTCSClient(int numPartitions, String rmqBroker) {
        return new TCSRmqBasedClientRuntime(numPartitions, rmqBroker);
    }

    /**
     * Create a TCSClientRuntime based on a config file
     *
     * @throws IOException
     */
    public static TCSClient createTCSClient(String configFile) throws IOException {
        byte[] configData = null;
        TCSTransportMode transportConfig = TCSTransportMode.RABBITMQ;
        int numPartitions;
        String brokerAddress = null;
        ;
        try {
            configData = Files.readAllBytes(Paths.get(configFile));
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> configMap = mapper.readValue(configData, Map.class);
            TCSClusterConfig clusterConfig = mapper.convertValue(configMap.get("clusterConfig"),
                    new TypeReference<TCSClusterConfig>() {
            });
            numPartitions = clusterConfig.getNumPartitions();
            if (configMap.containsKey("tcsTransportMode")) {
                transportConfig = TCSTransportMode.valueOf((String) configMap.get("tcsTransportMode"));
            }
            if (TCSTransportMode.ZEROMQ == transportConfig) {
                TCSZeromqTransportConfig zmqConfig = mapper.convertValue(configMap.get("zeromqConfig"),
                        new TypeReference<TCSZeromqTransportConfig>() {
                });
                return new TCSZmqBasedClientRuntime(numPartitions, zmqConfig);
            } else {
                TCSRMQTransportConfig rmqConfig = mapper.convertValue(configMap.get("rabbitConfig"),
                        new TypeReference<TCSRMQTransportConfig>() {
                });
                brokerAddress = rmqConfig.getBrokerAddress();
            }
        } catch (IOException e) {
            System.out.println("Error " + e);
            throw e;
        }
        System.out.println("Using Brokeraddress " + brokerAddress + ": partitions  " + numPartitions);
        return new TCSRmqBasedClientRuntime(numPartitions, brokerAddress);
    }
}
