package net.tcs.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TCSConfigReader {

    private static String DEFAULT_CONFIG_FILE_RELATIVE_PATH = "conf/config.json";

    public static TCSConfig readConfig(String configFile) throws IOException {
        final byte[] jsonData = Files.readAllBytes(Paths.get(configFile));
        final ObjectMapper objectMapper = new ObjectMapper();
        final TCSConfig config = objectMapper.readValue(jsonData, TCSConfig.class);
        return config;
    }

    public static TCSConfig readConfig() throws IOException {
        return readConfig(DEFAULT_CONFIG_FILE_RELATIVE_PATH);
    }

    public static List<String> getVNodes(TCSConfig config) {
        final List<String> vNodes = new ArrayList<>();

        for (int i = 0; i < config.getClusterConfig().getNumPartitions(); i++) {
            vNodes.add(String.format("%s_%d", config.getClusterConfig().getShardGroupName(), i));
        }
        System.out.println("VNodes: " + Arrays.toString(vNodes.toArray(new String[vNodes.size()])));
        return vNodes;
    }
}
