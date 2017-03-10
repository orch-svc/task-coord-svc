package net.tcs.utils;

import java.io.IOException;
import java.io.InputStream;

import org.testng.reporters.Files;

import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Assert;
import net.task.coordinator.service.config.TCSTransportMode;
import net.task.coordinator.service.config.TCSZeromqTransportConfig;
import net.tcs.config.TCSConfig;

public class TCSConfigTest {

    private static String DEFAULT_CONFIG_FILE_RELATIVE_PATH = "./config/testConfig.json";

    public void testTCSConfigIsCreatedAsExpected() throws IOException {
        String tcsConfigString = null;
        System.out.println(this.getClass().getResource(DEFAULT_CONFIG_FILE_RELATIVE_PATH).getFile());

        System.out.println(this.getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE_RELATIVE_PATH));
        try (InputStream fileStream = this.getClass().getResourceAsStream(DEFAULT_CONFIG_FILE_RELATIVE_PATH)) {
            tcsConfigString = Files.readFile(fileStream);
        }
        Assert.assertNotNull(tcsConfigString);
        ObjectMapper mapper = new ObjectMapper();
        TCSConfig tcsConfig = mapper.readValue(tcsConfigString, TCSConfig.class);
        Assert.assertNotNull(tcsConfig);
        TCSZeromqTransportConfig zeromqBrokerConfig = tcsConfig.getZeromqConfig();
        Assert.assertNotNull(zeromqBrokerConfig);
        TCSTransportMode tcsTransportMode = tcsConfig.getTcsTransportMode();
        Assert.assertEquals(TCSTransportMode.ZEROMQ, tcsTransportMode);
        Assert.assertEquals("localhost", zeromqBrokerConfig.getIpAddress());

    }

}
