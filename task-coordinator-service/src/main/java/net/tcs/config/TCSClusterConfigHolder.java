package net.tcs.config;

import java.io.IOException;

import net.tcs.exceptions.TCSConfigReadException;

public class TCSClusterConfigHolder {

    private static TCSConfig config;

    public static TCSConfig getConfig() {
        if (config == null) {
            final String configFile = System.getProperty("tcs.config", "conf/config.json");
            System.out.println("Config file name: " + configFile);
            try {
                config = TCSConfigReader.readConfig(configFile);
            } catch (final IOException e) {
                System.err.println("Error parsing Default Config file: ");
                e.printStackTrace();
                throw new TCSConfigReadException(e.getMessage());
            }
        }
        return config;
    }
}
