package net.messaging.clusterbox;

public interface ClusterBox {

    ClusterBoxConfig getClusterBoxConfig();

    void start();

    void shutDown();

    void registerMessageBox(ClusterBoxMessageBox clusterBoxMessageBox);

    void unregisterMessageBox(String clusterBoxMessageBoxId);

    ClusterBoxDropBox getDropBox();

    boolean isOn();

}
