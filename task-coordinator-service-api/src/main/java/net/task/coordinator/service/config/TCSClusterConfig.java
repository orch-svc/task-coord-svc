package net.task.coordinator.service.config;

public class TCSClusterConfig {
    @Override
    public String toString() {
        return "ClusterConfig [clusterName=" + clusterName + ", shardGroupName=" + shardGroupName + ", numPartitions="
                + numPartitions + "]";
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getShardGroupName() {
        return shardGroupName;
    }

    public void setShardGroupName(String shardGroupName) {
        this.shardGroupName = shardGroupName;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    private String clusterName;
    private String shardGroupName;
    private int numPartitions;

}
