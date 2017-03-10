package net.messaging.clusterbox.zeromq;

public interface ZeromqClusterBoxConstants {

    static final String WORKER_READY = "WORKER_READY";
    static final String WORKER_SHUTDOWN = "WORKER_SHUTDOWN";
    static final String REGISTER_WORKER = "REGISTER_WORKER";
    static final Integer DEFAULT_IO_THREAD_COUNT = 1;
    static final String CLUSTER_BOX_READY = "CLUSTER_BOX_READY";
    static final String CLUSTER_BOX_SHUTDOWN = "CLUSTER_BOX_SHUTDOWN";
    static final String REGISTER_CLUSTER_BOX = "REGISTER_CLUSTER_BOX";
    static final String CLUSTER_BOX_NAME_KEY = "clusterBoxName";
    static final String CLUSTER_BOX_MAIL_BOX_NAME_KEY = "clusterBoxMailBoxName";

    // Need to add it to external property
    static final String DEFAILT_CB_BACKEND_PORT = "9009";

    static final String PING_FRAME = "ping";

}
