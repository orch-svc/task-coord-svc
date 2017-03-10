package net.tcs.client.runtime.messagebox;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.task.coordinator.request.message.JobCompleteMessage;
import net.task.coordinator.request.message.JobFailedMessage;
import net.task.coordinator.request.message.JobRollbackCompleteMessage;
import net.tcs.api.TCSJobHandler;

public class TCSClientJobMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private TCSJobHandler callback;
    private final String messageBoxName;
    private final String messageBoxId;

    public TCSClientJobMessageBox(TCSJobHandler callback, String messageBoxName, String messageBoxId) {
        this.callback = callback;
        this.messageBoxName = messageBoxName;
        this.messageBoxId = messageBoxId;
    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {
        try {
            final Object object = message.getPayload();
            if (object instanceof JobCompleteMessage) {
                final JobCompleteMessage job = (JobCompleteMessage) object;
                callback.jobComplete(job.getJobId());
            } else if (object instanceof JobFailedMessage) {
                final JobFailedMessage job = (JobFailedMessage) object;
                callback.jobFailed(job.getJobId());
            } else if (object instanceof JobRollbackCompleteMessage) {
                final JobRollbackCompleteMessage job = (JobRollbackCompleteMessage) object;
                callback.jobRollbackComplete(job.getJobId());
            } else {
                System.out.println("Error unsupported message type");
            }

        } catch (final Exception ex) {
        }
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        return this;
    }

    @Override
    public String getMessageBoxId() {
        return this.messageBoxId;
    }

    @Override
    public String getMessageBoxName() {
        return this.messageBoxName;
    }

    @Override
    public String getRequestName() {
        return null;
    }
}
