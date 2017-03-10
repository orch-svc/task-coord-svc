package net.tcs.client.runtime.messagebox;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.task.coordinator.request.message.BeginTaskMessage;
import net.task.coordinator.request.message.BeginTaskRollbackMessage;
import net.tcs.api.TCSCallback;
import net.tcs.api.TCSTaskContext;
import net.tcs.core.TaskContextImpl;

public class TCSClientTaskMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private TCSCallback callback;
    private final String messageBoxName;
    private final String messageBoxId;

    public void registerCallback(TCSCallback callback) {
        this.callback = callback;

    }

    public TCSClientTaskMessageBox(String messageBoxName, String messageBoxId) {
        this.messageBoxName = messageBoxName;
        this.messageBoxId = messageBoxId;
    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {
        try {
            final Object object = message.getPayload();
            if (object instanceof BeginTaskMessage) {
                final BeginTaskMessage taskMsg = (BeginTaskMessage) object;
                final TCSTaskContext taskContext = new TaskContextImpl(taskMsg.getTaskName(),
                        taskMsg.getTaskParallelIndex(), taskMsg.getTaskId(), taskMsg.getJobName(), taskMsg.getJobId(),
                        taskMsg.getShardId(), taskMsg.getRetryCount());

                callback.startTask(taskContext,
                        (taskMsg.getTaskInput() != null) ? taskMsg.getTaskInput().getBytes() : null,
                        taskMsg.getParentTaskOutput(), taskMsg.getJobContext());
            } else if (object instanceof BeginTaskRollbackMessage) {
                final BeginTaskRollbackMessage taskMsg = (BeginTaskRollbackMessage) object;
                /*
                 * REVISIT TODO fix BeginTaskRollbackMessage to pass
                 * taskParallelExecutionIndex
                 */
                final TCSTaskContext taskContext = new TaskContextImpl(taskMsg.getTaskName(), 0, taskMsg.getTaskId(),
                        taskMsg.getJobName(), taskMsg.getJobId(), taskMsg.getShardId(), taskMsg.getRetryCount());

                callback.rollbackTask(taskContext, taskMsg.getTaskOutput().getBytes());
            } else {
                System.out.println("Error unsupported message type");
            }
        } catch (final Exception ex) {
        }
    }

    @Override
    public String getRequestName() {
        return null;
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
}
