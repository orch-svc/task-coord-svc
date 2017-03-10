package net.task.coordinator.request.message;

public class TaskRollbackCompleteMessage {
    public TaskRollbackCompleteMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRollbackCompleteMessage(String taskId) {
        super();
        this.taskId = taskId;
    }

    private String taskId;
}
