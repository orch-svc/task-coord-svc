package net.task.coordinator.request.message;

public class TaskInProgressMessage {
    public TaskInProgressMessage() {
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskInProgressMessage(String taskId) {
        this.taskId = taskId;
    }

    private String taskId;
}
