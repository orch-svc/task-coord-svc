package net.task.coordinator.request.message;

public class QueryJobInstanceRequest {

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    private String jobInstanceId;
}
