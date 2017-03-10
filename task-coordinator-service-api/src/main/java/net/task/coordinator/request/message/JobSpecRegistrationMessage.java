package net.task.coordinator.request.message;

import net.tcs.task.JobDefinition;

public class JobSpecRegistrationMessage {

    private JobDefinition jobSpec;

    public JobDefinition getJobSpec() {
        return jobSpec;
    }

    public void setJobSpec(JobDefinition jobSpec) {
        this.jobSpec = jobSpec;
    }
}
