package net.tcs.core;

import java.util.Arrays;
import java.util.HashSet;

import net.messaging.clusterbox.Address;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import net.tcs.task.JobDefinition;

public class TestJobDefCreateUtils {

    static final String BROKER_ADDRESS = "1.2.3.4";

    public static JobDefinition createJobDef(String jobName) {
        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t0000");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t1111");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t2222");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t3333");
        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t4444");
        final TcsTaskExecutionEndpoint t5Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t5555");
        final TcsTaskExecutionEndpoint t6Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t6666");
        final TcsTaskExecutionEndpoint t7Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t7777");
        final TcsTaskExecutionEndpoint t8Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t8888");
        final TcsTaskExecutionEndpoint t9Endpoint = new TcsTaskExecutionEndpoint(TCSConstants.TCS_JOB_EXEC_EXCHANGE,
                "tcs.rkey-t9999");

        final JobDefinition job = new JobDefinition(jobName);

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t10", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0", "t3")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2")));
        job.addTask("t5", t5Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t3")));
        job.addTask("t6", t6Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t4")));
        job.addTask("t7", t7Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t5", "t10")));
        job.addTask("t8", t8Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t5", "t10")));
        job.addTask("t9", t9Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t6", "t7", "t8")));
        return job;
    }

    public static JobDefinition createJobDefWithStepMarking0(String jobName) {
        final JobDefinition job = new JobDefinition(jobName);

        job.addTask("t0", new Address("foobar", "t0").toJson(), null);
        job.addTask("t1", new Address("foobar", "t1").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", new Address("foobar", "t2").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t3", new Address("foobar", "t3").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t4", new Address("foobar", "t4").toJson(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));
        job.addTask("t5", new Address("foobar", "t5").toJson(), new HashSet<String>(Arrays.asList("t4")));
        job.addTask("t6", new Address("foobar", "t6").toJson(), new HashSet<String>(Arrays.asList("t5")));
        job.addTask("t7", new Address("foobar", "t7").toJson(), new HashSet<String>(Arrays.asList("t6")));
        job.addTask("t8", new Address("foobar", "t8").toJson(), new HashSet<String>(Arrays.asList("t7")));
        job.addTask("t9", new Address("foobar", "t9").toJson(), new HashSet<String>(Arrays.asList("t7")));

        job.markStep("teststep", Arrays.asList("t4", "t5", "t6", "t7"));
        return job;
    }

    public static JobDefinition createJobDefWithStepMarking1(String jobName) {
        final JobDefinition job = new JobDefinition(jobName);

        job.addTask("t0", new Address("foobar", "t0").toJson(), null);
        job.addTask("t1", new Address("foobar", "t1").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", new Address("foobar", "t2").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t3", new Address("foobar", "t3").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t4", new Address("foobar", "t4").toJson(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));
        job.addTask("t5", new Address("foobar", "t5").toJson(), new HashSet<String>(Arrays.asList("t4")));
        job.addTask("t6", new Address("foobar", "t6").toJson(), new HashSet<String>(Arrays.asList("t5")));
        job.addTask("t7", new Address("foobar", "t7").toJson(), new HashSet<String>(Arrays.asList("t6")));
        job.addTask("t8", new Address("foobar", "t8").toJson(), new HashSet<String>(Arrays.asList("t7")));
        job.addTask("t10", new Address("foobar", "t10").toJson(), new HashSet<String>(Arrays.asList("t3")));
        job.addTask("t9", new Address("foobar", "t9").toJson(), new HashSet<String>(Arrays.asList("t7", "t10")));

        job.markStep("teststep", Arrays.asList("t4", "t5", "t6", "t7"));
        return job;
    }

    public static JobDefinition createJobDefWithStepMarking2(String jobName) {
        final JobDefinition job = new JobDefinition(jobName);

        job.addTask("t0", new Address("foobar", "t0").toJson(), null);
        job.addTask("t1", new Address("foobar", "t1").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", new Address("foobar", "t2").toJson(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", new Address("foobar", "t3").toJson(), new HashSet<String>(Arrays.asList("t2")));

        job.markStep("teststep", Arrays.asList("t1"));
        return job;
    }

    public static JobDefinition createJobDefWithStepMarking3(String jobName) {
        final JobDefinition job = new JobDefinition(jobName);

        job.addTask("t0", new Address("foobar", "t0").toJson(), null);
        job.addTask("t1", new Address("foobar", "t1").toJson(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", new Address("foobar", "t2").toJson(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", new Address("foobar", "t3").toJson(), new HashSet<String>(Arrays.asList("t2")));

        job.markStep("teststep", Arrays.asList("t1", "t2"));
        return job;
    }
}
