package net.tcs.messagebox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.request.message.JobSpecRegistrationMessage;
import net.tcs.core.JobDefinitionCycleDetector;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.exceptions.JobAlreadyExistsException;
import net.tcs.task.JobDefinition;

/**
 * RMQ Listener for RegisterJobSpec and QueryJobSpec events.
 *
 */
public class TcsJobRegisterMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobRegisterMessageBox.class);
    final ObjectMapper objectMapper = new ObjectMapper();
    private final JobDefintionDBAdapter jobDefAdapter;
    private final String queueName;
    private final String routingKey;

    public TcsJobRegisterMessageBox() {
        jobDefAdapter = new JobDefintionDBAdapter();
        this.queueName = TCSConstants.TCS_REGISTER_TASK_QUEUE;
        this.routingKey = TCSConstants.TCS_REGISTER_TASK_RKEY;
    }

    protected TcsJobRegisterMessageBox(String queueName, String routingKey) {
        jobDefAdapter = new JobDefintionDBAdapter();
        this.queueName = queueName;
        this.routingKey = routingKey;
    }

    public void cleanup() {

    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {

        ResultMessage<String> result = null;
        try {
            final JobDefinition object = (JobDefinition) message.getPayload();
            JobSpecRegistrationMessage registrationMessage = new JobSpecRegistrationMessage();
            registrationMessage.setJobSpec(object);
            if (registrationMessage instanceof JobSpecRegistrationMessage) {
                result = processRegisterJob(registrationMessage);
            } else {
                LOGGER.error("Error unsupported message type . Actual Type {}", object.getClass().getSimpleName());
            }
            if (result != null) {
                Address to = message.popFromChain();
                LOGGER.info("Sending Result to {}", to);
                result.setTo(to);
                dropBox.drop(result);
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobRegisterMessageBox.handleMessage()", e);
        }
    }

    /*
     * TODO REVISIT error codes
     */
    ResultMessage<String> processRegisterJob(JobSpecRegistrationMessage message) {
        final JobDefinition job = message.getJobSpec();

        final JobDefinitionCycleDetector cycleDetector = new JobDefinitionCycleDetector(job);
        if (cycleDetector.detectCycle()) {
            LOGGER.error("Cycle detected in Job definition for job: {}", job.getJobName());
            return new ResultMessage<String>("JOB_DEFINITION_DETECTED_CYCLE");
        }

        try {
            job.validateSteps();
        } catch (final IllegalArgumentException ex) {
            LOGGER.error("Exception in JobDefinition.validateSteps() for Job: " + job.getJobName(), ex);
            return new ResultMessage<String>("JOB_DEFINITION_INVALID_STEPS");
        }

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(job);
        } catch (final JsonProcessingException e) {
            LOGGER.error("Exception in TcsJobExecSubmitListener.processRegisterJob()", e);
            return null;
        }

        try {
            jobDefAdapter.saveJobSpec(job.getJobName(), bytes);
            return new ResultMessage<String>("ACK : " + job.getJobName());
        } catch (final JobAlreadyExistsException ex) {
            return new ResultMessage<String>("JOB_EXISTS");
        }
    }

    @Override
    public String getRequestName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        return this;
    }

    @Override
    public String getMessageBoxId() {
        return this.queueName;
    }

    @Override
    public String getMessageBoxName() {
        return this.routingKey;
    }
}
