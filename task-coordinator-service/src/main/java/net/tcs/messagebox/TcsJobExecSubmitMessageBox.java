package net.tcs.messagebox;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.FailureResponse;
import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.base.message.TCSMessageUtils;
import net.task.coordinator.request.message.JobRollbackRequest;
import net.task.coordinator.request.message.JobSubmitRequest;
import net.task.coordinator.request.message.JobSubmitRequestMessage;
import net.task.coordinator.response.message.JobRollbackResponse;
import net.task.coordinator.response.message.JobSubmitResponse;
import net.tcs.core.TCSCommandType;
import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.db.JobDefinitionDAO;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.exceptions.JobInstanceNotFoundException;
import net.tcs.exceptions.JobRollbackIllegalStateException;
import net.tcs.exceptions.JobStateException;
import net.tcs.state.JobState;
import net.tcs.task.JobDefinition;

public class TcsJobExecSubmitMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobExecSubmitMessageBox.class);

    private final String shardId;
    private final JobDefintionDBAdapter jobDBAdapter = new JobDefintionDBAdapter();
    private final JobInstanceDBAdapter jobInstanceDBAdapter = new JobInstanceDBAdapter();
    protected final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();
    private final TaskBoard taskBoard;
    private final ObjectMapper mapper = new ObjectMapper().enableDefaultTyping();
    private final TCSRollbackDispatcher rollbackDispatcher;
    private final String jobExecQueueName;
    private final String jobExecRoutingKey;

    public TcsJobExecSubmitMessageBox(String shardId, TaskBoard taskBoard, TCSRollbackDispatcher rollbackDispatcher) {
        this.shardId = shardId;
        this.taskBoard = taskBoard;
        this.rollbackDispatcher = rollbackDispatcher;
        jobExecQueueName = TCSMessageUtils.getQueueNameForProcessingJobsOnShard(shardId);
        jobExecRoutingKey = String.format("%s.%s", TCSConstants.TCS_JOB_EXEC_RKEY, shardId);
    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {
        try {
            final Object payload = message.getPayload();
            if (payload instanceof JobSubmitRequestMessage) {
                final ResultMessage<JobSubmitResponse> result = (ResultMessage<JobSubmitResponse>) handleSubmitJob(
                        ((JobSubmitRequestMessage) payload));
                if (result != null) {
                    Address from = message.popFromChain();
                    result.setTo(from);
                    dropBox.drop(result);
                }
            } else if (payload instanceof JobRollbackRequest) {
                final ResultMessage<JobRollbackResponse> result = (ResultMessage<JobRollbackResponse>) handleRollbackJob(
                        ((JobRollbackRequest) payload));
                if (result != null) {
                    Address from = message.popFromChain();
                    result.setTo(from);
                    dropBox.drop(result);
                }
            } else {
                LOGGER.error("Unsupported message payload {}", payload);
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobExecSubmitMessageBox HandleMessage", e);
            final FailureResponse errorResponse = new FailureResponse();
            errorResponse.setErrorCode("FAILED");
            errorResponse.setErrorMessage("Job Exec submit failed");
            final ResultMessage<FailureResponse> errorMessage = new ResultMessage<>(errorResponse);
            Address from = message.popFromChain();
            errorMessage.setTo(from);
            dropBox.drop(errorMessage);
        }
    }

    private JobInstanceDAO createJobDAO(JobSubmitRequest request) {
        final JobInstanceDAO jobDAO = new JobInstanceDAO();
        jobDAO.setInstanceId(request.getJobId());
        jobDAO.setName(request.getJobName());
        jobDAO.setShardId(shardId);
        jobDAO.setJobNotificationUri(request.getJobNotificationUri());
        jobDAO.setState(JobState.INPROGRESS.name());
        jobDAO.setStartTime(new Date());

        try {
            final String context = mapper.writeValueAsString(request.getJobContext());
            jobDAO.setJobContext(context);
        } catch (final JsonProcessingException e) {
            LOGGER.error("Exception while serializing JobContext", e);
        }
        return jobDAO;
    }

    /**
     * On receipt of a SubmitJob event, (1) save the Job in DB, (2) ack the
     * message and (3) submit the Job to TaskBoard for routing to a TCSDisptcher
     * for execution.
     *
     * @param channel
     * @param objectMapper
     * @param properties
     * @param body
     */
    Message<?> handleSubmitJob(JobSubmitRequestMessage jobSubmitRequestMessage) {

        /*
         * Check if Job is registered
         */
        final JobSubmitRequest jobRequest = jobSubmitRequestMessage.getRequest();
        final JobDefinitionDAO jobDefDAO = jobDBAdapter.getJobSpec(jobRequest.getJobName());
        if (jobDefDAO == null) {
            LOGGER.warn("No Job definition found in DB, for JobName: {}", jobRequest.getJobName());
            final FailureResponse errorResponse = new FailureResponse();
            errorResponse.setErrorCode("FAILED");
            errorResponse.setErrorMessage(jobRequest.getJobName() + " is not registered");
            final ResultMessage<FailureResponse> errorMessage = new ResultMessage<>(errorResponse);
            return errorMessage;
        }
        /*
         * Create JobInstance and save in DB
         */
        final JobInstanceDAO jobDAO = createJobDAO(jobRequest);
        final JobDefinition jobDef = jobDBAdapter.getJobDefinition(jobRequest.getJobName());
        jobInstanceDBAdapter.saveSubmittedJob(jobDAO, jobDef, jobRequest.getInput());
        /*
         * Send JobSubmitResponse
         */
        final JobSubmitResponse jobResponse = new JobSubmitResponse(jobDAO.getName(), jobDAO.getInstanceId(), shardId);
        final TCSDispatcher taskDispatcher = taskBoard.registerJobForExecution(jobDAO.getInstanceId());
        taskDispatcher.enqueueCommand(TCSCommandType.COMMAND_BEGIN_JOB, jobDAO);
        return new ResultMessage<JobSubmitResponse>(jobResponse);
    }

    private Message<?> handleRollbackJob(JobRollbackRequest jobRequest) {

        /*
         * Check if Job is registered
         */
        final JobDefinitionDAO jobDefDAO = jobDBAdapter.getJobSpec(jobRequest.getJobName());
        if (jobDefDAO == null) {
            LOGGER.warn("No Job definition found in DB, for JobName: {}", jobRequest.getJobName());
            final FailureResponse errorResponse = new FailureResponse();
            errorResponse.setErrorCode("FAILED");
            errorResponse.setErrorMessage(jobRequest.getJobName() + " is not registered");
            final ResultMessage<FailureResponse> errorMessage = new ResultMessage<>(errorResponse);
            return errorMessage;
        }

        final JobInstanceDAO jobDAO;

        try {
            checkIfJobReadyForRollback(jobRequest.getJobName(), jobRequest.getJobId());
            jobDAO = jobInstanceDBAdapter.beginRollbackJob(jobRequest.getJobId(), jobRequest.getJobNotificationUri());
        } catch (final RuntimeException ex) {
            final FailureResponse errorResponse = new FailureResponse();
            errorResponse.setErrorCode("FAILED");
            errorResponse.setErrorMessage(ex.getMessage());
            final ResultMessage<FailureResponse> errorMessage = new ResultMessage<>(errorResponse);
            return errorMessage;
        }

        if (jobDAO == null) {
            final JobRollbackResponse jobResponse = new JobRollbackResponse(jobRequest.getJobName(),
                    jobRequest.getJobId(), "Job not found");
            return new ResultMessage<JobRollbackResponse>(jobResponse);
        }

        /*
         * Send JobRollbackResponse
         */
        final JobRollbackResponse jobResponse = new JobRollbackResponse(jobDAO.getName(), jobDAO.getInstanceId());
        rollbackDispatcher.enqueueCommand(TCSCommandType.COMMAND_ROLLBACK_JOB, jobDAO);
        return new ResultMessage<JobRollbackResponse>(jobResponse);

    }

    private void checkIfJobReadyForRollback(String jobName, String jobInstanceId) {
        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.getJobInstanceFromDB(jobInstanceId);
        if (jobDAO == null) {
            throw new JobInstanceNotFoundException(jobInstanceId);
        }

        if (JobState.FAILED != JobState.get(jobDAO.getState())) {
            throw new JobStateException(JobState.FAILED, JobState.get(jobDAO.getState()));
        }

        final List<TaskInstanceDAO> inProgressTasks = taskDBAdapter.getAllInProgressTasksForJobId(jobInstanceId);
        if (!inProgressTasks.isEmpty()) {
            final String errMessage = String.format(
                    "Job cannot be rolled back, as one or more tasks are in progress. JobName: %s, JobId: %s", jobName,
                    jobInstanceId);
            throw new JobRollbackIllegalStateException(errMessage);
        }
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        return this;
    }

    @Override
    public String getMessageBoxId() {
        return jobExecQueueName;
    }

    @Override
    public String getMessageBoxName() {
        return jobExecRoutingKey;
    }

    @Override
    public String getRequestName() {
        // TODO Auto-generated method stub
        return null;
    }
}
