package net.tcs.client.runtime;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.message.FailureResponse;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.message.ResultMessage;
import net.task.coordinator.base.message.TCSConstants;
import net.task.coordinator.base.message.TCSMessageUtils;
import net.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import net.task.coordinator.request.message.JobRollbackRequest;
import net.task.coordinator.request.message.JobSpecRegistrationMessage;
import net.task.coordinator.request.message.JobSubmitRequest;
import net.task.coordinator.request.message.JobSubmitRequestMessage;
import net.task.coordinator.request.message.QueryJobInstanceRequest;
import net.task.coordinator.request.message.QueryJobSpecMessage;
import net.task.coordinator.request.message.TaskCompleteMessage;
import net.task.coordinator.request.message.TaskFailedMessage;
import net.task.coordinator.request.message.TaskInProgressMessage;
import net.task.coordinator.request.message.TaskRollbackCompleteMessage;
import net.task.coordinator.response.message.JobRollbackResponse;
import net.task.coordinator.response.message.JobSubmitResponse;
import net.task.coordinator.response.message.QueryJobInstanceResponse;
import net.tcs.api.TCSCallback;
import net.tcs.api.TCSClient;
import net.tcs.api.TCSJobHandler;
import net.tcs.api.TCSTaskContext;
import net.tcs.client.runtime.messagebox.TCSClientJobMessageBox;
import net.tcs.client.runtime.messagebox.TCSClientTaskMessageBox;
import net.tcs.core.TCSRMQCommandExecutor;
import net.tcs.exceptions.UnregisteredTaskSpecException;
import net.tcs.messaging.AddressParser;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;
import net.tcs.task.TaskDefinition;

public abstract class TCSCBBasedClientRuntime implements TCSClient {

    private final int numPartitions;
    private final String rmqBrokerAddress;

    // private SpringRmqConnectionFactory rmqFactory;
    // private AmqpTemplate template;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random r = new Random();
    private final ConcurrentMap<String, JobDefinition> jobSpecs = new ConcurrentHashMap<>();
    private final TCSRMQCommandExecutor rmqCommandExecutor = new TCSRMQCommandExecutor();
    private static Connection connection;
    private volatile Channel channel;
    protected ClusterBox clusterBox;

    private static final Logger LOGGER = LoggerFactory.getLogger(TCSCBBasedClientRuntime.class);

    public TCSCBBasedClientRuntime(int numPartitions, String rmqBrokerAddress) {
        this.numPartitions = numPartitions;
        this.rmqBrokerAddress = rmqBrokerAddress;

    }

    private static final class TaskHandlerKey {
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((jobName == null) ? 0 : jobName.hashCode());
            result = prime * result + ((taskName == null) ? 0 : taskName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final TaskHandlerKey other = (TaskHandlerKey) obj;
            if (jobName == null) {
                if (other.jobName != null)
                    return false;
            } else if (!jobName.equals(other.jobName))
                return false;
            if (taskName == null) {
                if (other.taskName != null)
                    return false;
            } else if (!taskName.equals(other.taskName))
                return false;
            return true;
        }

        public TaskHandlerKey(String jobName, String taskName) {
            this.jobName = jobName;
            this.taskName = taskName;
        }

        private final String jobName;
        private final String taskName;
    }

    private final ConcurrentMap<String, TCSClientJobMessageBox> jobListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskHandlerKey, TCSClientTaskMessageBox> taskCallbackListeners = new ConcurrentHashMap<>();

    @Override
    public void registerJob(JobSpec jobSpec) {
        final JobDefinition job = (JobDefinition) jobSpec;

        final JobSpecRegistrationMessage jobSpecRegistrationMessage = new JobSpecRegistrationMessage();
        jobSpecRegistrationMessage.setJobSpec(job);
        RequestMessage<JobDefinition> message = new RequestMessage<>(job);
        Address address = new Address(TCSConstants.TCS_EXCHANGE, TCSConstants.TCS_REGISTER_TASK_RKEY);
        message.setTo(address);
        System.out.println("Sending Message " + message);
        clusterBox.getDropBox().dropAndReceive(message);

    }

    private String chooseARandomShard() {
        return String.format("%s_%d", TCSConstants.TCS_SHARD_GROUP_NAME, r.nextInt(numPartitions));
    }

    @Override
    public String startJob(String jobName, Map<String, byte[]> input) {

        final Map<String, String> taskInputMap = new HashMap<>();
        if (input != null) {
            for (final Entry<String, byte[]> entry : input.entrySet()) {
                taskInputMap.put(entry.getKey(), new String(entry.getValue()));
            }
        }

        return submitJob(jobName, taskInputMap, new HashMap<String, String>());
    }

    @Override
    public String startJob(String jobName, byte[] jobInputForParentTasks) {
        return startJob(jobName, jobInputForParentTasks, new HashMap<String, String>());
    }

    @Override
    public String startJob(String jobName, byte[] jobInputForParentTasks, Map<String, String> jobContext) {
        final String jobInputAsStr = new String(jobInputForParentTasks);

        final JobDefinition job = getJobDefinition(jobName);

        final Map<String, String> taskInputMap = new HashMap<>();
        final Map<String, TaskDefinition> taskMap = job.getTaskMap();
        for (final TaskDefinition task : taskMap.values()) {
            if (task.getParents().isEmpty()) {
                taskInputMap.put(task.getTaskName(), jobInputAsStr);
            }
        }

        return submitJob(jobName, taskInputMap, jobContext);
    }

    @Override
    public String startJob(String jobName, Map<String, byte[]> input, Map<String, String> jobContext) {

        final Map<String, String> taskInputMap = new HashMap<>();
        if (input != null) {
            for (final Entry<String, byte[]> entry : input.entrySet()) {
                taskInputMap.put(entry.getKey(), new String(entry.getValue()));
            }
        }

        return submitJob(jobName, taskInputMap, jobContext);
    }

    private String submitJob(String jobName, final Map<String, String> taskInputMap, Map<String, String> jobContext) {
        final TCSClientJobMessageBox jobListener = jobListeners.get(jobName);

        if (jobListener == null) {
            System.out.println("Not prepared to execute job: " + jobName);
            System.out.println("use Prepare command to prepare for Job and task(s)");
            return null;
        }

        final String jobId = UUID.randomUUID().toString();

        final String shardId = chooseARandomShard();
        System.out.println("startJob: " + jobId + "   chose shard: " + shardId);
        Address returnAddress = new Address(clusterBox.getClusterBoxConfig().getClusterBoxName(),
                jobListener.getMessageBoxName());
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, returnAddress.toJson(), taskInputMap);
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        final TcsTaskExecutionEndpoint endpoint = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(shardId);
        Address address = new Address(endpoint.getExchangeName(), endpoint.getRoutingKey());
        RequestMessage<JobSubmitRequestMessage> message = new RequestMessage<>(jobSubmitMessage);
        message.setTo(address);
        Future<ResultMessage> result = clusterBox.getDropBox().dropAndReceive(message);

        if (result == null) {
            LOGGER.error("StartJob failed for Job: {}", jobName);
            return null;
        }

        try {
            if (result.get() instanceof ResultMessage) {
                JobSubmitResponse response = (JobSubmitResponse) result.get().getPayload();
                if (response != null) {
                    LOGGER.debug("Started Job; Name: {}, JobId: {}, ShardId: {}", jobName, response.getJobId(),
                            response.getShardId());
                }
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return jobId;
    }

    @Override
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput) {
        taskComplete(taskExecutionContext, taskOutput, new HashMap<String, String>());
    }

    @Override
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput,
            Map<String, String> taskContextOutput) {
        final TaskCompleteMessage req = new TaskCompleteMessage(taskExecutionContext.getTaskId(),
                taskExecutionContext.getJobId(), new String(taskOutput));
        req.setTaskContextOutput(taskContextOutput);

        final TcsTaskExecutionEndpoint endpoint = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskExecutionContext.getShardId());
        Address address = new Address(endpoint.getExchangeName(), endpoint.getRoutingKey());
        RequestMessage<TaskCompleteMessage> message = new RequestMessage<>(req);
        message.setTo(address);
        clusterBox.getDropBox().drop(message);
    }

    private JobDefinition getJobDefinition(String jobName) {
        if (jobSpecs.containsKey(jobName)) {
            return jobSpecs.get(jobName);
        }

        final JobSpec jobSpec = queryRegisteredJob(jobName);
        if (jobSpec != null) {
            final JobDefinition job = (JobDefinition) jobSpec;
            final JobDefinition existing = jobSpecs.putIfAbsent(jobName, job);
            if (existing != null) {
                return existing;
            } else {
                return job;
            }
        }
        return null;
    }

    @Override
    public void prepareToExecute(String jobName, TCSJobHandler jobHandler, Map<String, TCSCallback> taskHandlers) {
        final JobDefinition job = getJobDefinition(jobName);
        if (job != null) {
            final Map<String, TaskDefinition> taskMap = job.getTaskMap();
            final Set<String> registeredTasks = taskMap.keySet();

            final Set<String> tasks = taskHandlers.keySet();
            if (!registeredTasks.containsAll(tasks)) {
                LOGGER.error("Some of the tasks are not registered for Job: {}", jobName);

                for (final String task : tasks) {
                    if (!registeredTasks.contains(task)) {
                        throw new UnregisteredTaskSpecException(jobName, task);
                    }
                }
                return;
            }

            final String jobHandlerQueue = String.format("%s-%s", jobName, UUID.randomUUID().toString());
            final String rkey = jobHandlerQueue;

            if (!jobListeners.containsKey(jobName)) {
                TCSClientJobMessageBox jobListener = new TCSClientJobMessageBox(jobHandler, rkey, jobHandlerQueue);
                clusterBox.registerMessageBox(jobListener);
                jobListeners.put(jobName, jobListener);
            }

            for (final String task : tasks) {

                final TaskHandlerKey key = new TaskHandlerKey(jobName, task);
                if (taskCallbackListeners.containsKey(key)) {
                    LOGGER.info("Already registered to execute: {}/{}", jobName, task);
                    continue;
                }

                final TaskDefinition taskDef = taskMap.get(task);

                final TcsTaskExecutionEndpoint endpoint = AddressParser.parseAddress(taskDef.getTaskExecutionTarget());
                final TCSClientTaskMessageBox taskListener = new TCSClientTaskMessageBox(endpoint.getRoutingKey(),
                        String.format("%s.%s", endpoint.getRoutingKey(), UUID.randomUUID().toString()));

                if (null == taskCallbackListeners.putIfAbsent(key, taskListener)) {
                    taskListener.registerCallback(taskHandlers.get(task));
                    clusterBox.registerMessageBox(taskListener);
                }
            }
        }
        LOGGER.info("Completed preparation for job {}", jobName);
    }

    @Override
    public void taskFailed(TCSTaskContext taskContext, byte[] error) {
        final TaskFailedMessage req = new TaskFailedMessage(taskContext.getTaskId(), new String(error));

        final TcsTaskExecutionEndpoint endpoint = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        ResultMessage<TaskFailedMessage> message = new ResultMessage<>(req);
        Address address = new Address(endpoint.getExchangeName(), endpoint.getRoutingKey());
        message.setTo(address);
        clusterBox.getDropBox().drop(message);
    }

    @Override
    public JobSpec queryRegisteredJob(String jobName) {

        final QueryJobSpecMessage queryJobSpecMessage = new QueryJobSpecMessage();
        queryJobSpecMessage.setJobName(jobName);
        Address address = new Address(TCSConstants.TCS_EXCHANGE, TCSConstants.TCS_QUERY_TASK_RKEY);
        RequestMessage<QueryJobSpecMessage> message = new RequestMessage<>(queryJobSpecMessage);
        message.setTo(address);
        Future<ResultMessage> result = clusterBox.getDropBox().dropAndReceive(message);
        if (result == null) {
            LOGGER.warn("Job: {} not registered with TCS", jobName);
            return null;
        }

        try {
            if (result.get() instanceof ResultMessage) {
                LOGGER.info("Query Result {}", result.get().getPayload());
                JobDefinition jobDefinition = mapper.readValue((String) result.get().getPayload(), JobDefinition.class);
                return jobDefinition;
            }
        } catch (final Exception e) {
            LOGGER.error("Error in creating queryRegisteredJob. Cause {}", e);
            return null;
        }

        LOGGER.warn("Job: {} not registered with TCS", jobName);
        return null;
    }

    @Override
    public void taskInProgress(TCSTaskContext taskContext) {
        final TaskInProgressMessage req = new TaskInProgressMessage(taskContext.getTaskId());

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        RequestMessage<TaskInProgressMessage> message = new RequestMessage<>(req);
        message.setTo(new Address(address.getExchangeName(), address.getRoutingKey()));
        clusterBox.getDropBox().drop(message);
    }

    @Override
    public void rollbackJob(String jobName, String jobInstanceId) {

        final QueryJobInstanceRequest requestQuery = new QueryJobInstanceRequest();
        requestQuery.setJobInstanceId(jobInstanceId);
        RequestMessage<QueryJobInstanceRequest> message = new RequestMessage<>(requestQuery);
        message.setTo(new Address(TCSConstants.TCS_EXCHANGE, TCSConstants.TCS_QUERY_TASK_RKEY));
        Future<ResultMessage> resultQuery = clusterBox.getDropBox().dropAndReceive(message);

        if (resultQuery == null) {
            LOGGER.warn("Job InstanceId: {} not found with TCS", jobInstanceId);
            return;
        }

        final QueryJobInstanceResponse responseQuery;
        try {
            responseQuery = (QueryJobInstanceResponse) resultQuery.get().getPayload();
        } catch (final Exception e) {
            LOGGER.error("Response Query Failed {}", e);
            return;
        }

        if (!StringUtils.equalsIgnoreCase("FAILED", responseQuery.getStatus())) {
            final String errMessage = String.format(
                    "Job: %s, instanceId: %s is not FAILED; therefore cannot rollback. Current state: %s", jobName,
                    jobInstanceId, responseQuery.getStatus());
            LOGGER.error(errMessage);
            System.out.println(errMessage);
            return;
        }

        final TCSClientJobMessageBox jobListener = jobListeners.get(jobName);
        Address returnAddress = new Address(clusterBox.getClusterBoxConfig().getClusterBoxName(),
                jobListener.getMessageBoxName());
        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(responseQuery.getShardId());
        final JobRollbackRequest req = new JobRollbackRequest(jobName, jobInstanceId, returnAddress.toJson());

        RequestMessage<JobRollbackRequest> rollbackMessage = new RequestMessage<>(req);
        rollbackMessage.setTo(new Address(address.getExchangeName(), address.getRoutingKey()));

        Future<ResultMessage> result = clusterBox.getDropBox().dropAndReceive(rollbackMessage);

        if (result == null) {
            LOGGER.error("Rollback failed for Job: {}", jobName);
            return;
        }

        try {
            if (result.get() instanceof ResultMessage) {
                if (result.get().getPayload() instanceof FailureResponse) {
                    final FailureResponse errorMessage = (FailureResponse) result.get().getPayload();
                    System.out.println(errorMessage.getErrorCode());
                    System.out.println(errorMessage.getErrorMessage());
                    System.out.println(errorMessage.getDetailedMessage());
                } else {
                    final JobRollbackResponse response = (JobRollbackResponse) result.get().getPayload();
                    if (response != null) {
                        LOGGER.debug("Started Job rollback; Name: {}, JobId: {}", jobName, response.getJobId());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Job role back failed. Cause {}", e);
        }
    }

    @Override
    public void taskRollbackComplete(TCSTaskContext taskContext) {
        final TaskRollbackCompleteMessage req = new TaskRollbackCompleteMessage(taskContext.getTaskId());

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        RequestMessage<TaskRollbackCompleteMessage> message = new RequestMessage<>(req);
        message.setTo(new Address(address.getExchangeName(), address.getRoutingKey()));
        clusterBox.getDropBox().drop(message);
    }

    @Override
    public void taskRollbackNotSupported() {
        // TODO Auto-generated method stub

    }

    @Override
    public void taskRollbackFailed() {
        // TODO Auto-generated method stub
    }

}
