package net.tcs.messagebox;

import java.io.IOException;

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
import net.task.coordinator.request.message.QueryJobInstanceRequest;
import net.task.coordinator.request.message.QueryJobSpecMessage;
import net.task.coordinator.response.message.QueryJobInstanceResponse;
import net.tcs.db.JobDefinitionDAO;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;

/**
 * RMQ Listener for RegisterJobSpec and QueryJobSpec events.
 *
 */
public class TcsJobQueryMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobQueryMessageBox.class);
    final ObjectMapper objectMapper = new ObjectMapper();
    private final JobDefintionDBAdapter jobDefAdapter;
    private final JobInstanceDBAdapter jobInstanceAdapter;
    private final String queueName;
    private final String routingKey;

    public TcsJobQueryMessageBox() {
        jobDefAdapter = new JobDefintionDBAdapter();
        jobInstanceAdapter = new JobInstanceDBAdapter();
        this.queueName = TCSConstants.TCS_QUERY_TASK_QUEUE;
        this.routingKey = TCSConstants.TCS_QUERY_TASK_RKEY;
    }

    protected TcsJobQueryMessageBox(String queueName, String routingKey) {
        jobDefAdapter = new JobDefintionDBAdapter();
        jobInstanceAdapter = new JobInstanceDBAdapter();
        this.queueName = queueName;
        this.routingKey = routingKey;
    }

    public void cleanup() {
    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {

        ResultMessage<String> result = null;
        try {
            final Object object = message.getPayload();
            if (object instanceof QueryJobSpecMessage) {
                result = processQueryJob((QueryJobSpecMessage) object);
            } else if (object instanceof QueryJobInstanceRequest) {
                processQueryJobInstanceRequest(message, dropBox, object);
                return;
            } else {
                LOGGER.error("Error unsupported message type {}", object.getClass().getName());
                LOGGER.error(object.toString());
            }
            if (result != null) {
                Address to = message.popFromChain();
                LOGGER.info("Sending Response to {}", to);
                result.setTo(to);
                dropBox.drop(result);
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobRegisterListener.onMessage()", e);
        }
    }

    private void processQueryJobInstanceRequest(RequestMessage message, ClusterBoxDropBox dropBox, final Object object)
            throws IOException, JsonProcessingException {

        final QueryJobInstanceRequest request = (QueryJobInstanceRequest) object;
        final String jobId = request.getJobInstanceId();
        final JobInstanceDAO jobDAO = jobInstanceAdapter.getJobInstanceFromDB(jobId);
        if (jobDAO != null) {
            final QueryJobInstanceResponse result = new QueryJobInstanceResponse(jobDAO.getInstanceId(),
                    jobDAO.getShardId(), jobDAO.getState());
            ResultMessage<QueryJobInstanceResponse> response = new ResultMessage<>(result);
            response.setTo(message.popFromChain());
            dropBox.drop(response);
        }
    }

    ResultMessage<String> processQueryJob(QueryJobSpecMessage message) {
        final String jobName = message.getJobName();
        final JobDefinitionDAO jobDAO = jobDefAdapter.getJobSpec(jobName);
        if (jobDAO != null) {
            return new ResultMessage<String>(jobDAO.getBody());
        } else {
            return new ResultMessage<String>("JOB_NOT_FOUND");
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
        return this.queueName;
    }

    @Override
    public String getMessageBoxName() {
        return this.routingKey;
    }
}
