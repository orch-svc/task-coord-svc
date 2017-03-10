package net.tcs.messagebox;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import junit.framework.Assert;
import net.messaging.clusterbox.message.ResultMessage;
import net.task.coordinator.request.message.JobSpecRegistrationMessage;
import net.task.coordinator.request.message.QueryJobSpecMessage;
import net.tcs.core.TestJobDefCreateUtils;
import net.tcs.task.JobDefinition;

public class RegisterJobHandlerTest extends DBAdapterTestBase {
    @Override
    @BeforeClass
    public void setup() throws ClassNotFoundException, SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        super.setup();
    }

    @Override
    @AfterClass
    public void cleanup() {

        super.cleanup();
    }

    @Test
    public void testRegisterAndQueryJobSpec() throws IOException {

        final TcsJobRegisterMessageBox jobRegisterHandler = new TcsJobRegisterMessageBox();
        final TcsJobQueryMessageBox jobQueryHandler = new TcsJobQueryMessageBox();

        final String jobName = "testjob";

        final QueryJobSpecMessage queryMessage = new QueryJobSpecMessage();
        queryMessage.setJobName(jobName);

        ResultMessage<String> resultObjQuery = jobQueryHandler.processQueryJob(queryMessage);

        Assert.assertTrue(StringUtils.containsIgnoreCase(resultObjQuery.getPayload(), "JOB_NOT_FOUND"));

        final JobDefinition jobDef = TestJobDefCreateUtils.createJobDef(jobName);

        final JobSpecRegistrationMessage message = new JobSpecRegistrationMessage();
        message.setJobSpec(jobDef);

        ResultMessage<String> resultObj = jobRegisterHandler.processRegisterJob(message);

        Assert.assertTrue(StringUtils.containsIgnoreCase(resultObj.getPayload(), "ACK"));
        Assert.assertTrue(StringUtils.containsIgnoreCase(resultObj.getPayload(), jobName));

        resultObjQuery = jobQueryHandler.processQueryJob(queryMessage);

        final JobDefinition jobDefRead = mapper.readValue(resultObjQuery.getPayload(), JobDefinition.class);
        Assert.assertNotNull(jobDefRead);
        Assert.assertEquals(jobName, jobDefRead.getJobName());

        resultObj = jobRegisterHandler.processRegisterJob(message);
        Assert.assertTrue(StringUtils.containsIgnoreCase(resultObj.getPayload(), "JOB_EXISTS"));

    }
}
