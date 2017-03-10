package net.tcs.functional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "successTest", dependsOnGroups = "registerJobs")
public class TCSFunctionalJobExecutionSuccessTest extends TCSTestBase {

    @Override
    @BeforeClass
    @Parameters({ "rmqIP", "partitionCount", "configFile" })
    public void setup(@Optional("") String rmqIP, @Optional("4") int partitionCount, @Optional("") String configFile)
            throws IOException {
        if (StringUtils.isEmpty(rmqIP) && StringUtils.isEmpty(configFile)) {
            throw new SkipException("Skipping TCSFunctionalJobExecutionSuccessTest");
        }
        super.setup(rmqIP, partitionCount, configFile);
    }

    @Override
    @AfterClass
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testJobExecutionSuccess() throws InterruptedException {
        final List<String> jobs = new ArrayList<>();

        for (final String jobName : jobspecMap.keySet()) {
            final List<String> jobIds = testRuntime.executeJob(jobName, 5);
            jobs.addAll(jobIds);
            break;
        }

        for (final String jobId : jobs) {
            Assert.assertEquals("COMPLETE", testRuntime.waitForJobStatus(jobId, 10));
        }
    }
}
