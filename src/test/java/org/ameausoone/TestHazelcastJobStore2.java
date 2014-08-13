package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHazelcastJobStore2 extends AbstractTestHazelcastJobStore {

	@Test
	public void testStoreAndRetrieveJobs() throws Exception {

		// Store jobs.
		for (int i = 0; i < 10; i++) {
			String group = i < 5 ? "a" : "b";
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i, group).build();
			hazelcastJobStore.storeJob(job, false);
		}
		// Retrieve jobs.
		for (int i = 0; i < 10; i++) {
			String group = i < 5 ? "a" : "b";
			JobKey jobKey = JobKey.jobKey("job" + i, group);
			JobDetail storedJob = hazelcastJobStore.retrieveJob(jobKey);
			Assert.assertEquals(jobKey, storedJob.getKey());
		}
		// Retrieve by group
		assertThat(hazelcastJobStore.getJobKeys(GroupMatcher.jobGroupEquals("a")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of jobs in group 'a'");
		assertThat(hazelcastJobStore.getJobKeys(GroupMatcher.jobGroupEquals("b")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of jobs in group 'b'");

	}
}
