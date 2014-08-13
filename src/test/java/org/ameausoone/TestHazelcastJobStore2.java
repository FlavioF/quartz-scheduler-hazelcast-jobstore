package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
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

	@Test
	public void testJobGroupMatcher() throws Exception {
		// Store jobs.
		for (int i = 0; i < 14; i++) {
			String group = i < 5 ? "a" : "ab";
			if (i >= 10) {
				group = "c";
			}
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i, group).build();
			hazelcastJobStore.storeJob(job, false);
		}
		// Retrieve by group
		assertThat(hazelcastJobStore.getJobKeys(GroupMatcher.jobGroupEquals("a")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of jobs in group 'a'");
		assertThat(hazelcastJobStore.getJobKeys(GroupMatcher.jobGroupContains("a")).size()).isEqualTo(10)
				.overridingErrorMessage("Wrong number of jobs in group matching 'a'");
		assertThat(hazelcastJobStore.getJobKeys(GroupMatcher.anyJobGroup()).size()).isEqualTo(15)
				.overridingErrorMessage("Wrong number of jobs in any group");
	}

	@Test
	public void testStoreAndRetriveTriggers() throws Exception {

		JobStore store = hazelcastJobStore;

		// Store jobs and triggers.
		for (int i = 0; i < 10; i++) {
			String group = i < 5 ? "a" : "b";
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i, group).build();
			store.storeJob(job, true);
			SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity("job" + i, group).withSchedule(schedule)
					.forJob(job).build();
			store.storeTrigger((OperableTrigger) trigger, true);
		}
		// Retrieve job and trigger.
		for (int i = 0; i < 10; i++) {
			String group = i < 5 ? "a" : "b";
			JobKey jobKey = JobKey.jobKey("job" + i, group);
			JobDetail storedJob = store.retrieveJob(jobKey);
			Assert.assertEquals(jobKey, storedJob.getKey());

			TriggerKey triggerKey = TriggerKey.triggerKey("job" + i, group);
			Trigger storedTrigger = store.retrieveTrigger(triggerKey);
			Assert.assertEquals(triggerKey, storedTrigger.getKey());
		}
		// Retrieve by group
		assertThat(hazelcastJobStore.getTriggerKeys(GroupMatcher.triggerGroupEquals("a")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of triggers in group 'a'");
		assertThat(hazelcastJobStore.getTriggerKeys(GroupMatcher.triggerGroupEquals("b")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of triggers in group 'b'");

	}

	@Test
	public void testTriggerGroupMatcher() throws Exception {

		for (int i = 0; i < 15; i++) {
			String group = i < 5 ? "a" : "ab";
			if (i >= 10) {
				group = "c";
			}
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i, group).build();
			hazelcastJobStore.storeJob(job, true);
			SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity("job" + i, group).withSchedule(schedule)
					.forJob(job).build();
			hazelcastJobStore.storeTrigger((OperableTrigger) trigger, true);
		}

		// Retrieve by group
		assertThat(hazelcastJobStore.getTriggerKeys(GroupMatcher.triggerGroupEquals("a")).size()).isEqualTo(5)
				.overridingErrorMessage("Wrong number of triggers in group 'a'");
		assertThat(hazelcastJobStore.getTriggerKeys(GroupMatcher.triggerGroupContains("a")).size()).isEqualTo(10)
				.overridingErrorMessage("Wrong number of triggers in group matching 'a'");
		assertThat(hazelcastJobStore.getTriggerKeys(GroupMatcher.anyTriggerGroup()).size()).isEqualTo(15)
				.overridingErrorMessage("Wrong number of triggers in any group");
	}
}
