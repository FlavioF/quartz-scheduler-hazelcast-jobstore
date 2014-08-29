package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import java.util.Date;

import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
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
		for (int i = 0; i < 15; i++) {
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

	@SuppressWarnings("deprecation")
	@Test
	public void testAcquireNextTriggers() throws JobPersistenceException {

		Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
		long baseFireTime = baseFireTimeDate.getTime();
		JobDetailImpl fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		fJobDetail.setDurability(true);
		hazelcastJobStore.storeJob(fJobDetail, false);

		OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 200000), new Date(baseFireTime + 200000), 2, 2000);
		OperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 50000), new Date(baseFireTime + 200000), 2, 2000);
		OperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 100000), new Date(baseFireTime + 200000), 2, 2000);

		trigger1.computeFirstFireTime(null);
		trigger2.computeFirstFireTime(null);
		trigger3.computeFirstFireTime(null);
		hazelcastJobStore.storeTrigger(trigger1, false);
		hazelcastJobStore.storeTrigger(trigger3, false);
		hazelcastJobStore.storeTrigger(trigger2, false);

		long firstFireTime = new Date(trigger1.getNextFireTime().getTime()).getTime();

		assertThat(hazelcastJobStore.acquireNextTriggers(10, 1, 0L).isEmpty());
		assertThat(hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0).getKey()).isEqualTo(
				trigger2.getKey());
		assertThat(hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0).getKey()).isEqualTo(
				trigger3.getKey());
		assertThat(hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0).getKey()).isEqualTo(
				trigger1.getKey());
		Assert.assertTrue(hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).isEmpty());

		// release trigger3
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);
		assertEquals(
				trigger3,
				hazelcastJobStore.acquireNextTriggers(new Date(trigger1.getNextFireTime().getTime()).getTime() + 10000,
						1, 1L).get(0));
	}
}
