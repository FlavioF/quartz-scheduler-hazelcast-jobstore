package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
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

	@SuppressWarnings("deprecation")
	@Test
	public void testAcquireNextTriggerBatch() throws Exception {

		Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
		long baseFireTime = baseFireTimeDate.getTime();
		JobDetailImpl fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		fJobDetail.setDurability(true);
		hazelcastJobStore.storeJob(fJobDetail, false);

		OperableTrigger early = new SimpleTriggerImpl("early", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime), new Date(baseFireTime + 5), 2, 2000);

		OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 200000), new Date(baseFireTime + 200005), 2, 2000);
		OperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 200100), new Date(baseFireTime + 200105), 2, 2000);
		OperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 200200), new Date(baseFireTime + 200205), 2, 2000);
		OperableTrigger trigger4 = new SimpleTriggerImpl("trigger4", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 200300), new Date(baseFireTime + 200305), 2, 2000);

		OperableTrigger trigger10 = new SimpleTriggerImpl("trigger10", "triggerGroup2", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(baseFireTime + 500000), new Date(baseFireTime + 700000), 2, 2000);

		early.computeFirstFireTime(null);
		trigger1.computeFirstFireTime(null);
		trigger2.computeFirstFireTime(null);
		trigger3.computeFirstFireTime(null);
		trigger4.computeFirstFireTime(null);
		trigger10.computeFirstFireTime(null);
		hazelcastJobStore.storeTrigger(early, false);
		hazelcastJobStore.storeTrigger(trigger1, false);
		hazelcastJobStore.storeTrigger(trigger2, false);
		hazelcastJobStore.storeTrigger(trigger3, false);
		hazelcastJobStore.storeTrigger(trigger4, false);
		hazelcastJobStore.storeTrigger(trigger10, false);

		long firstFireTime = new Date(trigger1.getNextFireTime().getTime()).getTime();

		List<OperableTrigger> acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 4, 1000L);
		log.debug("acquiredTriggers : [{}]", acquiredTriggers);
		assertEquals(4, acquiredTriggers.size());
		assertEquals(acquiredTriggers.get(0).getKey(), early.getKey());
		assertEquals(acquiredTriggers.get(1).getKey(), trigger1.getKey());
		assertEquals(acquiredTriggers.get(2).getKey(), trigger2.getKey());
		assertEquals(acquiredTriggers.get(3).getKey(), trigger3.getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);
		hazelcastJobStore.releaseAcquiredTrigger(trigger2);
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);

		acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 5, 1000L);
		assertEquals(5, acquiredTriggers.size());
		assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
		assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
		assertEquals(trigger2.getKey(), acquiredTriggers.get(2).getKey());
		assertEquals(trigger3.getKey(), acquiredTriggers.get(3).getKey());
		assertEquals(trigger4.getKey(), acquiredTriggers.get(4).getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);
		hazelcastJobStore.releaseAcquiredTrigger(trigger2);
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);
		hazelcastJobStore.releaseAcquiredTrigger(trigger4);

		acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 6, 1000L);
		assertEquals(5, acquiredTriggers.size());
		assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
		assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
		assertEquals(trigger2.getKey(), acquiredTriggers.get(2).getKey());
		assertEquals(trigger3.getKey(), acquiredTriggers.get(3).getKey());
		assertEquals(trigger4.getKey(), acquiredTriggers.get(4).getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);
		hazelcastJobStore.releaseAcquiredTrigger(trigger2);
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);
		hazelcastJobStore.releaseAcquiredTrigger(trigger4);

		acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 1, 5, 0L);
		assertEquals(2, acquiredTriggers.size());
		assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
		assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);

		acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 250, 5, 199L);
		assertEquals(5, acquiredTriggers.size());
		assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
		assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
		assertEquals(trigger2.getKey(), acquiredTriggers.get(2).getKey());
		assertEquals(trigger3.getKey(), acquiredTriggers.get(3).getKey());
		assertEquals(trigger4.getKey(), acquiredTriggers.get(4).getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);
		hazelcastJobStore.releaseAcquiredTrigger(trigger2);
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);
		hazelcastJobStore.releaseAcquiredTrigger(trigger4);

		acquiredTriggers = hazelcastJobStore.acquireNextTriggers(firstFireTime + 150, 5, 50L);
		assertEquals(4, acquiredTriggers.size());
		assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
		assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
		assertEquals(trigger2.getKey(), acquiredTriggers.get(2).getKey());
		assertEquals(trigger3.getKey(), acquiredTriggers.get(3).getKey());
		hazelcastJobStore.releaseAcquiredTrigger(early);
		hazelcastJobStore.releaseAcquiredTrigger(trigger1);
		hazelcastJobStore.releaseAcquiredTrigger(trigger2);
		hazelcastJobStore.releaseAcquiredTrigger(trigger3);
	}

	@Test
	public void testTriggerStates() throws Exception {
		JobDetailImpl fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		fJobDetail.setDurability(true);
		hazelcastJobStore.storeJob(fJobDetail, false);

		OperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.getName(),
				fJobDetail.getGroup(), new Date(System.currentTimeMillis() + 100000), new Date(
						System.currentTimeMillis() + 200000), 2, 2000);
		trigger.computeFirstFireTime(null);
		assertEquals(TriggerState.NONE, hazelcastJobStore.getTriggerState(trigger.getKey()));
		hazelcastJobStore.storeTrigger(trigger, false);
		assertEquals(TriggerState.NORMAL, hazelcastJobStore.getTriggerState(trigger.getKey()));

		hazelcastJobStore.pauseTrigger(trigger.getKey());
		assertEquals(TriggerState.PAUSED, hazelcastJobStore.getTriggerState(trigger.getKey()));

		hazelcastJobStore.resumeTrigger(trigger.getKey());
		assertEquals(TriggerState.NORMAL, hazelcastJobStore.getTriggerState(trigger.getKey()));

		trigger = hazelcastJobStore.acquireNextTriggers(
				new Date(trigger.getNextFireTime().getTime()).getTime() + 10000, 1, 1L).get(0);
		Assert.assertNotNull(trigger);
		hazelcastJobStore.releaseAcquiredTrigger(trigger);
		trigger = hazelcastJobStore.acquireNextTriggers(
				new Date(trigger.getNextFireTime().getTime()).getTime() + 10000, 1, 1L).get(0);
		Assert.assertNotNull(trigger);
		Assert.assertTrue(hazelcastJobStore.acquireNextTriggers(
				new Date(trigger.getNextFireTime().getTime()).getTime() + 10000, 1, 1L).isEmpty());
	}

	public void testAcquireTriggers() throws Exception {
		// SchedulerSignaler schedSignaler = new SampleSignaler();
		// ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
		// loadHelper.initialize();
		//
		// // JobStore store = createJobStore("testAcquireTriggers");
		// hazelcastJobStore.initialize(loadHelper, schedSignaler);

		// Setup: Store jobs and triggers.
		long MIN = 60 * 1000L;
		Date startTime0 = new Date(System.currentTimeMillis() + MIN); // a min from now.
		for (int i = 0; i < 10; i++) {
			Date startTime = new Date(startTime0.getTime() + i * MIN); // a min apart
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i).build();
			SimpleScheduleBuilder schedule = SimpleScheduleBuilder.repeatMinutelyForever(2);
			OperableTrigger trigger = (OperableTrigger) TriggerBuilder.newTrigger().withIdentity("job" + i)
					.withSchedule(schedule).forJob(job).startAt(startTime).build();

			// Manually trigger the first fire time computation that scheduler would do. Otherwise
			// the hazelcastJobStore.acquireNextTriggers() will not work properly.
			Date fireTime = trigger.computeFirstFireTime(null);
			Assert.assertEquals(true, fireTime != null);

			hazelcastJobStore.storeJobAndTrigger(job, trigger);
		}

		// Test acquire one trigger at a time
		for (int i = 0; i < 10; i++) {
			long noLaterThan = (startTime0.getTime() + i * MIN);
			int maxCount = 1;
			long timeWindow = 0;
			List<OperableTrigger> triggers = hazelcastJobStore.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
			Assert.assertEquals(1, triggers.size());
			Assert.assertEquals("job" + i, triggers.get(0).getKey().getName());

			// Let's remove the trigger now.
			hazelcastJobStore.removeJob(triggers.get(0).getJobKey());
		}
	}

	public void testAcquireTriggersInBatch() throws Exception {
		// SchedulerSignaler schedSignaler = new SampleSignaler();
		// ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
		// loadHelper.initialize();
		//
		// hazelcastJobStore.initialize(loadHelper, schedSignaler);

		// Setup: Store jobs and triggers.
		long MIN = 60 * 1000L;
		Date startTime0 = new Date(System.currentTimeMillis() + MIN); // a min from now.
		for (int i = 0; i < 10; i++) {
			Date startTime = new Date(startTime0.getTime() + i * MIN); // a min apart
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity("job" + i).build();
			SimpleScheduleBuilder schedule = SimpleScheduleBuilder.repeatMinutelyForever(2);
			OperableTrigger trigger = (OperableTrigger) TriggerBuilder.newTrigger().withIdentity("job" + i)
					.withSchedule(schedule).forJob(job).startAt(startTime).build();

			// Manually trigger the first fire time computation that scheduler would do. Otherwise
			// the store.acquireNextTriggers() will not work properly.
			Date fireTime = trigger.computeFirstFireTime(null);
			Assert.assertEquals(true, fireTime != null);

			hazelcastJobStore.storeJobAndTrigger(job, trigger);
		}

		// Test acquire batch of triggers at a time
		long noLaterThan = startTime0.getTime() + 10 * MIN;
		int maxCount = 7;
		// time window needs to be big to be able to pick up multiple triggers when they are a minute apart
		long timeWindow = 8 * MIN;
		List<OperableTrigger> triggers = hazelcastJobStore.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
		Assert.assertEquals(7, triggers.size());
		for (int i = 0; i < 7; i++) {
			Assert.assertEquals("job" + i, triggers.get(i).getKey().getName());
		}
	}
}
