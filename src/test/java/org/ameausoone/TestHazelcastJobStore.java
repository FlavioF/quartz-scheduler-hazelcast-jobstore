package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.fest.assertions.Assertions;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;

@Slf4j
public class TestHazelcastJobStore extends AbstractTestHazelcastJobStore {

	@Test
	public void testStoreSimpleJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobName = "job20";
		storeJob(jobName);
		JobDetail retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void storeTwiceSameJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobName = "job21";
		storeJob(jobName);
		storeJob(jobName);
	}

	@Test
	public void testRemoveJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobName = "job22";
		JobDetail jobDetail = buildJob(jobName);
		storeJob(jobDetail);
		JobDetail retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNotNull();
		Trigger trigger = buildTrigger(jobDetail);
		storeTrigger(trigger);

		assertThat(retrieveTrigger(trigger.getKey())).isNotNull();

		boolean removeJob = this.hazelcastJobStore.removeJob(jobDetail.getKey());
		assertThat(removeJob).isTrue();
		retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNull();

		assertThat(retrieveTrigger(trigger.getKey())).isNull();

		removeJob = this.hazelcastJobStore.removeJob(jobDetail.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testRemoveJobs() throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobName = "job24";
		JobDetail jobDetail = buildJob(jobName);
		String jobName2 = "job25";
		JobDetail jobDetailImpl2 = buildJob(jobName2);
		this.hazelcastJobStore.storeJob(jobDetail, false);
		this.hazelcastJobStore.storeJob(jobDetailImpl2, false);
		JobDetail retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNotNull();
		List<JobKey> jobKeyList = Lists.newArrayList(jobDetail.getKey(), jobDetailImpl2.getKey());
		boolean removeJob = this.hazelcastJobStore.removeJobs(jobKeyList);
		assertThat(removeJob).isTrue();
		retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNull();
		retrieveJob = retrieveJob(jobName2);
		assertThat(retrieveJob).isNull();
		removeJob = this.hazelcastJobStore.removeJob(jobDetail.getKey());
		assertThat(removeJob).isFalse();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl2.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testCheckExistsJob() throws JobPersistenceException {
		JobDetail jobDetailImpl = buildJob("job23");
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		boolean checkExists = hazelcastJobStore.checkExists(jobDetailImpl.getKey());
		Assertions.assertThat(checkExists).isTrue();
	}

	@Test(expectedExceptions = { JobPersistenceException.class })
	public void testStoreTriggerWithoutJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger("trigger", "group");
		storeTrigger(trigger1);
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test
	public void testStoreTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();
		storeTrigger(trigger1);
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void testStoreTriggerThrowsAlreadyExists() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();
		storeTrigger(trigger1);
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
		storeTrigger(trigger1);
		retrieveTrigger = retrieveTrigger(trigger1.getKey());
	}

	@Test
	public void testStoreTriggerTwice() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();

		storeTrigger(trigger1);
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
		hazelcastJobStore.storeTrigger((OperableTrigger) trigger1, true);
		retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test
	public void testRemoveTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail storeJob = storeJob(buildJob("job"));
		Trigger trigger1 = buildTrigger(storeJob);
		TriggerKey triggerKey = trigger1.getKey();
		storeTrigger(trigger1);
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
		boolean removeTrigger = hazelcastJobStore.removeTrigger(triggerKey);
		assertThat(removeTrigger).isTrue();
		retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNull();
		removeTrigger = hazelcastJobStore.removeTrigger(triggerKey);
		assertThat(removeTrigger).isFalse();

		TriggerState triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isNull();
	}

	@Test
	public void testRemoveTriggers() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();
		Trigger trigger2 = buildTrigger();

		storeTrigger(trigger1);
		storeTrigger(trigger2);

		List<TriggerKey> triggerKeys = Lists.newArrayList(trigger1.getKey(), trigger2.getKey());
		boolean removeTriggers = hazelcastJobStore.removeTriggers(triggerKeys);
		assertThat(removeTriggers).isTrue();
	}

	@Test
	public void testTriggerCheckExists() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();
		TriggerKey triggerKey = trigger1.getKey();

		boolean checkExists = hazelcastJobStore.checkExists(triggerKey);
		assertThat(checkExists).isFalse();

		storeTrigger(trigger1);

		checkExists = hazelcastJobStore.checkExists(triggerKey);
		assertThat(checkExists).isTrue();
	}

	@Test
	public void testReplaceTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger1 = buildTrigger();

		storeTrigger(trigger1);

		Trigger newTrigger = buildTrigger();

		TriggerKey triggerKey = trigger1.getKey();
		boolean replaceTrigger = hazelcastJobStore.replaceTrigger(triggerKey, (OperableTrigger) newTrigger);
		assertThat(replaceTrigger).isTrue();
		Trigger retrieveTrigger = hazelcastJobStore.retrieveTrigger(triggerKey);
		assertThat(retrieveTrigger).isEqualTo(newTrigger);
	}

	@Test
	public void testStoreJobAndTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail jobDetailImpl = buildJob("job30");

		Trigger trigger1 = buildTrigger();
		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
		JobDetail retrieveJob = hazelcastJobStore.retrieveJob(jobDetailImpl.getKey());
		assertThat(retrieveJob).isNotNull();
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void testStoreJobAndTriggerThrowJobAlreadyExists() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetail jobDetailImpl = buildJob("job31");
		Trigger trigger1 = buildTrigger();
		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
		JobDetail retrieveJob = hazelcastJobStore.retrieveJob(jobDetailImpl.getKey());
		assertThat(retrieveJob).isNotNull();
		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();

		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
	}

	@Test
	public void storeCalendar() throws ObjectAlreadyExistsException, JobPersistenceException {
		String calName = "calendar";
		storeCalendar(calName);
		Calendar retrieveCalendar = hazelcastJobStore.retrieveCalendar(calName);
		assertThat(retrieveCalendar).isNotNull();
	}

	@Test
	public void testRemoveCalendar() throws ObjectAlreadyExistsException, JobPersistenceException {
		String calName = "calendar1";
		storeCalendar(calName);

		Calendar retrieveCalendar = hazelcastJobStore.retrieveCalendar(calName);
		assertThat(retrieveCalendar).isNotNull();
		boolean calendarExisted = hazelcastJobStore.removeCalendar(calName);
		assertThat(calendarExisted).isTrue();
		retrieveCalendar = hazelcastJobStore.retrieveCalendar(calName);
		assertThat(retrieveCalendar).isNull();
		calendarExisted = hazelcastJobStore.removeCalendar(calName);
		assertThat(calendarExisted).isFalse();

	}

	@Test
	public void testClearAllSchedulingData() throws JobPersistenceException {
		assertThat(hazelcastJobStore.getNumberOfJobs()).isEqualTo(0);

		assertThat(hazelcastJobStore.getNumberOfTriggers()).isEqualTo(0);

		assertThat(hazelcastJobStore.getNumberOfCalendars()).isEqualTo(0);

		String jobName = "job40";
		JobDetail storeJob = storeJob(jobName);
		assertThat(hazelcastJobStore.getNumberOfJobs()).isEqualTo(1);

		hazelcastJobStore.storeTrigger((OperableTrigger) buildTrigger("trig", "group", storeJob), false);
		assertThat(hazelcastJobStore.getNumberOfTriggers()).isEqualTo(1);

		hazelcastJobStore.storeCalendar("calendar", new BaseCalendar(), false, false);
		assertThat(hazelcastJobStore.getNumberOfCalendars()).isEqualTo(1);

		hazelcastJobStore.clearAllSchedulingData();
		assertThat(hazelcastJobStore.getNumberOfJobs()).isEqualTo(0);

		assertThat(hazelcastJobStore.getNumberOfTriggers()).isEqualTo(0);

		assertThat(hazelcastJobStore.getNumberOfCalendars()).isEqualTo(0);
	}

	@Test
	public void testStoreSameJobNameWithDifferentGroup() throws ObjectAlreadyExistsException, JobPersistenceException {
		storeJob(buildJob("job40", "group1"));
		storeJob(buildJob("job40", "group2"));
		// Assert there is no exception throws
	}

	@Test
	public void testGetJobGroupNames() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail buildJob = buildJob("job40", "group1");
		storeJob(buildJob);
		storeJob(buildJob("job41", "group2"));
		List<String> jobGroupNames = hazelcastJobStore.getJobGroupNames();
		assertThat(jobGroupNames).containsOnly("group1", "group2");

		this.hazelcastJobStore.removeJob(buildJob.getKey());

		jobGroupNames = hazelcastJobStore.getJobGroupNames();
		assertThat(jobGroupNames).containsOnly("group2");
	}

	@Test
	public void testJobKeyByGroup() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail job1group1 = buildJob("job1", "group1");
		storeJob(job1group1);
		JobDetail job1group2 = buildJob("job1", "group2");
		storeJob(job1group2);
		storeJob(buildJob("job2", "group2"));
		List<String> jobGroupNames = hazelcastJobStore.getJobGroupNames();
		assertThat(jobGroupNames).containsOnly("group1", "group2");

		this.hazelcastJobStore.removeJob(job1group1.getKey());

		jobGroupNames = hazelcastJobStore.getJobGroupNames();
		assertThat(jobGroupNames).containsOnly("group2");

		this.hazelcastJobStore.removeJob(job1group2.getKey());

		jobGroupNames = hazelcastJobStore.getJobGroupNames();
		assertThat(jobGroupNames).containsOnly("group2");
	}

	@Test
	public void testGetTriggerGroupNames() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail storeJob = storeJob(buildJob("job"));
		storeTrigger(buildTrigger("trigger1", "group1", storeJob));
		storeTrigger(buildTrigger("trigger2", "group2", storeJob));
		List<String> triggerGroupNames = hazelcastJobStore.getTriggerGroupNames();
		assertThat(triggerGroupNames).containsOnly("group1", "group2");
	}

	@Test
	public void testCalendarNames() throws JobPersistenceException {
		storeCalendar("cal1");
		storeCalendar("cal2");
		List<String> calendarNames = hazelcastJobStore.getCalendarNames();
		assertThat(calendarNames).containsOnly("cal1", "cal2");
	}

	@Test
	public void storeJobAndTriggers() throws ObjectAlreadyExistsException, JobPersistenceException {

		Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps.newHashMap();
		JobDetail job1 = buildJob();
		Trigger trigger1 = buildTrigger(job1);
		Set<Trigger> set1 = Sets.newHashSet();
		set1.add(trigger1);
		triggersAndJobs.put(job1, set1);
		JobDetail job2 = buildJob();
		Trigger trigger2 = buildTrigger(job2);
		Set<Trigger> set2 = Sets.newHashSet();
		set1.add(trigger2);
		triggersAndJobs.put(job2, set2);
		hazelcastJobStore.storeJobsAndTriggers(triggersAndJobs, false);

		JobDetail retrieveJob = retrieveJob(job1.getKey().getName());
		assertThat(retrieveJob).isNotNull();
		retrieveJob = retrieveJob(job2.getKey().getName());
		assertThat(retrieveJob).isNotNull();

		Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
		assertThat(retrieveTrigger).isNotNull();
		retrieveTrigger = retrieveTrigger(trigger2.getKey());
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void storeJobAndTriggersThrowException() throws ObjectAlreadyExistsException, JobPersistenceException {

		Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps.newHashMap();
		JobDetail job1 = buildJob();
		storeJob(job1);
		Trigger trigger1 = buildTrigger(job1);
		Set<Trigger> set1 = Sets.newHashSet();
		set1.add(trigger1);
		triggersAndJobs.put(job1, set1);
		hazelcastJobStore.storeJobsAndTriggers(triggersAndJobs, false);
	}

	@Test
	public void testGetTriggersForJob() throws JobPersistenceException {
		JobDetail jobDetail = buildAndStoreJob();
		Trigger trigger1 = buildTrigger(jobDetail);
		Trigger trigger2 = buildTrigger(jobDetail);
		storeTrigger(trigger1);
		storeTrigger(trigger2);

		List<OperableTrigger> triggersForJob = hazelcastJobStore.getTriggersForJob(jobDetail.getKey());
		assertThat(triggersForJob).containsOnly(trigger1, trigger2);
	}

	@Test
	public void testPauseTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildTrigger();
		storeTrigger(trigger);
		TriggerKey triggerKey = trigger.getKey();
		TriggerState triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
		hazelcastJobStore.pauseTrigger(triggerKey);
		triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isEqualTo(TriggerState.PAUSED);
	}

	@Test
	public void testResumeTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildTrigger();
		storeTrigger(trigger);
		TriggerKey triggerKey = trigger.getKey();
		TriggerState triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
		hazelcastJobStore.pauseTrigger(triggerKey);
		triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isEqualTo(TriggerState.PAUSED);

		hazelcastJobStore.resumeTrigger(triggerKey);
		triggerState = hazelcastJobStore.getTriggerState(triggerKey);
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
	}

	@Test
	public void testPauseTriggers() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildAndStoreTrigger();
		Trigger trigger1 = buildAndStoreTrigger();
		Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
		storeTrigger(trigger2);
		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger1.getKey())).isEqualTo(TriggerState.NORMAL);

		Collection<String> pauseTriggers = hazelcastJobStore.pauseTriggers(GroupMatcher.triggerGroupEquals(trigger
				.getKey().getGroup()));

		assertThat(pauseTriggers).containsOnly(trigger.getKey().getGroup());
		assertThat(hazelcastJobStore.getPausedTriggerGroups()).containsOnly(trigger.getKey().getGroup());

		Trigger trigger3 = buildAndStoreTrigger();

		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger1.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger2.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.PAUSED);
	}

	@Test
	public void testResumeTriggers() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildAndStoreTrigger();
		Trigger trigger1 = buildAndStoreTrigger();
		Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
		storeTrigger(trigger2);
		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger1.getKey())).isEqualTo(TriggerState.NORMAL);

		Collection<String> pauseTriggers = hazelcastJobStore.pauseTriggers(GroupMatcher.triggerGroupEquals(trigger
				.getKey().getGroup()));

		assertThat(pauseTriggers).containsOnly(trigger.getKey().getGroup());

		Trigger trigger3 = buildAndStoreTrigger();

		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger1.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger2.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.PAUSED);

		Collection<String> resumeTriggers = hazelcastJobStore.resumeTriggers(GroupMatcher.triggerGroupEquals(trigger
				.getKey().getGroup()));

		assertThat(resumeTriggers).containsOnly(trigger.getKey().getGroup());

		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger1.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.NORMAL);

		Trigger trigger4 = buildAndStoreTrigger();
		assertThat(hazelcastJobStore.getTriggerState(trigger4.getKey())).isEqualTo(TriggerState.NORMAL);
	}

	@Test
	public void testResumeTriggerWithPausedJobs() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail job1 = buildJob("job", "group3");
		storeJob(job1);
		Trigger trigger5 = buildTrigger(job1);
		storeTrigger(trigger5);

		assertThat(hazelcastJobStore.getTriggerState(trigger5.getKey())).isEqualTo(TriggerState.NORMAL);
		hazelcastJobStore.pauseJobs(GroupMatcher.jobGroupEquals("group3"));
		hazelcastJobStore.resumeTriggers(GroupMatcher.triggerGroupEquals(trigger5.getKey().getGroup()));
		assertThat(hazelcastJobStore.getTriggerState(trigger5.getKey())).isEqualTo(TriggerState.PAUSED);
	}

	@Test
	public void testPauseJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail jobDetail = buildAndStoreJob();
		Trigger trigger = buildTrigger(jobDetail);
		storeTrigger(trigger);
		TriggerState triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);

		hazelcastJobStore.pauseJob(jobDetail.getKey());

		triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
		assertThat(triggerState).isEqualTo(TriggerState.PAUSED);
	}

	@Test
	public void testResumeJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail jobDetail = buildAndStoreJob();
		Trigger trigger = buildTrigger(jobDetail);
		storeTrigger(trigger);
		TriggerState triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);

		hazelcastJobStore.pauseJob(jobDetail.getKey());

		triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
		assertThat(triggerState).isEqualTo(TriggerState.PAUSED);

		hazelcastJobStore.resumeJob(jobDetail.getKey());

		triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
		assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
	}

	@Test
	public void testPauseJobs() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail job1 = buildAndStoreJobWithTrigger();
		JobDetail job2 = buildAndStoreJobWithTrigger();

		JobDetail job3 = buildJob("job3", "newgroup");
		storeJob(job3);
		storeTrigger(buildTrigger(job3));
		List<OperableTrigger> triggersForJob = hazelcastJobStore.getTriggersForJob(job1.getKey());
		triggersForJob.addAll(hazelcastJobStore.getTriggersForJob(job2.getKey()));
		for (OperableTrigger trigger : triggersForJob) {
			assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		}

		Collection<String> pauseJobs = hazelcastJobStore.pauseJobs(GroupMatcher
				.jobGroupEquals(job1.getKey().getGroup()));

		assertThat(pauseJobs).containsOnly(job1.getKey().getGroup());

		JobDetail job4 = buildAndStoreJobWithTrigger();

		triggersForJob = hazelcastJobStore.getTriggersForJob(job1.getKey());
		triggersForJob.addAll(hazelcastJobStore.getTriggersForJob(job2.getKey()));
		triggersForJob.addAll(hazelcastJobStore.getTriggersForJob(job4.getKey()));
		for (OperableTrigger trigger : triggersForJob) {
			TriggerState triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
			log.debug("State : [" + triggerState + "]Should be PAUSED for trigger : [" + trigger.getKey()
					+ "] and job [" + trigger.getJobKey() + "]");
			assertThat(triggerState).isEqualTo(TriggerState.PAUSED).overridingErrorMessage(
					"Should be PAUSED for trigger : [" + trigger.getKey() + "] and job [" + trigger.getJobKey() + "]");
		}

		triggersForJob = hazelcastJobStore.getTriggersForJob(job3.getKey());

		for (OperableTrigger trigger : triggersForJob) {
			assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		}
	}

	@Test
	public void testResumeJobs() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail job1 = buildAndStoreJobWithTrigger();
		JobDetail job2 = buildAndStoreJob();
		Trigger trigger2 = buildTrigger("trigger", "trigGroup2", job2);
		storeTrigger(trigger2);

		JobDetail job3 = buildJob("job3", "newgroup");
		storeJob(job3);
		storeTrigger(buildTrigger(job3));

		Collection<String> pauseJobs = hazelcastJobStore.pauseJobs(GroupMatcher.anyJobGroup());

		assertThat(pauseJobs).containsOnly(job1.getKey().getGroup(), "newgroup");

		List<OperableTrigger> triggersForJob = hazelcastJobStore.getTriggersForJob(job1.getKey());

		for (OperableTrigger trigger : triggersForJob) {
			TriggerState triggerState = hazelcastJobStore.getTriggerState(trigger.getKey());
			assertThat(triggerState).isEqualTo(TriggerState.PAUSED).overridingErrorMessage(
					"Should be PAUSED for trigger : [" + trigger.getKey() + "] and job [" + trigger.getJobKey() + "]");
		}

		triggersForJob = hazelcastJobStore.getTriggersForJob(job3.getKey());
		for (OperableTrigger trigger : triggersForJob) {
			assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		}

		hazelcastJobStore.pauseTriggers(GroupMatcher.triggerGroupEquals("trigGroup2"));

		hazelcastJobStore.resumeJobs(GroupMatcher.jobGroupEquals(job1.getKey().getGroup()));

		triggersForJob = hazelcastJobStore.getTriggersForJob(job3.getKey());
		for (OperableTrigger trigger : triggersForJob) {
			assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		}

		triggersForJob = hazelcastJobStore.getTriggersForJob(job1.getKey());
		for (OperableTrigger trigger : triggersForJob) {
			assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		}
	}

	@Test
	public void testPauseAll() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildAndStoreTrigger();
		Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
		storeTrigger(trigger2);
		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);

		hazelcastJobStore.pauseAll();

		assertThat(hazelcastJobStore.getPausedTriggerGroups()).containsOnly(trigger.getKey().getGroup(),
				trigger2.getKey().getGroup());

		Trigger trigger3 = buildAndStoreTrigger();

		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger2.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.PAUSED);
	}

	@Test
	public void testResumeAll() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildAndStoreTrigger();
		Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
		storeTrigger(trigger2);
		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);

		hazelcastJobStore.pauseAll();

		assertThat(hazelcastJobStore.getPausedTriggerGroups()).containsOnly(trigger.getKey().getGroup(),
				trigger2.getKey().getGroup());

		Trigger trigger3 = buildAndStoreTrigger();

		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger2.getKey())).isEqualTo(TriggerState.PAUSED);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.PAUSED);

		hazelcastJobStore.resumeAll();
		assertThat(hazelcastJobStore.getTriggerState(trigger.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger2.getKey())).isEqualTo(TriggerState.NORMAL);
		assertThat(hazelcastJobStore.getTriggerState(trigger3.getKey())).isEqualTo(TriggerState.NORMAL);

	}
}
