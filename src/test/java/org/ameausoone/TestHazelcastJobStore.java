package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.fest.assertions.Assertions;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.spi.OperableTrigger;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

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
		JobDetailImpl jobDetailImpl = buildJob(jobName);
		storeJob(jobDetailImpl);
		JobDetail retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNotNull();
		boolean removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl.getKey());
		assertThat(removeJob).isTrue();
		retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNull();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testRemoveJobs() throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobName = "job24";
		JobDetailImpl jobDetailImpl = buildJob(jobName);
		String jobName2 = "job25";
		JobDetailImpl jobDetailImpl2 = buildJob(jobName2);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		this.hazelcastJobStore.storeJob(jobDetailImpl2, false);
		JobDetail retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNotNull();
		List<JobKey> jobKeyList = Lists.newArrayList(jobDetailImpl.getKey(), jobDetailImpl2.getKey());
		boolean removeJob = this.hazelcastJobStore.removeJobs(jobKeyList);
		assertThat(removeJob).isTrue();
		retrieveJob = retrieveJob(jobName);
		assertThat(retrieveJob).isNull();
		retrieveJob = retrieveJob(jobName2);
		assertThat(retrieveJob).isNull();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl.getKey());
		assertThat(removeJob).isFalse();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl2.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testCheckExistsJob() throws JobPersistenceException {
		JobDetailImpl jobDetailImpl = buildJob("job23");
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		boolean checkExists = hazelcastJobStore.checkExists(jobDetailImpl.getKey());
		Assertions.assertThat(checkExists).isTrue();
	}

	@Test(expectedExceptions = { JobPersistenceException.class })
	public void testStoreTriggerWithoutJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger("trigger", "group");
		storeTrigger(trigger1);
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test
	public void testStoreTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger();
		storeTrigger(trigger1);
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void testStoreTriggerThrowsAlreadyExists() throws ObjectAlreadyExistsException, JobPersistenceException {

		OperableTrigger trigger1 = buildTrigger();

		storeTrigger(trigger1);
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
		storeTrigger(trigger1);
		retrieveTrigger = retrieveTrigger(trigger1);
	}

	@Test
	public void testStoreTriggerTwice() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger();

		storeTrigger(trigger1);
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
		hazelcastJobStore.storeTrigger(trigger1, true);
		retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test
	public void testRemoveTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail storeJob = storeJob(buildJob("job"));
		OperableTrigger trigger1 = buildTrigger(storeJob);
		TriggerKey triggerKey = trigger1.getKey();
		storeTrigger(trigger1);
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
		boolean removeTrigger = hazelcastJobStore.removeTrigger(triggerKey);
		assertThat(removeTrigger).isTrue();
		retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNull();
		removeTrigger = hazelcastJobStore.removeTrigger(triggerKey);
		assertThat(removeTrigger).isFalse();
	}

	@Test
	public void testRemoveTriggers() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger();
		OperableTrigger trigger2 = buildTrigger();

		storeTrigger(trigger1);
		storeTrigger(trigger2);

		List<TriggerKey> triggerKeys = Lists.newArrayList(trigger1.getKey(), trigger2.getKey());
		boolean removeTriggers = hazelcastJobStore.removeTriggers(triggerKeys);
		assertThat(removeTriggers).isTrue();
	}

	@Test
	public void testTriggerCheckExists() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger();
		TriggerKey triggerKey = trigger1.getKey();

		boolean checkExists = hazelcastJobStore.checkExists(triggerKey);
		assertThat(checkExists).isFalse();

		storeTrigger(trigger1);

		checkExists = hazelcastJobStore.checkExists(triggerKey);
		assertThat(checkExists).isTrue();
	}

	@Test
	public void testReplaceTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		OperableTrigger trigger1 = buildTrigger();

		storeTrigger(trigger1);

		OperableTrigger newTrigger = buildTrigger();

		TriggerKey triggerKey = trigger1.getKey();
		boolean replaceTrigger = hazelcastJobStore.replaceTrigger(triggerKey, newTrigger);
		assertThat(replaceTrigger).isTrue();
		OperableTrigger retrieveTrigger = hazelcastJobStore.retrieveTrigger(triggerKey);
		assertThat(retrieveTrigger).isEqualTo(newTrigger);
	}

	@Test
	public void testStoreJobAndTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetailImpl jobDetailImpl = buildJob("job30");

		OperableTrigger trigger1 = buildTrigger();
		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, trigger1);
		JobDetail retrieveJob = hazelcastJobStore.retrieveJob(jobDetailImpl.getKey());
		assertThat(retrieveJob).isNotNull();
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void testStoreJobAndTriggerThrowJobAlreadyExists() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetailImpl jobDetailImpl = buildJob("job31");
		OperableTrigger trigger1 = buildTrigger();
		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, trigger1);
		JobDetail retrieveJob = hazelcastJobStore.retrieveJob(jobDetailImpl.getKey());
		assertThat(retrieveJob).isNotNull();
		OperableTrigger retrieveTrigger = retrieveTrigger(trigger1);
		assertThat(retrieveTrigger).isNotNull();

		hazelcastJobStore.storeJobAndTrigger(jobDetailImpl, trigger1);
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

		hazelcastJobStore.storeTrigger(buildTrigger("trig", "group", storeJob), false);
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
		JobDetailImpl buildJob = buildJob("job40", "group1");
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
		JobDetailImpl job1group1 = buildJob("job1", "group1");
		storeJob(job1group1);
		JobDetailImpl job1group2 = buildJob("job1", "group2");
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

}
