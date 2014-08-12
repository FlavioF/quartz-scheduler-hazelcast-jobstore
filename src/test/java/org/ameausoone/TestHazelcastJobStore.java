package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;
import static org.quartz.Scheduler.DEFAULT_GROUP;

import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.fest.assertions.Assertions;
import org.quartz.Calendar;
import org.quartz.DateBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Slf4j
public class TestHazelcastJobStore {

	private HazelcastInstance hazelcastInstance;
	private HazelcastJobStore hazelcastJobStore;
	private SampleSignaler fSignaler;
	private JobDetailImpl fJobDetail;

	private int buildTriggerIndex = 0;

	@SuppressWarnings("deprecation")
	@BeforeClass
	public void setUp() throws SchedulerException, InterruptedException {
		log.info("SetUp");
		Config config = new Config();
		config.setProperty("hazelcast.logging.type", "slf4j");
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);

		this.fSignaler = new SampleSignaler();
		hazelcastJobStore = createJobStore("jobstore");
		ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
		log.debug("Before loadHelper.initialize()");
		loadHelper.initialize();

		hazelcastJobStore.initialize(loadHelper, this.fSignaler);
		hazelcastJobStore.setInstanceId("SimpleInstance");
		hazelcastJobStore.schedulerStarted();

		this.fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		this.hazelcastJobStore.storeJob(this.fJobDetail, false);
	}

	@AfterMethod
	public void cleanUpAfterEachTest() throws JobPersistenceException {
		hazelcastJobStore.clearAllSchedulingData();
	}

	protected HazelcastJobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);
		return hazelcastJobStore;
	}

	private JobDetailImpl buildJob(String jobName) {
		return buildJob(jobName, DEFAULT_GROUP);
	}

	@SuppressWarnings("deprecation")
	private JobDetailImpl buildJob(String jobName, String grouName) {
		JobDetailImpl jobDetailImpl = new JobDetailImpl(jobName, grouName, MyJob.class);
		return jobDetailImpl;
	}

	private void storeJob(String jobName) throws ObjectAlreadyExistsException, JobPersistenceException {
		storeJob(buildJob(jobName));
	}

	private void storeJob(JobDetailImpl jobDetailImpl) throws ObjectAlreadyExistsException, JobPersistenceException {
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
	}

	private JobDetail retrieveJob(String jobName) throws JobPersistenceException {
		return this.hazelcastJobStore.retrieveJob(new JobKey(jobName, DEFAULT_GROUP));
	}

	private OperableTrigger buildTrigger() {
		return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP);
	}

	@SuppressWarnings("deprecation")
	private OperableTrigger buildTrigger(String triggerName, String groupName) {
		Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
		long baseFireTime = baseFireTimeDate.getTime();

		OperableTrigger trigger1 = new SimpleTriggerImpl(triggerName, groupName, this.fJobDetail.getName(),
				this.fJobDetail.getGroup(), new Date(baseFireTime + 200000), new Date(baseFireTime + 200000), 2, 2000);
		return trigger1;
	}

	private OperableTrigger retrieveTrigger(OperableTrigger trigger1) throws JobPersistenceException {
		return hazelcastJobStore.retrieveTrigger(trigger1.getKey());
	}

	private void storeTrigger(OperableTrigger trigger1) throws ObjectAlreadyExistsException, JobPersistenceException {
		hazelcastJobStore.storeTrigger(trigger1, false);
	}

	private void storeCalendar(String calName) throws ObjectAlreadyExistsException, JobPersistenceException {
		hazelcastJobStore.storeCalendar(calName, new BaseCalendar(), false, false);
	}

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
		OperableTrigger trigger1 = buildTrigger();
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
		storeJob(jobName);
		assertThat(hazelcastJobStore.getNumberOfJobs()).isEqualTo(1);

		hazelcastJobStore.storeTrigger(buildTrigger(), false);
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
		storeTrigger(buildTrigger("trigger1", "group1"));
		storeTrigger(buildTrigger("trigger2", "group2"));
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

	@SuppressWarnings("deprecation")
	// @Test
	public void testAcquireNextTrigger() throws JobPersistenceException {
		Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
		long baseFireTime = baseFireTimeDate.getTime();

		// Trigger trigger1 = TriggerBuilder
		// .newTrigger()
		// .withIdentity("trigger1", "triggerGroup1")
		// .startAt(new Date(baseFireTime + 200000))
		// .withSchedule(
		// SimpleScheduleBuilder.repeatSecondlyForTotalCount(2, 2))
		// .endAt(new Date(baseFireTime + 200000)).build();
		OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", this.fJobDetail.getName(),
				this.fJobDetail.getGroup(), new Date(baseFireTime + 200000), new Date(baseFireTime + 200000), 2, 2000);
		// new SimpleTriggerImpl("trigger1", "triggerGroup1",
		// this.fJobDetail.getName(), this.fJobDetail.getGroup(),
		// new Date(baseFireTime + 200000),
		// new Date(baseFireTime + 200000), 2, 2000);
		// OperableTrigger trigger2 = new SimpleTriggerImpl("trigger2",
		// "triggerGroup1", this.fJobDetail.getName(),
		// this.fJobDetail.getGroup(), new Date(baseFireTime + 50000),
		// new Date(baseFireTime + 200000), 2, 2000);
		// OperableTrigger trigger3 = new SimpleTriggerImpl("trigger1",
		// "triggerGroup2", this.fJobDetail.getName(),
		// this.fJobDetail.getGroup(), new Date(baseFireTime + 100000),
		// new Date(baseFireTime + 200000), 2, 2000);
		// trigger1.computeFirstFireTime(null);
		// trigger2.computeFirstFireTime(null);
		// trigger3.computeFirstFireTime(null);
		this.hazelcastJobStore.storeTrigger(trigger1, false);
		// this.fJobStore.storeTrigger(trigger2, false);
		// this.fJobStore.storeTrigger(trigger3, false);

		Date nextFireTime = trigger1.getNextFireTime();
		Date date = new Date(nextFireTime.getTime());
		long firstFireTime = date.getTime();

		Assert.assertTrue(this.hazelcastJobStore.acquireNextTriggers(10, 1, 0L).isEmpty());
		// assertEquals(
		// trigger2.getKey(),
		// this.fJobStore
		// .acquireNextTriggers(firstFireTime + 10000, 1, 0L)
		// .get(0).getKey());
		// assertEquals(
		// trigger3.getKey(),
		// this.fJobStore
		// .acquireNextTriggers(firstFireTime + 10000, 1, 0L)
		// .get(0).getKey());
		Assert.assertEquals(trigger1.getKey(), this.hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L)
				.get(0).getKey());
		Assert.assertTrue(this.hazelcastJobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).isEmpty());

		// release trigger3
		// this.fJobStore.releaseAcquiredTrigger(trigger3);
		// assertEquals(
		// trigger3,
		// this.fJobStore.acquireNextTriggers(
		// new Date(trigger1.getNextFireTime().getTime())
		// .getTime() + 10000, 1, 1L).get(0));
	}

	@AfterClass
	public void cleanUp() {
		hazelcastJobStore.shutdown();
		hazelcastInstance.shutdown();
	}
}
