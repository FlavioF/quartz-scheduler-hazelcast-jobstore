package org.ameausoone;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.fest.assertions.Assertions;
import org.quartz.DateBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Slf4j
public class TestHazelcastJobStore {

	private HazelcastInstance hazelcastInstance;
	private HazelcastJobStore hazelcastJobStore;
	private SampleSignaler fSignaler;
	private JobDetailImpl fJobDetail;

	@SuppressWarnings("deprecation")
	@BeforeClass
	public void setUp() throws SchedulerException, InterruptedException {
		log.info("SetUp");
		Config config = new Config();
		config.setProperty("hazelcast.logging.type", "slf4j");
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		Thread.sleep(1000);

		this.fSignaler = new SampleSignaler();
		hazelcastJobStore = createJobStore("jobstore");
		ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
		log.debug("Before loadHelper.initialize()");
		loadHelper.initialize();

		hazelcastJobStore.initialize(loadHelper, this.fSignaler);
		hazelcastJobStore.setInstanceId("SimpleInstance");
		hazelcastJobStore.schedulerStarted();

		// this.fJobDetail = this.fJobDetail = new JobDetailImpl("job1",
		// "jobGroup1", MyJob.class);
		// this.hazelcastJobStore.storeJob(this.fJobDetail, false);
	}

	protected HazelcastJobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);
		return hazelcastJobStore;
	}

	@Test
	public void storeSimpleJob() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetailImpl jobDetailImpl = new JobDetailImpl("job20", "jobGroup20",
				MyJob.class);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		JobDetail retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey(
				"job20", "jobGroup20"));
		assertThat(retrieveJob).isNotNull();
	}

	@Test(expectedExceptions = { ObjectAlreadyExistsException.class })
	public void storeTwiceSameJob() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetailImpl jobDetailImpl = new JobDetailImpl("job21", "jobGroup21",
				MyJob.class);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
	}

	@Test
	public void testRemoveJob() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetailImpl jobDetailImpl = new JobDetailImpl("job22", "jobGroup22",
				MyJob.class);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		JobDetail retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey(
				"job22", "jobGroup22"));
		assertThat(retrieveJob).isNotNull();
		boolean removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl
				.getKey());
		assertThat(removeJob).isTrue();
		retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey("job22",
				"jobGroup22"));
		assertThat(retrieveJob).isNull();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testRemoveJobs() throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetailImpl jobDetailImpl = new JobDetailImpl("job24", "jobGroup24",
				MyJob.class);
		JobDetailImpl jobDetailImpl2 = new JobDetailImpl("job25", "jobGroup24",
				MyJob.class);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		this.hazelcastJobStore.storeJob(jobDetailImpl2, false);
		JobDetail retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey(
				"job24", "jobGroup24"));
		assertThat(retrieveJob).isNotNull();
		List<JobKey> jobKeyList = Lists.newArrayList(jobDetailImpl.getKey(),
				jobDetailImpl2.getKey());
		boolean removeJob = this.hazelcastJobStore.removeJobs(jobKeyList);
		assertThat(removeJob).isTrue();
		retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey("job24",
				"jobGroup24"));
		assertThat(retrieveJob).isNull();
		retrieveJob = this.hazelcastJobStore.retrieveJob(new JobKey("job25",
				"jobGroup24"));
		assertThat(retrieveJob).isNull();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl.getKey());
		assertThat(removeJob).isFalse();
		removeJob = this.hazelcastJobStore.removeJob(jobDetailImpl2.getKey());
		assertThat(removeJob).isFalse();
	}

	@Test
	public void testCheckExistsJob() throws JobPersistenceException {
		JobDetailImpl jobDetailImpl = new JobDetailImpl("job23", "jobGroup23",
				MyJob.class);
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
		boolean checkExists = hazelcastJobStore.checkExists(jobDetailImpl
				.getKey());
		Assertions.assertThat(checkExists).isTrue();
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
		OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1",
				"triggerGroup1", this.fJobDetail.getName(),
				this.fJobDetail.getGroup(), new Date(baseFireTime + 200000),
				new Date(baseFireTime + 200000), 2, 2000);
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

		Assert.assertTrue(this.hazelcastJobStore.acquireNextTriggers(10, 1, 0L)
				.isEmpty());
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
		Assert.assertEquals(trigger1.getKey(), this.hazelcastJobStore
				.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0)
				.getKey());
		Assert.assertTrue(this.hazelcastJobStore.acquireNextTriggers(
				firstFireTime + 10000, 1, 0L).isEmpty());

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
		// hazelcastInstance.shutdown();
	}
}
