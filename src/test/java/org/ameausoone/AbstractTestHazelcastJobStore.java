package org.ameausoone;

import static org.quartz.Scheduler.DEFAULT_GROUP;
import lombok.extern.slf4j.Slf4j;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Slf4j
public abstract class AbstractTestHazelcastJobStore {

	private HazelcastInstance hazelcastInstance;
	protected HazelcastJobStore hazelcastJobStore;
	protected SampleSignaler fSignaler;
	// protected JobDetailImpl fJobDetail;
	protected int buildTriggerIndex = 0;
	protected int buildJobIndex = 0;

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

		// this.fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		// this.hazelcastJobStore.storeJob(this.fJobDetail, false);
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

	protected JobDetail buildJob() {
		return buildJob("jobName" + buildJobIndex++, DEFAULT_GROUP);
	}

	protected JobDetail buildJob(String jobName) {
		return buildJob(jobName, DEFAULT_GROUP);
	}

	protected JobDetail buildJob(String jobName, String grouName) {
		JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity(jobName, grouName).build();
		return job;
	}

	protected JobDetail storeJob(String jobName) throws ObjectAlreadyExistsException, JobPersistenceException {
		return storeJob(buildJob(jobName));
	}

	protected JobDetail storeJob(JobDetail jobDetail) throws ObjectAlreadyExistsException, JobPersistenceException {
		this.hazelcastJobStore.storeJob(jobDetail, false);
		return (JobDetail) jobDetail;
	}

	protected JobDetail buildAndStoreJob() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail buildJob = buildJob();
		this.hazelcastJobStore.storeJob(buildJob, false);
		return (JobDetail) buildJob;
	}

	protected JobDetail buildAndStoreJobWithTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		JobDetail buildJob = buildJob();
		this.hazelcastJobStore.storeJob(buildJob, false);
		Trigger trigger = buildTrigger(buildJob);
		storeTrigger(trigger);
		return (JobDetail) buildJob;
	}

	protected JobDetail retrieveJob(String jobName) throws JobPersistenceException {
		return this.hazelcastJobStore.retrieveJob(new JobKey(jobName, DEFAULT_GROUP));
	}

	/**
	 * @return Trigger with default (and incremented) name and default group, and attached to a (already stored) job.
	 */
	protected Trigger buildTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP, buildAndStoreJob());
	}

	protected Trigger buildTrigger(JobDetail jobDetail) {
		return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP, jobDetail);
	}

	protected Trigger buildAndStoreTrigger() throws ObjectAlreadyExistsException, JobPersistenceException {
		Trigger trigger = buildTrigger();
		storeTrigger(trigger);
		return trigger;
	}

	/**
	 * @return build Trigger with specified name and group, unattached to a job.
	 */
	protected Trigger buildTrigger(String triggerName, String groupName) {
		SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, groupName).withSchedule(schedule)
				.build();
		return (Trigger) trigger;
	}

	protected Trigger buildTrigger(String triggerName, String groupName, JobDetail jobDetail) {
		SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, groupName).withSchedule(schedule)
				.forJob(jobDetail).build();

		return trigger;
	}

	protected Trigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		return hazelcastJobStore.retrieveTrigger(triggerKey);
	}

	protected void storeTrigger(Trigger trigger) throws ObjectAlreadyExistsException, JobPersistenceException {
		hazelcastJobStore.storeTrigger((OperableTrigger) trigger, false);
	}

	protected void storeCalendar(String calName) throws ObjectAlreadyExistsException, JobPersistenceException {
		hazelcastJobStore.storeCalendar(calName, new BaseCalendar(), false, false);
	}

	@AfterClass
	public void cleanUp() {
		hazelcastJobStore.shutdown();
		hazelcastInstance.shutdown();
	}
}
