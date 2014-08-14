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
import org.quartz.impl.JobDetailImpl;
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
	protected JobDetailImpl fJobDetail;

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

	protected int buildTriggerIndex = 0;

	@AfterMethod
	public void cleanUpAfterEachTest() throws JobPersistenceException {
		hazelcastJobStore.clearAllSchedulingData();
	}

	protected HazelcastJobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);
		return hazelcastJobStore;
	}

	protected JobDetailImpl buildJob(String jobName) {
		return buildJob(jobName, DEFAULT_GROUP);
	}

	protected JobDetailImpl buildJob(String jobName, String grouName) {
		JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity(jobName, grouName).build();
		return (JobDetailImpl) job;
	}

	protected void storeJob(String jobName) throws ObjectAlreadyExistsException, JobPersistenceException {
		storeJob(buildJob(jobName));
	}

	protected void storeJob(JobDetailImpl jobDetailImpl) throws ObjectAlreadyExistsException, JobPersistenceException {
		this.hazelcastJobStore.storeJob(jobDetailImpl, false);
	}

	protected JobDetail retrieveJob(String jobName) throws JobPersistenceException {
		return this.hazelcastJobStore.retrieveJob(new JobKey(jobName, DEFAULT_GROUP));
	}

	protected OperableTrigger buildTrigger() {
		return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP);
	}

	protected OperableTrigger buildTrigger(String triggerName, String groupName) {
		SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, groupName).withSchedule(schedule)
				.build();
		return (OperableTrigger) trigger;
	}

	protected OperableTrigger buildTrigger(String triggerName, String groupName, JobDetail jobDetail) {
		SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, groupName).withSchedule(schedule)
				.forJob(jobDetail).build();
		return (OperableTrigger) trigger;
	}

	protected OperableTrigger retrieveTrigger(OperableTrigger trigger1) throws JobPersistenceException {
		return hazelcastJobStore.retrieveTrigger(trigger1.getKey());
	}

	protected void storeTrigger(OperableTrigger trigger1) throws ObjectAlreadyExistsException, JobPersistenceException {
		hazelcastJobStore.storeTrigger(trigger1, false);
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
