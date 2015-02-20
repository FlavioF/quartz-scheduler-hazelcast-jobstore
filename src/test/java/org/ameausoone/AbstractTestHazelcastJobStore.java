package org.ameausoone;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import static org.quartz.Scheduler.DEFAULT_GROUP;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.jobs.NoOpJob;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class AbstractTestHazelcastJobStore {

  private HazelcastInstance hazelcastInstance;
  protected JobStore jobStore;
  protected SampleSignaler fSignaler;
  protected int buildTriggerIndex = 0;
  protected int buildJobIndex = 0;

  // protected JobStore jobStore;
  protected JobDetailImpl fJobDetail;

  @BeforeClass
  public void setUp() throws SchedulerException, InterruptedException {

    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    hazelcastInstance = Hazelcast.newHazelcastInstance(config);

    this.fSignaler = new SampleSignaler();

    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    this.jobStore = createJobStore("AbstractJobStoreTest");
    this.jobStore.initialize(loadHelper, this.fSignaler);
    this.jobStore.schedulerStarted();

    this.fJobDetail = new JobDetailImpl("job1", "jobGroup1", NoOpJob.class);
    this.fJobDetail.setDurability(true);
    this.jobStore.storeJob(this.fJobDetail, false);
  }

  @AfterClass
  public void tearDown() {
    hazelcastInstance.shutdown();
  }

  @AfterMethod
  public void cleanUpAfterEachTest() throws JobPersistenceException {
    jobStore.clearAllSchedulingData();

  }

  protected HazelcastJobStore createJobStore(String name) {
    return new HazelcastJobStore();
  }

  protected JobDetail buildJob() {
    return buildJob("jobName" + buildJobIndex++, DEFAULT_GROUP);
  }

  protected JobDetail buildJob(String jobName) {
    return buildJob(jobName, DEFAULT_GROUP);
  }

  protected JobDetail buildJob(String jobName, String grouName) {
    JobDetail job = JobBuilder.newJob(Job.class)
        .withIdentity(jobName, grouName).build();
    return job;
  }

  protected JobDetail storeJob(String jobName)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    return storeJob(buildJob(jobName));
  }

  protected JobDetail storeJob(JobDetail jobDetail)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    this.jobStore.storeJob(jobDetail, false);
    return (JobDetail) jobDetail;
  }

  protected JobDetail buildAndStoreJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail buildJob = buildJob();
    this.jobStore.storeJob(buildJob, false);
    return (JobDetail) buildJob;
  }

  protected JobDetail buildAndStoreJobWithTrigger()
      throws ObjectAlreadyExistsException, JobPersistenceException {
    JobDetail buildJob = buildJob();
    this.jobStore.storeJob(buildJob, false);
    Trigger trigger = buildTrigger(buildJob);
    storeTrigger(trigger);
    return (JobDetail) buildJob;
  }

  protected JobDetail retrieveJob(String jobName)
      throws JobPersistenceException {
    return this.jobStore.retrieveJob(new JobKey(jobName, DEFAULT_GROUP));
  }

  /**
   * @return Trigger with default (and incremented) name and default group, and
   *         attached to a (already stored) job.
   */
  protected Trigger buildTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP,
        buildAndStoreJob());
  }

  protected Trigger buildTrigger(JobDetail jobDetail) {
    return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP,
        jobDetail);
  }

  protected Trigger buildAndStoreTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildTrigger();
    storeTrigger(trigger);
    return trigger;
  }

  /**
   * @return build Trigger with specified name and group, unattached to a job.
   */
  protected Trigger buildTrigger(String triggerName, String groupName) {
    SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(triggerName, groupName).withSchedule(schedule).build();
    return (Trigger) trigger;
  }

  protected Trigger buildTrigger(String triggerName, String groupName,
      JobDetail jobDetail) {
    SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(triggerName, groupName).withSchedule(schedule)
        .forJob(jobDetail).build();
    ((OperableTrigger) trigger).computeFirstFireTime(null);
    return trigger;
  }

  protected Trigger retrieveTrigger(TriggerKey triggerKey)
      throws JobPersistenceException {
    return jobStore.retrieveTrigger(triggerKey);
  }

  protected void storeTrigger(Trigger trigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    jobStore.storeTrigger((OperableTrigger) trigger, false);
  }

  protected void storeCalendar(String calName)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    jobStore.storeCalendar(calName, new BaseCalendar(), false, false);
  }

  @AfterClass
  public void cleanUp() {
    jobStore.shutdown();
    hazelcastInstance.shutdown();
  }

  public static class SampleSignaler implements SchedulerSignaler {
    volatile int fMisfireCount = 0;

    @Override
    public void notifyTriggerListenersMisfired(Trigger trigger) {
      log.debug("Trigger misfired: " + trigger.getKey() + ", fire time: "
          + trigger.getNextFireTime());
      fMisfireCount++;
    }

    @Override
    public void signalSchedulingChange(long candidateNewNextFireTime) {
    }

    @Override
    public void notifySchedulerListenersFinalized(Trigger trigger) {
    }

    @Override
    public void notifySchedulerListenersJobDeleted(JobKey jobKey) {
    }

    @Override
    public void notifySchedulerListenersError(String string,
        SchedulerException jpe) {
    }
  }
}
