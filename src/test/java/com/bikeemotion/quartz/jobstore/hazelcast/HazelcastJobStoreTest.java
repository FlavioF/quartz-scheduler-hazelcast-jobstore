package com.bikeemotion.quartz.jobstore.hazelcast;

import com.beust.jcommander.internal.Maps;
import com.bikeemotion.quartz.AbstractTest;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;

import static org.quartz.DateBuilder.newDate;
import static org.quartz.Scheduler.DEFAULT_GROUP;

import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.jobs.NoOpJob;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class HazelcastJobStoreTest extends AbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(HazelcastJobStoreTest.class);

  private SampleSignaler fSignaler;
  private JobDetail jobDetail;

  @BeforeClass
  public void setUp()
    throws SchedulerException, InterruptedException {

    fSignaler = new SampleSignaler();

    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    hazelcastInstance = Hazelcast.newHazelcastInstance(config);

    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    HazelcastJobStore.setHazelcastClient(hazelcastInstance);
    jobStore = createJobStore("AbstractJobStoreTest");
    jobStore.initialize(loadHelper, this.fSignaler);
    jobStore.schedulerStarted();

    jobDetail = JobBuilder.newJob(NoOpJob.class).withIdentity("job1", "jobGroup1").build();
    jobStore.storeJob(jobDetail, false);
  }

  @AfterClass
  public void tearDown() {

    jobStore.shutdown();
    hazelcastInstance.shutdown();
  }

  @BeforeMethod
  public void setUpBeforeEachTest() {}

  @AfterMethod
  public void cleanUpAfterEachTest()
    throws JobPersistenceException {

    jobStore.clearAllSchedulingData();

  }

  @Test
  public void testShuttingDownWithShuttingDownHazelcast()
    throws SchedulerException {

    HazelcastInstance hazelcastInstance = createHazelcastInstance(UUID.randomUUID().toString());

    HazelcastJobStore jobStore = createJobStore("test-shutting-down-hazelcast");
    HazelcastJobStore.setHazelcastClient(hazelcastInstance);
    jobStore.schedulerStarted();
    jobStore.shutdown();

    assertFalse(hazelcastInstance.getLifecycleService().isRunning());
  }

  @Test
  public void testShuttingDownWithoutShuttingDownHazelcast()
    throws SchedulerException {

    HazelcastInstance hazelcastInstance = createHazelcastInstance(UUID.randomUUID().toString());

    HazelcastJobStore jobStore = createJobStore("test-shutting-down-hazelcast");
    HazelcastJobStore.setHazelcastClient(hazelcastInstance);
    jobStore.setShutdownHazelcastOnShutdown(false);
    jobStore.schedulerStarted();
    jobStore.shutdown();

    assertTrue(hazelcastInstance.getLifecycleService().isRunning());
  }

  @Test
  public void testAcquireNextTrigger()
    throws Exception {

    long baseFireTime = DateBuilder.newDate().build().getTime();

    JobDetail job = JobBuilder.newJob(NoOpJob.class).build();
    jobStore.storeJob(job, true);

    OperableTrigger t1 = buildAndComputeTrigger("trigger1", "testAcquireNextTrigger", job, baseFireTime + 2000);
    OperableTrigger t2 = buildAndComputeTrigger("trigger2", "testAcquireNextTrigger", job, baseFireTime + 500);
    OperableTrigger t3 = buildAndComputeTrigger("trigger3", "testAcquireNextTrigger", job, baseFireTime + 1000);

    assertTrue(jobStore.acquireNextTriggers(baseFireTime, 1, 0L).isEmpty());

    jobStore.storeTrigger(t1, false);
    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 2000, 1, 0L).get(0), t1);

    jobStore.storeTrigger(t2, false);
    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 600, 1, 0L).get(0), t2);

    assertTrue(jobStore.acquireNextTriggers(baseFireTime + 600, 1, 0L).isEmpty());

    jobStore.storeTrigger(t3, false);
    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 5000, 1, 0L).get(0), t3);

    // release trigger3
    jobStore.releaseAcquiredTrigger(t3);
    assertEquals(jobStore.acquireNextTriggers(t3.getNextFireTime().getTime() + 5000, 1, 1L).get(0), t3);

    assertTrue(jobStore.acquireNextTriggers(baseFireTime + 10000, 1, 0L).isEmpty());

    jobStore.removeTrigger(t1.getKey());
    jobStore.removeTrigger(t2.getKey());
    jobStore.removeTrigger(t3.getKey());
  }

  @Test
  public void testAcquireNextTriggerAfterMissFire()
    throws Exception {
    long oldThreshold = jobStore.getMisfireThreshold();
    long baseFireTime = newDate().build().getTime();

    JobDetail job = JobBuilder.newJob(NoOpJob.class).build();
    jobStore.storeJob(job, true);
    jobStore.setMisfireThreshold(500);

    OperableTrigger t1 = buildAndComputeTrigger(
        "trigger1",
        "testAcquireNextTriggerAfterMissFire",
        job,
        baseFireTime + 500,
        null,
        SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow());
    OperableTrigger t2 = buildAndComputeTrigger(
        "trigger2",
        "testAcquireNextTriggerAfterMissFire",
        job,
        baseFireTime + 500,
        null,
        SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow());

    jobStore.storeTrigger(t1, false);
    jobStore.storeTrigger(t2, false);

    List<OperableTrigger> acquired = jobStore.acquireNextTriggers(baseFireTime + 1000, 1, 0L);
    assertEquals(acquired.size(), 1);
    jobStore.triggersFired(acquired);
    Thread.sleep(800);

    long now = newDate().build().getTime();
    //misfired is acquired immediately
    assertEquals(jobStore.acquireNextTriggers(now + 1000, 1, 0L).size(), 1);

    jobStore.removeTrigger(t1.getKey());
    jobStore.removeTrigger(t2.getKey());
    
    jobStore.setMisfireThreshold(oldThreshold);

  }

  @Test
  public void testAcquireNextTriggerAfterMissFire_triggersImmediately_ifNextScheduleTimeInRange()
    throws Exception {

    long baseFireTime = newDate().build().getTime();

    JobDetail job = JobBuilder.newJob(NoOpJob.class).build();
    jobStore.storeJob(job, true);

    SimpleScheduleBuilder scheduleBuilder = simpleSchedule().withIntervalInSeconds(3).repeatForever()
        .withMisfireHandlingInstructionFireNow();
    OperableTrigger t1 = buildAndComputeTrigger("trigger1", "triggerGroup1", job, baseFireTime + 500, null,
        scheduleBuilder);
    jobStore.storeTrigger(t1, false);

    assertAcquiredAndRelease(baseFireTime, 1);

    Thread.sleep(5000);
    // missed one execution, next execution is immediate
    assertAcquiredAndRelease(newDate().build().getTime() + 500, 1);

    Thread.sleep(1000);
    // next execution is at 8 seconds tick (5 + 3) outside interval (6 sec to 7 sec tick), no triggers should be acquired
    assertAcquiredAndRelease(newDate().build().getTime() + 1050, 0);
    // increase interval to contain 8 seconds tick
    assertAcquiredAndRelease(newDate().build().getTime() + 2550, 1);
  }

  @Test
  public void testAcquireNextTriggerAfterMissFire_doesNotTrigger_ifNextScheduleTimeOutOfRange()
    throws Exception {

    long baseFireTime = newDate().build().getTime();

    JobDetail job = JobBuilder.newJob(NoOpJob.class).build();
    jobStore.storeJob(job, true);

    ScheduleBuilder scheduleBuilder = simpleSchedule().withIntervalInSeconds(3).repeatForever()
        .withMisfireHandlingInstructionNextWithExistingCount();
    OperableTrigger t1 = buildAndComputeTrigger("trigger1", "triggerGroup1", job, baseFireTime + 500, null,
        scheduleBuilder);
    jobStore.storeTrigger(t1, false);

    assertAcquiredAndRelease(baseFireTime, 1);

    Thread.sleep(5000);
    // missed one execution (3 seconds tick is more than 1 seconds ago), next execution (at 6 seconds tick) is not yet picked up
    assertAcquiredAndRelease(newDate().build().getTime() + 250, 0);

    // try acquire on larger interval (containing 6 sec tick)
    assertAcquiredAndRelease(newDate().build().getTime() + 1550, 1);

  }

  @Test
  public void testAcquireNextTriggerBatch()
    throws Exception {

    long baseFireTime = DateBuilder.newDate().build().getTime();

    jobStore.storeJob(jobDetail, true);

    OperableTrigger trigger1 = buildTrigger("trigger1",
        "testAcquireNextTriggerBatch",
        jobDetail,
        baseFireTime + 2000,
        baseFireTime + 2005);
    OperableTrigger trigger2 = buildTrigger("trigger2",
        "testAcquireNextTriggerBatch",
        jobDetail,
        baseFireTime + 2100,
        baseFireTime + 2105);
    OperableTrigger trigger3 = buildTrigger("trigger3",
        "testAcquireNextTriggerBatch",
        jobDetail,
        baseFireTime + 2200,
        baseFireTime + 2205);
    OperableTrigger trigger4 = buildTrigger("trigger4",
        "testAcquireNextTriggerBatch",
        jobDetail,
        baseFireTime + 2300,
        baseFireTime + 2305);
    OperableTrigger trigger5 = buildTrigger("trigger5",
        "testAcquireNextTriggerBatch",
        jobDetail,
        baseFireTime + 5000,
        baseFireTime + 7000);

    trigger1.computeFirstFireTime(null);
    trigger2.computeFirstFireTime(null);
    trigger3.computeFirstFireTime(null);
    trigger4.computeFirstFireTime(null);
    trigger5.computeFirstFireTime(null);

    jobStore.storeTrigger(trigger1, false);
    jobStore.storeTrigger(trigger2, false);
    jobStore.storeTrigger(trigger3, false);
    jobStore.storeTrigger(trigger4, false);
    jobStore.storeTrigger(trigger5, false);

    List<OperableTrigger> acquiredTriggers = jobStore.acquireNextTriggers(baseFireTime + 1500, 3, 1000L);
    assertEquals(acquiredTriggers.size(), 3);

    jobStore.releaseAcquiredTrigger(trigger1);
    jobStore.releaseAcquiredTrigger(trigger2);
    jobStore.releaseAcquiredTrigger(trigger3);

    acquiredTriggers = jobStore.acquireNextTriggers(baseFireTime + 1500, 4, 1000L);
    assertEquals(acquiredTriggers.size(), 4);

    jobStore.releaseAcquiredTrigger(trigger1);
    jobStore.releaseAcquiredTrigger(trigger2);
    jobStore.releaseAcquiredTrigger(trigger3);
    jobStore.releaseAcquiredTrigger(trigger4);

    acquiredTriggers = jobStore.acquireNextTriggers(baseFireTime + 1500, 5, 1000L);
    assertEquals(acquiredTriggers.size(), 4);

    jobStore.releaseAcquiredTrigger(trigger1);
    jobStore.releaseAcquiredTrigger(trigger2);

    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 2001, 5, 0L).size(), 1);
    jobStore.releaseAcquiredTrigger(trigger1);

    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 2000, 5, 150L).size(), 2);
    jobStore.releaseAcquiredTrigger(trigger1);

    assertEquals(jobStore.acquireNextTriggers(baseFireTime + 2000, 5, 150L).size(), 1);
    jobStore.releaseAcquiredTrigger(trigger1);

    jobStore.removeTrigger(trigger1.getKey());
    jobStore.removeTrigger(trigger2.getKey());
    jobStore.removeTrigger(trigger3.getKey());
    jobStore.removeTrigger(trigger4.getKey());
    jobStore.removeTrigger(trigger5.getKey());
  }

  @Test
  public void testTriggerStates()
    throws Exception {

    JobDetail newJob = JobBuilder.newJob(NoOpJob.class).withIdentity("job1", "testTriggerStates").build();
    jobStore.storeJob(newJob, false);

    OperableTrigger trigger = buildTrigger("trigger1",
        "testTriggerStates",
        newJob,
        DateBuilder.newDate().build().getTime() + 1000,
        DateBuilder.newDate().build().getTime() + 2000);

    trigger.computeFirstFireTime(null);

    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NONE);

    jobStore.storeTrigger(trigger, false);
    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);

    jobStore.pauseTrigger(trigger.getKey());
    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.PAUSED);

    jobStore.resumeTrigger(trigger.getKey());
    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);

    OperableTrigger rt1 = jobStore.acquireNextTriggers(
        new Date(trigger.getNextFireTime().getTime()).getTime() + 10000,
        1,
        1L)
        .get(0);
    assertNotNull(rt1);
    jobStore.releaseAcquiredTrigger(rt1);

    OperableTrigger rt2 = jobStore.acquireNextTriggers(
        new Date(rt1.getNextFireTime().getTime()).getTime() + 1500,
        1,
        1L)
        .get(0);

    assertNotNull(rt2);
    assertEquals(rt2.getJobKey(), rt1.getJobKey());

    assertTrue(jobStore.acquireNextTriggers(new Date(rt2.getNextFireTime().getTime()).getTime() + 1500,
        1,
        1L)
        .isEmpty());
  }

  @Test
  public void testStoreTriggerReplacesTrigger()
    throws Exception {

    JobDetail job = buildJob("replacesTrigJob99", "replacesTrigJobGroup");
    jobStore.storeJob(job, false);

    OperableTrigger tr = buildTrigger("stReplacesTrigger1", "stReplacesTriggerGroup", job, new Date().getTime());
    tr.setCalendarName(null);

    jobStore.storeTrigger(tr, false);
    assertEquals(jobStore.retrieveTrigger(tr.getKey()), tr);

    try {
      jobStore.storeTrigger(tr, false);
      fail("an attempt to store duplicate trigger succeeded");
    } catch (ObjectAlreadyExistsException ex) {
      // expected
    }

    tr.setCalendarName("QQ");
    jobStore.storeTrigger(tr, true);
    assertEquals(jobStore.retrieveTrigger(tr.getKey()), tr);
    assertEquals(jobStore.retrieveTrigger(tr.getKey()).getCalendarName(), "QQ", "StoreJob doesn't replace triggers");
  }

  @Test
  public void testPauseJobGroupPausesNewJob()
    throws Exception {

    final String jobGroup = "PauseJobGroupPausesNewJobGroup";

    JobDetail job1 = buildJob("PauseJobGroupPausesNewJob", jobGroup);
    jobStore.storeJob(job1, false);
    jobStore.pauseJobs(GroupMatcher.jobGroupEquals(jobGroup));

    JobDetail job2 = buildJob("PauseJobGroupPausesNewJob2", jobGroup);
    jobStore.storeJob(job2, false);

    OperableTrigger tr = buildTrigger(
        "PauseJobGroupPausesNewJobTrigger",
        "PauseJobGroupPausesNewJobTriggerGroup",
        job2,
        new Date().getTime());

    jobStore.storeTrigger(tr, false);
    assertEquals(jobStore.getTriggerState(tr.getKey()), Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testStoreAndRetrieveJobs()
    throws Exception {

    final int nJobs = 10;

    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testStoreAndRetrieveJobs");
    store.initialize(loadHelper, schedSignaler);

    // Store jobs.
    for (int i = 0; i < nJobs; i++) {
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i).build();
      store.storeJob(job, false);
    }

    // Retrieve jobs.
    for (int i = 0; i < nJobs; i++) {
      JobKey jobKey = JobKey.jobKey("job" + i);
      JobDetail storedJob = store.retrieveJob(jobKey);
      Assert.assertEquals(storedJob.getKey(), jobKey);
    }
  }

  @Test
  public void testStoreAndRetriveTriggers()
    throws Exception {

    final int nJobs = 10;

    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testStoreAndRetriveTriggers");
    store.initialize(loadHelper, schedSignaler);

    // Store jobs and triggers.
    for (int i = 0; i < nJobs; i++) {
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i).build();
      store.storeJob(job, true);

      OperableTrigger trigger = buildTrigger("job" + i, DEFAULT_GROUP, job);
      store.storeTrigger((OperableTrigger) trigger, true);
    }
    // Retrieve jobs and triggers.
    for (int i = 0; i < nJobs; i++) {
      JobKey jobKey = JobKey.jobKey("job" + i);
      JobDetail storedJob = store.retrieveJob(jobKey);
      Assert.assertEquals(storedJob.getKey(), jobKey);

      TriggerKey triggerKey = TriggerKey.triggerKey("job" + i);
      OperableTrigger storedTrigger = store.retrieveTrigger(triggerKey);
      Assert.assertEquals(storedTrigger.getKey(), triggerKey);
    }
  }

  @Test
  public void testAcquireTriggers()
    throws Exception {

    final int nJobs = 10;

    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testAcquireTriggers");
    store.initialize(loadHelper, schedSignaler);

    // Setup: Store jobs and triggers.
    long MIN = 60 * 1000L;
    Date startTime0 = new Date(System.currentTimeMillis() + MIN); // a min from
    // now.
    for (int i = 0; i < nJobs; i++) {
      Date startTime = new Date(startTime0.getTime() + i * MIN); // a min apart
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i).build();
      SimpleScheduleBuilder schedule = SimpleScheduleBuilder.repeatMinutelyForever(2);
      OperableTrigger trigger = (OperableTrigger) newTrigger()
          .withIdentity("job" + i)
          .withSchedule(schedule).forJob(job)
          .startAt(startTime)
          .build();

      // Manually trigger the first fire time computation that scheduler would
      // do. Otherwise
      // the store.acquireNextTriggers() will not work properly.
      Date fireTime = trigger.computeFirstFireTime(null);
      Assert.assertNotNull(fireTime);

      store.storeJobAndTrigger(job, trigger);
    }

    // Test acquire one trigger at a time
    for (int i = 0; i < nJobs; i++) {
      long noLaterThan = (startTime0.getTime() + i * MIN);
      int maxCount = 1;
      long timeWindow = 0;
      List<OperableTrigger> triggers = store.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
      Assert.assertEquals(triggers.size(), 1);
      Assert.assertEquals(triggers.get(0).getKey().getName(), "job" + i);

      // Let's remove the trigger now.
      store.removeJob(triggers.get(0).getJobKey());
    }
  }

  @Test
  public void testAcquireTriggersInBatch()
    throws Exception {

    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testAcquireTriggersInBatch");
    store.initialize(loadHelper, schedSignaler);

    // Setup: Store jobs and triggers.
    long MIN = 60 * 1000L;
    Date startTime0 = new Date(System.currentTimeMillis() + MIN); // a min from
    // now.
    for (int i = 0; i < 10; i++) {
      Date startTime = new Date(startTime0.getTime() + i * MIN); // a min apart
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i).build();
      SimpleScheduleBuilder schedule = SimpleScheduleBuilder.repeatMinutelyForever(2);
      OperableTrigger trigger = (OperableTrigger) newTrigger()
          .withIdentity("job" + i)
          .withSchedule(schedule)
          .forJob(job)
          .startAt(startTime)
          .build();

      // Manually trigger the first fire time computation that scheduler would
      // do. Otherwise
      // the store.acquireNextTriggers() will not work properly.
      Date fireTime = trigger.computeFirstFireTime(null);
      Assert.assertNotNull(fireTime);

      store.storeJobAndTrigger(job, trigger);
    }

    // Test acquire batch of triggers at a time
    long noLaterThan = startTime0.getTime() + 10 * MIN;
    int maxCount = 7;
    // time window needs to be big to be able to pick up multiple triggers when
    // they are a minute apart
    long timeWindow = 8 * MIN;
    List<OperableTrigger> triggers = store.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
    Assert.assertEquals(triggers.size(), 7);
  }

  @Test
  public void testStoreSimpleJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String jobName = "job20";
    storeJob(jobName);
    JobDetail retrieveJob = retrieveJob(jobName);
    assertNotNull(retrieveJob);
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void storeTwiceSameJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String jobName = "job21";
    storeJob(jobName);
    storeJob(jobName);
  }

  @Test
  public void testRemoveJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String jobName = "job22";
    JobDetail jobDetail = buildJob(jobName);
    storeJob(jobDetail);

    JobDetail retrieveJob = retrieveJob(jobName);
    assertNotNull(retrieveJob);

    OperableTrigger trigger = buildTrigger(jobDetail);
    jobStore.storeTrigger(trigger, false);

    assertNotNull(retrieveTrigger(trigger.getKey()));

    boolean removeJob = jobStore.removeJob(jobDetail.getKey());
    assertTrue(removeJob);
    retrieveJob = retrieveJob(jobName);
    assertNull(retrieveJob);

    assertNull(retrieveTrigger(trigger.getKey()));

    removeJob = jobStore.removeJob(jobDetail.getKey());
    assertFalse(removeJob);
  }

  @Test
  public void testRemoveJobs()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String jobName = "job24";
    JobDetail jobDetail = buildJob(jobName);

    String jobName2 = "job25";
    JobDetail jobDetailImpl2 = buildJob(jobName2);

    jobStore.storeJob(jobDetail, false);
    jobStore.storeJob(jobDetailImpl2, false);

    JobDetail retrieveJob = retrieveJob(jobName);
    assertNotNull(retrieveJob);

    List<JobKey> jobKeyList = Lists.newArrayList(jobDetail.getKey(), jobDetailImpl2.getKey());
    boolean removeJob = jobStore.removeJobs(jobKeyList);
    assertTrue(removeJob);

    retrieveJob = retrieveJob(jobName);
    assertNull(retrieveJob);
    retrieveJob = retrieveJob(jobName2);
    assertNull(retrieveJob);

    removeJob = jobStore.removeJob(jobDetail.getKey());
    assertFalse(removeJob);
    removeJob = jobStore.removeJob(jobDetailImpl2.getKey());
    assertFalse(removeJob);
  }

  @Test
  public void testCheckExistsJob()
    throws JobPersistenceException {

    JobDetail jobDetailImpl = buildJob("job23");
    jobStore.storeJob(jobDetailImpl, false);
    boolean checkExists = jobStore.checkExists(jobDetailImpl.getKey());
    assertTrue(checkExists);
  }

  @Test(expectedExceptions = { JobPersistenceException.class })
  public void testStoreTriggerWithoutJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = (OperableTrigger) newTrigger().withIdentity("tKey1", "group").build();
    jobStore.storeTrigger(trigger1, false);
  }

  @Test
  public void testStoreTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();
    jobStore.storeTrigger(trigger1, false);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void testStoreTriggerThrowsAlreadyExists()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();
    jobStore.storeTrigger(trigger1, false);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
    jobStore.storeTrigger(trigger1, false);
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
  }

  @Test
  public void testStoreTriggerTwice()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();

    jobStore.storeTrigger(trigger1, false);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
    jobStore.storeTrigger((OperableTrigger) trigger1, true);
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
  }

  @Test
  public void testRemoveTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail storeJob = storeJob(buildJob("job"));
    OperableTrigger trigger1 = buildTrigger(storeJob);
    TriggerKey triggerKey = trigger1.getKey();
    jobStore.storeTrigger(trigger1, false);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
    boolean removeTrigger = jobStore.removeTrigger(triggerKey);
    assertTrue(removeTrigger);
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNull(retrieveTrigger);
    removeTrigger = jobStore.removeTrigger(triggerKey);
    assertFalse(removeTrigger);

    Trigger.TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.NONE);
  }

  @Test
  public void testRemoveTriggers()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();
    OperableTrigger trigger2 = buildTrigger();

    jobStore.storeTrigger(trigger1, false);
    jobStore.storeTrigger(trigger2, false);

    List<TriggerKey> triggerKeys = Lists.newArrayList(trigger1.getKey(), trigger2.getKey());
    boolean removeTriggers = jobStore.removeTriggers(triggerKeys);
    assertTrue(removeTriggers);
  }

  @Test
  public void testTriggerCheckExists()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();
    TriggerKey triggerKey = trigger1.getKey();

    boolean checkExists = jobStore.checkExists(triggerKey);
    assertFalse(checkExists);

    jobStore.storeTrigger(trigger1, false);

    checkExists = jobStore.checkExists(triggerKey);
    assertTrue(checkExists);
  }

  @Test
  public void testReplaceTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildTrigger();

    jobStore.storeTrigger(trigger1, false);

    OperableTrigger newTrigger = buildTrigger();

    TriggerKey triggerKey = trigger1.getKey();
    boolean replaceTrigger = jobStore.replaceTrigger(triggerKey, (OperableTrigger) newTrigger);
    assertTrue(replaceTrigger);
    OperableTrigger retrieveTrigger = jobStore.retrieveTrigger(triggerKey);
    assertEquals(retrieveTrigger, newTrigger);
  }

  @Test
  public void testStoreJobAndTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail jobDetailImpl = buildJob("job30");

    OperableTrigger trigger1 = buildTrigger();
    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
    JobDetail retrieveJob = jobStore.retrieveJob(jobDetailImpl.getKey());
    assertNotNull(retrieveJob);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void testStoreJobAndTriggerThrowJobAlreadyExists()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    JobDetail jobDetailImpl = buildJob("job31");
    OperableTrigger trigger1 = buildTrigger();
    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
    JobDetail retrieveJob = jobStore.retrieveJob(jobDetailImpl.getKey());
    assertNotNull(retrieveJob);
    OperableTrigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger);

    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
  }

  @Test
  public void storeCalendar()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String calName = "calendar";
    storeCalendar(calName);
    Calendar retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertNotNull(retrieveCalendar);
  }

  @Test
  public void testRemoveCalendar()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    String calName = "calendar1";
    storeCalendar(calName);

    Calendar retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertNotNull(retrieveCalendar);
    boolean calendarExisted = jobStore.removeCalendar(calName);
    assertTrue(calendarExisted);
    retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertNull(retrieveCalendar);
    calendarExisted = jobStore.removeCalendar(calName);
    assertFalse(calendarExisted);

  }

  @Test
  public void testClearAllSchedulingData()
    throws JobPersistenceException {

    assertEquals(jobStore.getNumberOfJobs(), 0);

    assertEquals(jobStore.getNumberOfTriggers(), 0);

    assertEquals(jobStore.getNumberOfCalendars(), 0);

    final String jobName = "job40";
    final JobDetail storeJob = storeJob(jobName);
    assertEquals(jobStore.getNumberOfJobs(), 1);

    jobStore.storeTrigger((OperableTrigger) buildTrigger(storeJob), false);
    assertEquals(jobStore.getNumberOfTriggers(), 1);

    jobStore.storeCalendar("calendar", new BaseCalendar(), false, false);
    assertEquals(jobStore.getNumberOfCalendars(), 1);

    jobStore.clearAllSchedulingData();
    assertEquals(jobStore.getNumberOfJobs(), 0);

    assertEquals(jobStore.getNumberOfTriggers(), 0);

    assertEquals(jobStore.getNumberOfCalendars(), 0);
  }

  @Test
  public void testStoreSameJobNameWithDifferentGroup()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    storeJob(buildJob("job40", "group1"));
    storeJob(buildJob("job40", "group2"));
    // Assert there is no exception throws
  }

  @Test
  public void testGetJobGroupNames()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail buildJob = buildJob("job40", "group1");
    storeJob(buildJob);
    storeJob(buildJob("job41", "group2"));
    List<String> jobGroupNames = jobStore.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 2);
    assertTrue(jobGroupNames.contains("group1"));
    assertTrue(jobGroupNames.contains("group2"));

    jobStore.removeJob(buildJob.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 1);
    assertTrue(jobGroupNames.contains("group2"));
  }

  @Test
  public void testJobKeyByGroup()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail job1group1 = buildJob("job1", "group1");
    storeJob(job1group1);
    JobDetail job1group2 = buildJob("job1", "group2");
    storeJob(job1group2);
    storeJob(buildJob("job2", "group2"));
    List<String> jobGroupNames = jobStore.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 2);
    assertTrue(jobGroupNames.contains("group1"));
    assertTrue(jobGroupNames.contains("group2"));

    jobStore.removeJob(job1group1.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 1);
    assertTrue(jobGroupNames.contains("group2"));

    jobStore.removeJob(job1group2.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 1);
    assertTrue(jobGroupNames.contains("group2"));
  }

  @Test
  public void testGetTriggerGroupNames()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail storeJob = storeJob(buildJob("job"));

    jobStore.storeTrigger(buildTrigger("trigger1", "group1", storeJob), false);
    jobStore.storeTrigger(buildTrigger("trigger2", "group2", storeJob), false);

    List<String> triggerGroupNames = jobStore.getTriggerGroupNames();
    assertEquals(triggerGroupNames.size(), 2);
    assertTrue(triggerGroupNames.contains("group1"));
    assertTrue(triggerGroupNames.contains("group2"));
  }

  @Test
  public void testCalendarNames()
    throws JobPersistenceException {

    storeCalendar("cal1");
    storeCalendar("cal2");
    List<String> calendarNames = jobStore.getCalendarNames();
    assertEquals(calendarNames.size(), 2);
    assertTrue(calendarNames.contains("cal1"));
    assertTrue(calendarNames.contains("cal2"));
  }

  @Test
  public void storeJobAndTriggers()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps
        .newHashMap();

    JobDetail job1 = buildJob();
    OperableTrigger trigger1 = buildTrigger(job1);
    Set<Trigger> set1 = Sets.newHashSet();
    set1.add(trigger1);
    triggersAndJobs.put(job1, set1);

    JobDetail job2 = buildJob();
    OperableTrigger trigger2 = buildTrigger(job2);
    Set<Trigger> set2 = Sets.newHashSet();
    set2.add(trigger2);
    triggersAndJobs.put(job2, set2);

    jobStore.storeJobsAndTriggers(triggersAndJobs, false);

    JobDetail retrieveJob1 = retrieveJob(job1.getKey().getName());
    assertNotNull(retrieveJob1);

    JobDetail retrieveJob2 = retrieveJob(job2.getKey().getName());
    assertNotNull(retrieveJob2);

    OperableTrigger retrieveTrigger1 = retrieveTrigger(trigger1.getKey());
    assertNotNull(retrieveTrigger1);

    OperableTrigger retrieveTrigger2 = retrieveTrigger(trigger2.getKey());
    assertNotNull(retrieveTrigger2);
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void storeJobAndTriggersThrowException()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps.newHashMap();
    JobDetail job1 = buildJob();
    storeJob(job1);
    OperableTrigger trigger1 = buildTrigger(job1);
    Set<Trigger> set1 = Sets.newHashSet();
    set1.add(trigger1);
    triggersAndJobs.put(job1, set1);
    jobStore.storeJobsAndTriggers(triggersAndJobs, false);
  }

  @Test
  public void testGetTriggersForJob()
    throws JobPersistenceException {

    JobDetail job = buildAndStoreJob();
    OperableTrigger trigger1 = buildTrigger(job);
    OperableTrigger trigger2 = buildTrigger(job);
    jobStore.storeTrigger(trigger1, false);
    jobStore.storeTrigger(trigger2, false);

    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(job.getKey());

    assertEquals(triggersForJob.size(), 2);
    assertTrue(triggersForJob.contains(trigger1));
    assertTrue(triggersForJob.contains(trigger2));
  }

  @Test
  public void testPauseTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger = buildTrigger();
    jobStore.storeTrigger(trigger, false);
    TriggerKey triggerKey = trigger.getKey();
    Trigger.TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);
    jobStore.pauseTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testResumeTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger = buildTrigger();
    jobStore.storeTrigger(trigger, false);
    TriggerKey triggerKey = trigger.getKey();
    Trigger.TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);
    jobStore.pauseTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.PAUSED);

    jobStore.resumeTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);
  }

  @Test
  public void testPauseTriggers()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger = buildAndStoreTrigger();
    OperableTrigger trigger1 = buildAndStoreTrigger();
    OperableTrigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    jobStore.storeTrigger(trigger2, false);
    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);

    Collection<String> pauseTriggers = jobStore
        .pauseTriggers(GroupMatcher.triggerGroupEquals(trigger.getKey().getGroup()));

    assertEquals(pauseTriggers.size(), 1);
    assertTrue(pauseTriggers.contains(trigger.getKey().getGroup()));

    assertEquals(jobStore.getPausedTriggerGroups().size(), 1);
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger.getKey().getGroup()));

    OperableTrigger trigger3 = buildAndStoreTrigger();

    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger2.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testResumeTriggers()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger = buildAndStoreTrigger();
    OperableTrigger trigger1 = buildAndStoreTrigger();
    OperableTrigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    jobStore.storeTrigger(trigger2, false);
    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);

    Collection<String> pauseTriggers = jobStore
        .pauseTriggers(GroupMatcher.triggerGroupEquals(trigger.getKey().getGroup()));

    assertEquals(pauseTriggers.size(), 1);
    assertTrue(pauseTriggers.contains(trigger.getKey().getGroup()));

    OperableTrigger trigger3 = buildAndStoreTrigger();

    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger2.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.PAUSED);

    Collection<String> resumeTriggers = jobStore.resumeTriggers(GroupMatcher.triggerGroupEquals(trigger.getKey()
        .getGroup()));

    assertEquals(resumeTriggers.size(), 1);
    assertTrue(resumeTriggers.contains(trigger.getKey().getGroup()));

    assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.NORMAL);

    OperableTrigger trigger4 = buildAndStoreTrigger();
    assertEquals(jobStore.getTriggerState(trigger4.getKey()), Trigger.TriggerState.NORMAL);
  }

  @Test
  public void testResumeTriggerWithPausedJobs()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    JobDetail job1 = buildJob("job", "group3");
    storeJob(job1);
    OperableTrigger trigger5 = buildTrigger(job1);
    jobStore.storeTrigger(trigger5, false);

    assertEquals(jobStore.getTriggerState(trigger5.getKey()), Trigger.TriggerState.NORMAL);
    jobStore.pauseJobs(GroupMatcher.jobGroupEquals("group3"));
    jobStore.resumeTriggers(GroupMatcher.triggerGroupEquals(trigger5.getKey().getGroup()));
    assertEquals(jobStore.getTriggerState(trigger5.getKey()), Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testPauseJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail jobDetail = buildAndStoreJob();
    OperableTrigger trigger = buildTrigger(jobDetail);
    jobStore.storeTrigger(trigger, false);
    Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);

    jobStore.pauseJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertEquals(triggerState, Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testResumeJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail jobDetail = buildAndStoreJob();
    OperableTrigger trigger = buildTrigger(jobDetail);
    jobStore.storeTrigger(trigger, false);

    Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);

    jobStore.pauseJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertEquals(triggerState, Trigger.TriggerState.PAUSED);

    jobStore.resumeJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertEquals(triggerState, Trigger.TriggerState.NORMAL);
  }

  @Test
  public void testPauseJobs()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail job1 = buildAndStoreJobWithTrigger();
    JobDetail job2 = buildAndStoreJobWithTrigger();

    JobDetail job3 = buildJob("job3", "newgroup");
    storeJob(job3);
    jobStore.storeTrigger(buildTrigger(job3), false);

    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(job1.getKey());
    triggersForJob.addAll(jobStore.getTriggersForJob(job2.getKey()));
    for (OperableTrigger trigger : triggersForJob) {
      assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    }

    Collection<String> pauseJobs = jobStore.pauseJobs(GroupMatcher.jobGroupEquals(job1.getKey().getGroup()));

    assertEquals(pauseJobs.size(), 1);
    assertTrue(pauseJobs.contains(job1.getKey().getGroup()));

    JobDetail job4 = buildAndStoreJobWithTrigger();

    triggersForJob = jobStore.getTriggersForJob(job1.getKey());
    triggersForJob.addAll(jobStore.getTriggersForJob(job2.getKey()));
    triggersForJob.addAll(jobStore.getTriggersForJob(job4.getKey()));
    for (OperableTrigger trigger : triggersForJob) {
      Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
      LOG.debug("State : [" + triggerState
          + "]Should be PAUSED for trigger : [" + trigger.getKey()
          + "] and job [" + trigger.getJobKey() + "]");
      assertEquals(triggerState, Trigger.TriggerState.PAUSED);
    }

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());

    for (OperableTrigger trigger : triggersForJob) {
      assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    }
  }

  @Test
  public void testResumeJobs()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail job1 = buildAndStoreJobWithTrigger();
    JobDetail job2 = buildAndStoreJob();
    OperableTrigger trigger2 = buildTrigger("trigger", "trigGroup2", job2);

    jobStore.storeTrigger(trigger2, false);

    JobDetail job3 = buildJob("job3", "newgroup");
    storeJob(job3);

    jobStore.storeTrigger(buildTrigger(job3), false);

    Collection<String> pauseJobs = jobStore.pauseJobs(GroupMatcher.anyJobGroup());

    assertEquals(pauseJobs.size(), 2);
    assertTrue(pauseJobs.contains(job1.getKey().getGroup()));
    assertTrue(pauseJobs.contains("newgroup"));

    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(job1.getKey());

    for (OperableTrigger trigger : triggersForJob) {
      Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
      assertEquals(triggerState, Trigger.TriggerState.PAUSED);
    }

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.PAUSED);
    }

    jobStore.pauseTriggers(GroupMatcher.triggerGroupEquals("trigGroup2"));

    jobStore.resumeJobs(GroupMatcher.jobGroupEquals(job1.getKey().getGroup()));

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.PAUSED);
    }

    triggersForJob = jobStore.getTriggersForJob(job1.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertEquals(jobStore.getTriggerState(trigger.getKey()), Trigger.TriggerState.NORMAL);
    }
  }

  @Test
  public void testPauseAll()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildAndStoreTrigger();
    OperableTrigger trigger2 = buildAndStoreTrigger();

    OperableTrigger trigger3 = buildTrigger("SpecialTriggerG2", "SpecialGroupG2");
    jobStore.storeTrigger(trigger3, false);

    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);

    jobStore.pauseAll();

    assertEquals(jobStore.getPausedTriggerGroups().size(), 2);
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger1.getKey().getGroup()));
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger2.getKey().getGroup()));
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger3.getKey().getGroup()));

    OperableTrigger trigger4 = buildAndStoreTrigger();

    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger2.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger4.getKey()), Trigger.TriggerState.PAUSED);
  }

  @Test
  public void testResumeAll()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger1 = buildAndStoreTrigger();
    OperableTrigger trigger2 = buildAndStoreTrigger();
    OperableTrigger trigger3 = buildTrigger("SpecialTriggerG2", "SpecialGroupG2");
    jobStore.storeTrigger(trigger3, false);

    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);

    jobStore.pauseAll();

    assertEquals(jobStore.getPausedTriggerGroups().size(), 2);
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger1.getKey().getGroup()));
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger2.getKey().getGroup()));
    assertTrue(jobStore.getPausedTriggerGroups().contains(trigger3.getKey().getGroup()));

    OperableTrigger trigger4 = buildAndStoreTrigger();

    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger2.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.PAUSED);
    assertEquals(jobStore.getTriggerState(trigger4.getKey()), Trigger.TriggerState.PAUSED);

    jobStore.resumeAll();
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger2.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger3.getKey()), Trigger.TriggerState.NORMAL);
    assertEquals(jobStore.getTriggerState(trigger4.getKey()), Trigger.TriggerState.NORMAL);
  }

  @Test
  public void testGetTriggerState()
    throws JobPersistenceException {

    Trigger.TriggerState triggerState = jobStore.getTriggerState(new TriggerKey("noname"));
    assertEquals(triggerState, Trigger.TriggerState.NONE);

  }

  @Test
  public void testTriggersFired()
    throws Exception {

    long baseFireTime = DateBuilder.newDate().build().getTime();

    JobDetail newJob = JobBuilder.newJob(NoOpJob.class).withIdentity("job1", "testTriggersFired").build();
    jobStore.storeJob(newJob, false);

    OperableTrigger trigger1 = buildAndComputeTrigger("triggerFired1",
        "triggerFiredGroup",
        newJob,
        baseFireTime + 100,
        baseFireTime + 100);

    jobStore.storeTrigger(trigger1, false);

    long firstFireTime = new Date(trigger1.getNextFireTime().getTime()).getTime();

    List<OperableTrigger> acquiredTriggers = jobStore.acquireNextTriggers(firstFireTime + 500, 1, 0L);
    assertEquals(acquiredTriggers.size(), 1);

    List<TriggerFiredResult> triggerFired = jobStore.triggersFired(acquiredTriggers);
    assertEquals(triggerFired.size(), 1);

    assertTrue(jobStore.checkExists(trigger1.getKey()));
    assertEquals(jobStore.getTriggerState(trigger1.getKey()), Trigger.TriggerState.NORMAL);
    jobStore.removeTrigger(trigger1.getKey());
  }

  private void assertAcquiredAndRelease(long baseFireTime, int numTriggersExpected)
    throws JobPersistenceException {
    List<OperableTrigger> operableTriggers = jobStore.acquireNextTriggers(baseFireTime + 600, 1, 0L);
    assertEquals(operableTriggers.size(), numTriggersExpected);

    operableTriggers.stream().forEach(t -> {
      t.triggered(null);
      jobStore.releaseAcquiredTrigger(t);
    });
  }

  protected HazelcastJobStore createJobStore(String name) {
    HazelcastJobStore jobStore = super.createJobStore(name);
    // shorter sleep interval to shorten test duration
    jobStore.setMisfireThreshold(1000);
    return jobStore;
  }

}
