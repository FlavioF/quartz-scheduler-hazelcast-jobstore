package org.ameausoone;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.fest.assertions.Assertions;
import static org.fest.assertions.Assertions.assertThat;
import org.quartz.Calendar;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.jdbcjobstore.JobStoreSupport;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.jobs.NoOpJob;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;

@Slf4j
public class TestHazelcastJobStore extends AbstractTestHazelcastJobStore {

  @Test()
  public void testAcquireNextTrigger() throws Exception {

    Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
    long baseFireTime = baseFireTimeDate.getTime();

    this.jobStore.storeJob(fJobDetail, false);

    OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 200000), new Date(baseFireTime + 200000), 2,
        2000);
    OperableTrigger trigger2 = new SimpleTriggerImpl("trigger2",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 50000), new Date(baseFireTime + 200000), 2,
        2000);
    OperableTrigger trigger3 = new SimpleTriggerImpl("trigger1",
        "triggerGroup2", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 100000), new Date(baseFireTime + 200000), 2,
        2000);

    trigger1.computeFirstFireTime(null);
    trigger2.computeFirstFireTime(null);
    trigger3.computeFirstFireTime(null);

    long firstFireTime = new Date(trigger1.getNextFireTime().getTime())
        .getTime();

    assertTrue(this.jobStore.acquireNextTriggers(10, 1, 0L).isEmpty());

    this.jobStore.storeTrigger(trigger2, false);
    assertEquals(trigger2.getKey(),
        this.jobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0)
            .getKey());

    this.jobStore.storeTrigger(trigger3, false);
    assertEquals(trigger3.getKey(),
        this.jobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0)
            .getKey());

    this.jobStore.storeTrigger(trigger1, false);
    assertEquals(trigger1.getKey(),
        this.jobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L).get(0)
            .getKey());

    assertTrue(this.jobStore.acquireNextTriggers(firstFireTime + 10000, 1, 0L)
        .isEmpty());

    // release trigger3
    this.jobStore.releaseAcquiredTrigger(trigger3);
    assertEquals(
        trigger3,
        this.jobStore.acquireNextTriggers(
            new Date(trigger1.getNextFireTime().getTime()).getTime() + 10000,
            1, 1L).get(0));

    jobStore.removeTrigger(trigger1.getKey());
    jobStore.removeTrigger(trigger2.getKey());
    jobStore.removeTrigger(trigger3.getKey());
  }

  @Test()
  public void testAcquireNextTriggerBatch() throws Exception {

    Date baseFireTimeDate = DateBuilder.evenMinuteDateAfterNow();
    long baseFireTime = baseFireTimeDate.getTime();

    this.jobStore.storeJob(fJobDetail, false);

    OperableTrigger trigger1 = new SimpleTriggerImpl("trigger1",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 200000), new Date(baseFireTime + 200005), 2,
        2000);
    OperableTrigger trigger2 = new SimpleTriggerImpl("trigger2",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 200100), new Date(baseFireTime + 200105), 2,
        2000);
    OperableTrigger trigger3 = new SimpleTriggerImpl("trigger3",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 200200), new Date(baseFireTime + 200205), 2,
        2000);
    OperableTrigger trigger4 = new SimpleTriggerImpl("trigger4",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 200300), new Date(baseFireTime + 200305), 2,
        2000);

    OperableTrigger trigger10 = new SimpleTriggerImpl("trigger10",
        "triggerGroup2", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(baseFireTime + 500000), new Date(baseFireTime + 700000), 2,
        2000);

    trigger1.computeFirstFireTime(null);
    trigger2.computeFirstFireTime(null);
    trigger3.computeFirstFireTime(null);
    trigger4.computeFirstFireTime(null);
    trigger10.computeFirstFireTime(null);
    this.jobStore.storeTrigger(trigger1, false);
    this.jobStore.storeTrigger(trigger2, false);
    this.jobStore.storeTrigger(trigger3, false);
    this.jobStore.storeTrigger(trigger4, false);
    this.jobStore.storeTrigger(trigger10, false);

    long firstFireTime = new Date(trigger1.getNextFireTime().getTime())
        .getTime();

    List<OperableTrigger> acquiredTriggers = this.jobStore.acquireNextTriggers(
        firstFireTime + 10000, 3, 1000L);
    assertEquals(3, acquiredTriggers.size());
    // assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
    // assertEquals(trigger2.getKey(), acquiredTriggers.get(1).getKey());
    // assertEquals(trigger3.getKey(), acquiredTriggers.get(2).getKey());
    // release all the triggers since there is no order ensurance
    this.jobStore.releaseAcquiredTrigger(trigger1);
    this.jobStore.releaseAcquiredTrigger(trigger2);
    this.jobStore.releaseAcquiredTrigger(trigger3);
    this.jobStore.releaseAcquiredTrigger(trigger4);

    acquiredTriggers = this.jobStore.acquireNextTriggers(firstFireTime + 10000,
        4, 1000L);
    assertEquals(4, acquiredTriggers.size());
    // assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
    // assertEquals(trigger2.getKey(), acquiredTriggers.get(1).getKey());
    // assertEquals(trigger3.getKey(), acquiredTriggers.get(2).getKey());
    // assertEquals(trigger4.getKey(), acquiredTriggers.get(3).getKey());
    this.jobStore.releaseAcquiredTrigger(trigger1);
    this.jobStore.releaseAcquiredTrigger(trigger2);
    this.jobStore.releaseAcquiredTrigger(trigger3);
    this.jobStore.releaseAcquiredTrigger(trigger4);

    acquiredTriggers = this.jobStore.acquireNextTriggers(firstFireTime + 10000,
        5, 1000L);
    assertEquals(4, acquiredTriggers.size());
    // assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
    // assertEquals(trigger2.getKey(), acquiredTriggers.get(1).getKey());
    // assertEquals(trigger3.getKey(), acquiredTriggers.get(2).getKey());
    // assertEquals(trigger4.getKey(), acquiredTriggers.get(3).getKey());
    this.jobStore.releaseAcquiredTrigger(trigger1);
    this.jobStore.releaseAcquiredTrigger(trigger2);

    assertEquals(1, this.jobStore.acquireNextTriggers(firstFireTime + 1, 5, 0L)
        .size());
    this.jobStore.releaseAcquiredTrigger(trigger1);

    assertEquals(2,
        this.jobStore.acquireNextTriggers(firstFireTime + 250, 5, 199L).size());
    this.jobStore.releaseAcquiredTrigger(trigger1);

    assertEquals(1,
        this.jobStore.acquireNextTriggers(firstFireTime + 150, 5, 50L).size());
    this.jobStore.releaseAcquiredTrigger(trigger1);

    jobStore.removeTrigger(trigger1.getKey());
    jobStore.removeTrigger(trigger2.getKey());
    jobStore.removeTrigger(trigger3.getKey());
    jobStore.removeTrigger(trigger4.getKey());
    jobStore.removeTrigger(trigger10.getKey());
  }

  @Test()
  public void testTriggerStates() throws Exception {
    this.jobStore.storeJob(fJobDetail, false);

    final OperableTrigger trigger = new SimpleTriggerImpl("trigger1",
        "triggerGroup1", this.fJobDetail.getName(), this.fJobDetail.getGroup(),
        new Date(System.currentTimeMillis() + 100000), new Date(
            System.currentTimeMillis() + 200000), 2, 2000);
    trigger.computeFirstFireTime(null);

    assertEquals(Trigger.TriggerState.NONE,
        this.jobStore.getTriggerState(trigger.getKey()));

    this.jobStore.storeTrigger(trigger, false);
    assertEquals(Trigger.TriggerState.NORMAL,
        this.jobStore.getTriggerState(trigger.getKey()));

    this.jobStore.pauseTrigger(trigger.getKey());
    assertEquals(Trigger.TriggerState.PAUSED,
        this.jobStore.getTriggerState(trigger.getKey()));

    this.jobStore.resumeTrigger(trigger.getKey());
    assertEquals(Trigger.TriggerState.NORMAL,
        this.jobStore.getTriggerState(trigger.getKey()));

    final OperableTrigger rTrigger1 = this.jobStore.acquireNextTriggers(
        new Date(trigger.getNextFireTime().getTime()).getTime() + 10000, 1, 1L)
        .get(0);
    assertNotNull(rTrigger1);
    this.jobStore.releaseAcquiredTrigger(rTrigger1);

    final OperableTrigger rTrigger2 = this.jobStore.acquireNextTriggers(
        new Date(rTrigger1.getNextFireTime().getTime()).getTime() + 10000, 1,
        1L).get(0);
    assertNotNull(rTrigger2);
    assertTrue(this.jobStore.acquireNextTriggers(
        new Date(rTrigger2.getNextFireTime().getTime()).getTime() + 10000, 1,
        1L).isEmpty());
  }

  @Test()
  public void testStoreTriggerReplacesTrigger() throws Exception {

    String jobName = "StoreTriggerReplacesTrigger";
    String jobGroup = "StoreTriggerReplacesTriggerGroup";
    JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, NoOpJob.class);
    jobStore.storeJob(detail, false);

    String trName = "StoreTriggerReplacesTrigger";
    String trGroup = "StoreTriggerReplacesTriggerGroup";
    OperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, new Date());
    tr.setJobKey(new JobKey(jobName, jobGroup));
    tr.setCalendarName(null);

    jobStore.storeTrigger(tr, false);
    assertEquals(tr, jobStore.retrieveTrigger(tr.getKey()));

    try {
      jobStore.storeTrigger(tr, false);
      fail("an attempt to store duplicate trigger succeeded");
    } catch (ObjectAlreadyExistsException oaee) {
      // expected
    }

    tr.setCalendarName("QQ");
    jobStore.storeTrigger(tr, true); // fails here
    assertEquals(tr, jobStore.retrieveTrigger(tr.getKey()));
    assertEquals("QQ", jobStore.retrieveTrigger(tr.getKey()).getCalendarName(),
        "StoreJob doesn't replace triggers");
  }

  @Test()
  public void testPauseJobGroupPausesNewJob() throws Exception {
    // Pausing job groups in JDBCJobStore is broken, see QTZ-208
    if (jobStore instanceof JobStoreSupport)
      return;

    final String jobName1 = "PauseJobGroupPausesNewJob";
    final String jobName2 = "PauseJobGroupPausesNewJob2";
    final String jobGroup = "PauseJobGroupPausesNewJobGroup";

    JobDetailImpl detail = new JobDetailImpl(jobName1, jobGroup, NoOpJob.class);
    detail.setDurability(true);
    jobStore.storeJob(detail, false);
    jobStore.pauseJobs(GroupMatcher.jobGroupEquals(jobGroup));

    detail = new JobDetailImpl(jobName2, jobGroup, NoOpJob.class);
    detail.setDurability(true);
    jobStore.storeJob(detail, false);

    String trName = "PauseJobGroupPausesNewJobTrigger";
    String trGroup = "PauseJobGroupPausesNewJobTriggerGroup";
    OperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, new Date());
    tr.setJobKey(new JobKey(jobName2, jobGroup));
    jobStore.storeTrigger(tr, false);
    assertEquals(Trigger.TriggerState.PAUSED,
        jobStore.getTriggerState(tr.getKey()));
  }

  @Test()
  public void testStoreAndRetrieveJobs() throws Exception {
    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testStoreAndRetrieveJobs");
    store.initialize(loadHelper, schedSignaler);

    // Store jobs.
    for (int i = 0; i < 10; i++) {
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i)
          .build();
      store.storeJob(job, false);
    }
    // Retrieve jobs.
    for (int i = 0; i < 10; i++) {
      JobKey jobKey = JobKey.jobKey("job" + i);
      JobDetail storedJob = store.retrieveJob(jobKey);
      Assert.assertEquals(jobKey, storedJob.getKey());
    }
  }

  @Test()
  public void testStoreAndRetriveTriggers() throws Exception {
    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testStoreAndRetriveTriggers");
    store.initialize(loadHelper, schedSignaler);

    // Store jobs and triggers.
    for (int i = 0; i < 10; i++) {
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i)
          .build();
      store.storeJob(job, true);
      SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule();
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity("job" + i)
          .withSchedule(schedule).forJob(job).build();
      store.storeTrigger((OperableTrigger) trigger, true);
    }
    // Retrieve job and trigger.
    for (int i = 0; i < 10; i++) {
      JobKey jobKey = JobKey.jobKey("job" + i);
      JobDetail storedJob = store.retrieveJob(jobKey);
      Assert.assertEquals(jobKey, storedJob.getKey());

      TriggerKey triggerKey = TriggerKey.triggerKey("job" + i);
      Trigger storedTrigger = store.retrieveTrigger(triggerKey);
      Assert.assertEquals(triggerKey, storedTrigger.getKey());
    }
  }

  @Test()
  public void testAcquireTriggers() throws Exception {
    SchedulerSignaler schedSignaler = new SampleSignaler();
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();

    JobStore store = createJobStore("testAcquireTriggers");
    store.initialize(loadHelper, schedSignaler);

    // Setup: Store jobs and triggers.
    long MIN = 60 * 1000L;
    Date startTime0 = new Date(System.currentTimeMillis() + MIN); // a min from
                                                                  // now.
    for (int i = 0; i < 10; i++) {
      Date startTime = new Date(startTime0.getTime() + i * MIN); // a min apart
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i)
          .build();
      SimpleScheduleBuilder schedule = SimpleScheduleBuilder
          .repeatMinutelyForever(2);
      OperableTrigger trigger = (OperableTrigger) TriggerBuilder.newTrigger()
          .withIdentity("job" + i).withSchedule(schedule).forJob(job)
          .startAt(startTime).build();

      // Manually trigger the first fire time computation that scheduler would
      // do. Otherwise
      // the store.acquireNextTriggers() will not work properly.
      Date fireTime = trigger.computeFirstFireTime(null);
      Assert.assertEquals(true, fireTime != null);

      store.storeJobAndTrigger(job, trigger);
    }

    // Test acquire one trigger at a time
    for (int i = 0; i < 10; i++) {
      long noLaterThan = (startTime0.getTime() + i * MIN);
      int maxCount = 1;
      long timeWindow = 0;
      List<OperableTrigger> triggers = store.acquireNextTriggers(noLaterThan,
          maxCount, timeWindow);
      Assert.assertEquals(1, triggers.size());
      Assert.assertEquals("job" + i, triggers.get(0).getKey().getName());

      // Let's remove the trigger now.
      store.removeJob(triggers.get(0).getJobKey());
    }
  }

  @Test()
  public void testAcquireTriggersInBatch() throws Exception {
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
      JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("job" + i)
          .build();
      SimpleScheduleBuilder schedule = SimpleScheduleBuilder
          .repeatMinutelyForever(2);
      OperableTrigger trigger = (OperableTrigger) TriggerBuilder.newTrigger()
          .withIdentity("job" + i).withSchedule(schedule).forJob(job)
          .startAt(startTime).build();

      // Manually trigger the first fire time computation that scheduler would
      // do. Otherwise
      // the store.acquireNextTriggers() will not work properly.
      Date fireTime = trigger.computeFirstFireTime(null);
      Assert.assertEquals(true, fireTime != null);

      store.storeJobAndTrigger(job, trigger);
    }

    // Test acquire batch of triggers at a time
    long noLaterThan = startTime0.getTime() + 10 * MIN;
    int maxCount = 7;
    // time window needs to be big to be able to pick up multiple triggers when
    // they are a minute apart
    long timeWindow = 8 * MIN;
    List<OperableTrigger> triggers = store.acquireNextTriggers(noLaterThan,
        maxCount, timeWindow);
    Assert.assertEquals(7, triggers.size());
  }

  @Test
  public void testStoreSimpleJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String jobName = "job20";
    storeJob(jobName);
    JobDetail retrieveJob = retrieveJob(jobName);
    assertThat(retrieveJob).isNotNull();
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void storeTwiceSameJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String jobName = "job21";
    storeJob(jobName);
    storeJob(jobName);
  }

  @Test
  public void testRemoveJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String jobName = "job22";
    JobDetail jobDetail = buildJob(jobName);
    storeJob(jobDetail);
    JobDetail retrieveJob = retrieveJob(jobName);
    assertThat(retrieveJob).isNotNull();
    Trigger trigger = buildTrigger(jobDetail);
    storeTrigger(trigger);

    assertThat(retrieveTrigger(trigger.getKey())).isNotNull();

    boolean removeJob = this.jobStore.removeJob(jobDetail.getKey());
    assertThat(removeJob).isTrue();
    retrieveJob = retrieveJob(jobName);
    assertThat(retrieveJob).isNull();

    assertThat(retrieveTrigger(trigger.getKey())).isNull();

    removeJob = this.jobStore.removeJob(jobDetail.getKey());
    assertThat(removeJob).isFalse();
  }

  @Test
  public void testRemoveJobs() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String jobName = "job24";
    JobDetail jobDetail = buildJob(jobName);
    String jobName2 = "job25";
    JobDetail jobDetailImpl2 = buildJob(jobName2);
    this.jobStore.storeJob(jobDetail, false);
    this.jobStore.storeJob(jobDetailImpl2, false);
    JobDetail retrieveJob = retrieveJob(jobName);
    assertThat(retrieveJob).isNotNull();
    List<JobKey> jobKeyList = Lists.newArrayList(jobDetail.getKey(),
        jobDetailImpl2.getKey());
    boolean removeJob = this.jobStore.removeJobs(jobKeyList);
    assertThat(removeJob).isTrue();
    retrieveJob = retrieveJob(jobName);
    assertThat(retrieveJob).isNull();
    retrieveJob = retrieveJob(jobName2);
    assertThat(retrieveJob).isNull();
    removeJob = this.jobStore.removeJob(jobDetail.getKey());
    assertThat(removeJob).isFalse();
    removeJob = this.jobStore.removeJob(jobDetailImpl2.getKey());
    assertThat(removeJob).isFalse();
  }

  @Test
  public void testCheckExistsJob() throws JobPersistenceException {
    JobDetail jobDetailImpl = buildJob("job23");
    this.jobStore.storeJob(jobDetailImpl, false);
    boolean checkExists = jobStore.checkExists(jobDetailImpl.getKey());
    Assertions.assertThat(checkExists).isTrue();
  }

  @Test(expectedExceptions = { JobPersistenceException.class })
  public void testStoreTriggerWithoutJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger("trigger", "group");
    storeTrigger(trigger1);
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
  }

  @Test
  public void testStoreTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger();
    storeTrigger(trigger1);
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void testStoreTriggerThrowsAlreadyExists()
      throws ObjectAlreadyExistsException, JobPersistenceException {
    Trigger trigger1 = buildTrigger();
    storeTrigger(trigger1);
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
    storeTrigger(trigger1);
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
  }

  @Test
  public void testStoreTriggerTwice() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger();

    storeTrigger(trigger1);
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
    jobStore.storeTrigger((OperableTrigger) trigger1, true);
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
  }

  @Test
  public void testRemoveTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail storeJob = storeJob(buildJob("job"));
    Trigger trigger1 = buildTrigger(storeJob);
    TriggerKey triggerKey = trigger1.getKey();
    storeTrigger(trigger1);
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
    boolean removeTrigger = jobStore.removeTrigger(triggerKey);
    assertThat(removeTrigger).isTrue();
    retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNull();
    removeTrigger = jobStore.removeTrigger(triggerKey);
    assertThat(removeTrigger).isFalse();

    TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.NONE);
  }

  @Test
  public void testRemoveTriggers() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger();
    Trigger trigger2 = buildTrigger();

    storeTrigger(trigger1);
    storeTrigger(trigger2);

    List<TriggerKey> triggerKeys = Lists.newArrayList(trigger1.getKey(),
        trigger2.getKey());
    boolean removeTriggers = jobStore.removeTriggers(triggerKeys);
    assertThat(removeTriggers).isTrue();
  }

  @Test
  public void testTriggerCheckExists() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger();
    TriggerKey triggerKey = trigger1.getKey();

    boolean checkExists = jobStore.checkExists(triggerKey);
    assertThat(checkExists).isFalse();

    storeTrigger(trigger1);

    checkExists = jobStore.checkExists(triggerKey);
    assertThat(checkExists).isTrue();
  }

  @Test
  public void testReplaceTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger1 = buildTrigger();

    storeTrigger(trigger1);

    Trigger newTrigger = buildTrigger();

    TriggerKey triggerKey = trigger1.getKey();
    boolean replaceTrigger = jobStore.replaceTrigger(triggerKey,
        (OperableTrigger) newTrigger);
    assertThat(replaceTrigger).isTrue();
    Trigger retrieveTrigger = jobStore.retrieveTrigger(triggerKey);
    assertThat(retrieveTrigger).isEqualTo(newTrigger);
  }

  @Test
  public void testStoreJobAndTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail jobDetailImpl = buildJob("job30");

    Trigger trigger1 = buildTrigger();
    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
    JobDetail retrieveJob = jobStore.retrieveJob(jobDetailImpl.getKey());
    assertThat(retrieveJob).isNotNull();
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void testStoreJobAndTriggerThrowJobAlreadyExists()
      throws ObjectAlreadyExistsException, JobPersistenceException {
    JobDetail jobDetailImpl = buildJob("job31");
    Trigger trigger1 = buildTrigger();
    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
    JobDetail retrieveJob = jobStore.retrieveJob(jobDetailImpl.getKey());
    assertThat(retrieveJob).isNotNull();
    Trigger retrieveTrigger = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger).isNotNull();

    jobStore.storeJobAndTrigger(jobDetailImpl, (OperableTrigger) trigger1);
  }

  @Test
  public void storeCalendar() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String calName = "calendar";
    storeCalendar(calName);
    Calendar retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertThat(retrieveCalendar).isNotNull();
  }

  @Test
  public void testRemoveCalendar() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    String calName = "calendar1";
    storeCalendar(calName);

    Calendar retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertThat(retrieveCalendar).isNotNull();
    boolean calendarExisted = jobStore.removeCalendar(calName);
    assertThat(calendarExisted).isTrue();
    retrieveCalendar = jobStore.retrieveCalendar(calName);
    assertThat(retrieveCalendar).isNull();
    calendarExisted = jobStore.removeCalendar(calName);
    assertThat(calendarExisted).isFalse();

  }

  @Test
  public void testClearAllSchedulingData() throws JobPersistenceException {
    assertThat(jobStore.getNumberOfJobs()).isEqualTo(0);

    assertThat(jobStore.getNumberOfTriggers()).isEqualTo(0);

    assertThat(jobStore.getNumberOfCalendars()).isEqualTo(0);

    final String jobName = "job40";
    final JobDetail storeJob = storeJob(jobName);
    assertThat(jobStore.getNumberOfJobs()).isEqualTo(1);

    jobStore.storeTrigger((OperableTrigger) buildTrigger(storeJob), false);
    assertThat(jobStore.getNumberOfTriggers()).isEqualTo(1);

    jobStore.storeCalendar("calendar", new BaseCalendar(), false, false);
    assertThat(jobStore.getNumberOfCalendars()).isEqualTo(1);

    jobStore.clearAllSchedulingData();
    assertThat(jobStore.getNumberOfJobs()).isEqualTo(0);

    assertThat(jobStore.getNumberOfTriggers()).isEqualTo(0);

    assertThat(jobStore.getNumberOfCalendars()).isEqualTo(0);
  }

  @Test
  public void testStoreSameJobNameWithDifferentGroup()
      throws ObjectAlreadyExistsException, JobPersistenceException {
    storeJob(buildJob("job40", "group1"));
    storeJob(buildJob("job40", "group2"));
    // Assert there is no exception throws
  }

  @Test
  public void testGetJobGroupNames() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail buildJob = buildJob("job40", "group1");
    storeJob(buildJob);
    storeJob(buildJob("job41", "group2"));
    List<String> jobGroupNames = jobStore.getJobGroupNames();
    assertThat(jobGroupNames).containsOnly("group1", "group2");

    this.jobStore.removeJob(buildJob.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertThat(jobGroupNames).containsOnly("group2");
  }

  @Test
  public void testJobKeyByGroup() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail job1group1 = buildJob("job1", "group1");
    storeJob(job1group1);
    JobDetail job1group2 = buildJob("job1", "group2");
    storeJob(job1group2);
    storeJob(buildJob("job2", "group2"));
    List<String> jobGroupNames = jobStore.getJobGroupNames();
    assertThat(jobGroupNames).containsOnly("group1", "group2");

    this.jobStore.removeJob(job1group1.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertThat(jobGroupNames).containsOnly("group2");

    this.jobStore.removeJob(job1group2.getKey());

    jobGroupNames = jobStore.getJobGroupNames();
    assertThat(jobGroupNames).containsOnly("group2");
  }

  @Test
  public void testGetTriggerGroupNames() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail storeJob = storeJob(buildJob("job"));
    storeTrigger(buildTrigger("trigger1", "group1", storeJob));
    storeTrigger(buildTrigger("trigger2", "group2", storeJob));
    List<String> triggerGroupNames = jobStore.getTriggerGroupNames();
    assertThat(triggerGroupNames).containsOnly("group1", "group2");
  }

  @Test
  public void testCalendarNames() throws JobPersistenceException {
    storeCalendar("cal1");
    storeCalendar("cal2");
    List<String> calendarNames = jobStore.getCalendarNames();
    assertThat(calendarNames).containsOnly("cal1", "cal2");
  }

  @Test
  public void storeJobAndTriggers() throws ObjectAlreadyExistsException,
      JobPersistenceException {

    final Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps
        .newHashMap();

    final JobDetail job1 = buildJob();
    final Trigger trigger1 = buildTrigger(job1);
    final Set<Trigger> set1 = Sets.newHashSet();
    set1.add(trigger1);
    triggersAndJobs.put(job1, set1);

    final JobDetail job2 = buildJob();
    final Trigger trigger2 = buildTrigger(job2);
    final Set<Trigger> set2 = Sets.newHashSet();
    set2.add(trigger2);
    triggersAndJobs.put(job2, set2);

    jobStore.storeJobsAndTriggers(triggersAndJobs, false);

    final JobDetail retrieveJob1 = retrieveJob(job1.getKey().getName());
    assertThat(retrieveJob1).isNotNull();

    final JobDetail retrieveJob2 = retrieveJob(job2.getKey().getName());
    assertThat(retrieveJob2).isNotNull();

    final Trigger retrieveTrigger1 = retrieveTrigger(trigger1.getKey());
    assertThat(retrieveTrigger1).isNotNull();

    final Trigger retrieveTrigger2 = retrieveTrigger(trigger2.getKey());
    assertThat(retrieveTrigger2).isNotNull();
  }

  @Test(expectedExceptions = { ObjectAlreadyExistsException.class })
  public void storeJobAndTriggersThrowException()
      throws ObjectAlreadyExistsException, JobPersistenceException {

    Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = Maps.newHashMap();
    JobDetail job1 = buildJob();
    storeJob(job1);
    Trigger trigger1 = buildTrigger(job1);
    Set<Trigger> set1 = Sets.newHashSet();
    set1.add(trigger1);
    triggersAndJobs.put(job1, set1);
    jobStore.storeJobsAndTriggers(triggersAndJobs, false);
  }

  @Test
  public void testGetTriggersForJob() throws JobPersistenceException {
    JobDetail jobDetail = buildAndStoreJob();
    Trigger trigger1 = buildTrigger(jobDetail);
    Trigger trigger2 = buildTrigger(jobDetail);
    storeTrigger(trigger1);
    storeTrigger(trigger2);

    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(jobDetail
        .getKey());
    assertThat(triggersForJob).containsOnly(trigger1, trigger2);
  }

  @Test
  public void testPauseTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildTrigger();
    storeTrigger(trigger);
    TriggerKey triggerKey = trigger.getKey();
    TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
    jobStore.pauseTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.PAUSED);
  }

  @Test
  public void testResumeTrigger() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildTrigger();
    storeTrigger(trigger);
    TriggerKey triggerKey = trigger.getKey();
    TriggerState triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
    jobStore.pauseTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.PAUSED);

    jobStore.resumeTrigger(triggerKey);
    triggerState = jobStore.getTriggerState(triggerKey);
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
  }

  @Test
  public void testPauseTriggers() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildAndStoreTrigger();
    Trigger trigger1 = buildAndStoreTrigger();
    Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    storeTrigger(trigger2);
    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger1.getKey())).isEqualTo(
        TriggerState.NORMAL);

    Collection<String> pauseTriggers = jobStore.pauseTriggers(GroupMatcher
        .triggerGroupEquals(trigger.getKey().getGroup()));

    assertThat(pauseTriggers).containsOnly(trigger.getKey().getGroup());
    assertThat(jobStore.getPausedTriggerGroups()).containsOnly(
        trigger.getKey().getGroup());

    Trigger trigger3 = buildAndStoreTrigger();

    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger1.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger2.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.PAUSED);
  }

  @Test
  public void testResumeTriggers() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildAndStoreTrigger();
    Trigger trigger1 = buildAndStoreTrigger();
    Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    storeTrigger(trigger2);
    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger1.getKey())).isEqualTo(
        TriggerState.NORMAL);

    Collection<String> pauseTriggers = jobStore.pauseTriggers(GroupMatcher
        .triggerGroupEquals(trigger.getKey().getGroup()));

    assertThat(pauseTriggers).containsOnly(trigger.getKey().getGroup());

    Trigger trigger3 = buildAndStoreTrigger();

    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger1.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger2.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.PAUSED);

    Collection<String> resumeTriggers = jobStore.resumeTriggers(GroupMatcher
        .triggerGroupEquals(trigger.getKey().getGroup()));

    assertThat(resumeTriggers).containsOnly(trigger.getKey().getGroup());

    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger1.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.NORMAL);

    Trigger trigger4 = buildAndStoreTrigger();
    assertThat(jobStore.getTriggerState(trigger4.getKey())).isEqualTo(
        TriggerState.NORMAL);
  }

  @Test
  public void testResumeTriggerWithPausedJobs()
      throws ObjectAlreadyExistsException, JobPersistenceException {
    JobDetail job1 = buildJob("job", "group3");
    storeJob(job1);
    Trigger trigger5 = buildTrigger(job1);
    storeTrigger(trigger5);

    assertThat(jobStore.getTriggerState(trigger5.getKey())).isEqualTo(
        TriggerState.NORMAL);
    jobStore.pauseJobs(GroupMatcher.jobGroupEquals("group3"));
    jobStore.resumeTriggers(GroupMatcher.triggerGroupEquals(trigger5.getKey()
        .getGroup()));
    assertThat(jobStore.getTriggerState(trigger5.getKey())).isEqualTo(
        TriggerState.PAUSED);
  }

  @Test
  public void testPauseJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail jobDetail = buildAndStoreJob();
    Trigger trigger = buildTrigger(jobDetail);
    storeTrigger(trigger);
    TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);

    jobStore.pauseJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertThat(triggerState).isEqualTo(TriggerState.PAUSED);
  }

  @Test
  public void testResumeJob() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail jobDetail = buildAndStoreJob();
    Trigger trigger = buildTrigger(jobDetail);
    storeTrigger(trigger);
    TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);

    jobStore.pauseJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertThat(triggerState).isEqualTo(TriggerState.PAUSED);

    jobStore.resumeJob(jobDetail.getKey());

    triggerState = jobStore.getTriggerState(trigger.getKey());
    assertThat(triggerState).isEqualTo(TriggerState.NORMAL);
  }

  @Test
  public void testPauseJobs() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail job1 = buildAndStoreJobWithTrigger();
    JobDetail job2 = buildAndStoreJobWithTrigger();

    JobDetail job3 = buildJob("job3", "newgroup");
    storeJob(job3);
    storeTrigger(buildTrigger(job3));
    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(job1
        .getKey());
    triggersForJob.addAll(jobStore.getTriggersForJob(job2.getKey()));
    for (OperableTrigger trigger : triggersForJob) {
      assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
          TriggerState.NORMAL);
    }

    Collection<String> pauseJobs = jobStore.pauseJobs(GroupMatcher
        .jobGroupEquals(job1.getKey().getGroup()));

    assertThat(pauseJobs).containsOnly(job1.getKey().getGroup());

    JobDetail job4 = buildAndStoreJobWithTrigger();

    triggersForJob = jobStore.getTriggersForJob(job1.getKey());
    triggersForJob.addAll(jobStore.getTriggersForJob(job2.getKey()));
    triggersForJob.addAll(jobStore.getTriggersForJob(job4.getKey()));
    for (OperableTrigger trigger : triggersForJob) {
      TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
      log.debug("State : [" + triggerState
          + "]Should be PAUSED for trigger : [" + trigger.getKey()
          + "] and job [" + trigger.getJobKey() + "]");
      assertThat(triggerState).isEqualTo(TriggerState.PAUSED)
          .overridingErrorMessage(
              "Should be PAUSED for trigger : [" + trigger.getKey()
                  + "] and job [" + trigger.getJobKey() + "]");
    }

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());

    for (OperableTrigger trigger : triggersForJob) {
      assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
          TriggerState.NORMAL);
    }
  }

  @Test
  public void testResumeJobs() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    JobDetail job1 = buildAndStoreJobWithTrigger();
    JobDetail job2 = buildAndStoreJob();
    Trigger trigger2 = buildTrigger("trigger", "trigGroup2", job2);
    storeTrigger(trigger2);

    JobDetail job3 = buildJob("job3", "newgroup");
    storeJob(job3);
    storeTrigger(buildTrigger(job3));

    Collection<String> pauseJobs = jobStore.pauseJobs(GroupMatcher
        .anyJobGroup());

    assertThat(pauseJobs).containsOnly(job1.getKey().getGroup(), "newgroup");

    List<OperableTrigger> triggersForJob = jobStore.getTriggersForJob(job1
        .getKey());

    for (OperableTrigger trigger : triggersForJob) {
      TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
      assertThat(triggerState).isEqualTo(TriggerState.PAUSED)
          .overridingErrorMessage(
              "Should be PAUSED for trigger : [" + trigger.getKey()
                  + "] and job [" + trigger.getJobKey() + "]");
    }

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
          TriggerState.PAUSED);
    }

    jobStore.pauseTriggers(GroupMatcher.triggerGroupEquals("trigGroup2"));

    jobStore.resumeJobs(GroupMatcher.jobGroupEquals(job1.getKey().getGroup()));

    triggersForJob = jobStore.getTriggersForJob(job3.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
          TriggerState.PAUSED);
    }

    triggersForJob = jobStore.getTriggersForJob(job1.getKey());
    for (OperableTrigger trigger : triggersForJob) {
      assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
          TriggerState.NORMAL);
    }
  }

  @Test
  public void testPauseAll() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildAndStoreTrigger();
    Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    storeTrigger(trigger2);
    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);

    jobStore.pauseAll();

    assertThat(jobStore.getPausedTriggerGroups()).containsOnly(
        trigger.getKey().getGroup(), trigger2.getKey().getGroup());

    Trigger trigger3 = buildAndStoreTrigger();

    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger2.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.PAUSED);
  }

  @Test
  public void testResumeAll() throws ObjectAlreadyExistsException,
      JobPersistenceException {
    Trigger trigger = buildAndStoreTrigger();
    Trigger trigger2 = buildTrigger("trigger2", "group2", buildAndStoreJob());
    storeTrigger(trigger2);
    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);

    jobStore.pauseAll();

    assertThat(jobStore.getPausedTriggerGroups()).containsOnly(
        trigger.getKey().getGroup(), trigger2.getKey().getGroup());

    Trigger trigger3 = buildAndStoreTrigger();

    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger2.getKey())).isEqualTo(
        TriggerState.PAUSED);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.PAUSED);

    jobStore.resumeAll();
    assertThat(jobStore.getTriggerState(trigger.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger2.getKey())).isEqualTo(
        TriggerState.NORMAL);
    assertThat(jobStore.getTriggerState(trigger3.getKey())).isEqualTo(
        TriggerState.NORMAL);
  }

  @Test
  public void testGetTriggerState() throws JobPersistenceException {
    TriggerState triggerState = jobStore.getTriggerState(new TriggerKey(
        "noname"));
    assertThat(triggerState).isEqualTo(TriggerState.NONE);

  }
}
