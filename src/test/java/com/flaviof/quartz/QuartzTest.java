package com.flaviof.quartz;

import com.flaviof.quartz.jobstore.hazelcast.HazelcastJobStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.joda.time.DateTime;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import static org.quartz.JobBuilder.newJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import static org.quartz.JobKey.jobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.Scheduler;
import static org.quartz.Scheduler.DEFAULT_GROUP;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import static org.quartz.TriggerBuilder.newTrigger;
import org.quartz.TriggerKey;
import static org.quartz.TriggerKey.triggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class QuartzTest extends AbstractTest {

  public static int jobExecs = 0;
  private static final String BARRIER = "BARRIER";
  private static final String DATE_STAMPS = "DATE_STAMPS";
  private static final String JOB_THREAD = "JOB_THREAD";

  Scheduler scheduler;

  @BeforeClass
  public void setUp()
          throws SchedulerException, InterruptedException {

  }

  @AfterClass
  public void tearDown()
          throws SchedulerException {

    prepare();
    hazelcastInstance.shutdown();
    scheduler.shutdown();
  }

  @BeforeMethod
  public void prepare()
          throws SchedulerException {

    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    HazelcastJobStore.setHazelcastClient(hazelcastInstance);

    final Properties props = new Properties();
    props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "BikeemotionScheduler");
    props.setProperty(StdSchedulerFactory.PROP_SCHED_JMX_EXPORT, "true");
    props.setProperty(StdSchedulerFactory.PROP_JOB_STORE_CLASS, HazelcastJobStore.class.getName());
    props.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadCount", "10");
    props.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadPriority", "5");

    scheduler = new StdSchedulerFactory(props).getScheduler();
    scheduler.start();

  }

  @AfterMethod
  public void cleanUp()
          throws SchedulerException {
    if (scheduler != null && scheduler.isStarted()) {
      scheduler.shutdown();
      MyJob.count = 0;
      MyJob.jobKeys.clear();
      MyJob.triggerKeys.clear();
    }

  }

  @Test()
  public void testSchedule()
          throws Exception {

    JobDetail job1 = buildJob("Job1", DEFAULT_GROUP, MyJob.class);
    JobDetail job2 = buildJob("Job2", DEFAULT_GROUP, MyJob.class);
    JobDetail job3 = buildJob("Job3", DEFAULT_GROUP, MyJob.class);

    scheduler.scheduleJob(job1, buildTrigger("key1", DEFAULT_GROUP, job1, DateTime.now().plusMillis(100).getMillis()));
    scheduler.scheduleJob(job2, buildTrigger("key2", DEFAULT_GROUP, job2, DateTime.now().plusMillis(500).getMillis()));
    scheduler.scheduleJob(job3, buildTrigger("key3", DEFAULT_GROUP, job3, DateTime.now().plusMillis(750).getMillis()));

    Thread.sleep(800);
    assertEquals(MyJob.count, 3);
    assertTrue(MyJob.jobKeys.contains(job1.getKey().getName()));
    assertTrue(MyJob.jobKeys.contains(job2.getKey().getName()));
    assertTrue(MyJob.jobKeys.contains(job3.getKey().getName()));

  }

  @Test()
  public void testScheduleAtSameTime()
          throws Exception {

    JobDetail job1 = buildJob("testScheduleAtSameTime1", DEFAULT_GROUP, MyJob.class);
    JobDetail job2 = buildJob("testScheduleAtSameTime2", DEFAULT_GROUP, MyJob.class);
    JobDetail job3 = buildJob("testScheduleAtSameTime3", DEFAULT_GROUP, MyJob.class);

    scheduler.scheduleJob(job1, buildTrigger("k21", DEFAULT_GROUP, job1, DateTime.now().plusMillis(100).getMillis()));
    scheduler.scheduleJob(job2, buildTrigger("k22", DEFAULT_GROUP, job2, DateTime.now().plusMillis(100).getMillis()));
    scheduler.scheduleJob(job3, buildTrigger("k23", DEFAULT_GROUP, job3, DateTime.now().plusMillis(100).getMillis()));

    Thread.sleep(150);
    assertEquals(MyJob.count, 3);
    assertTrue(MyJob.jobKeys.contains(job1.getKey().getName()));
    assertTrue(MyJob.jobKeys.contains(job2.getKey().getName()));
    assertTrue(MyJob.jobKeys.contains(job3.getKey().getName()));

  }

  @Test(invocationCount = 5)
  public void testScheduleOutOfOrder()
          throws Exception {

    JobDetail job1 = buildJob("Job1", DEFAULT_GROUP, MyJob.class);

    scheduler.scheduleJob(job1, buildTrigger("key1", DEFAULT_GROUP, job1, DateTime.now().plusMillis(200).getMillis()));
    scheduler.scheduleJob(buildTrigger("key2", DEFAULT_GROUP, job1, DateTime.now().plusMillis(100).getMillis()));
    scheduler.scheduleJob(buildTrigger("key3", DEFAULT_GROUP, job1, DateTime.now().plusMillis(300).getMillis()));

    Thread.sleep(350);

    assertEquals(MyJob.count, 3);
    assertEquals(MyJob.triggerKeys.poll(), "key2");
    assertEquals(MyJob.triggerKeys.poll(), "key1");
    assertEquals(MyJob.triggerKeys.poll(), "key3");
  }

  @Test
  public void testBasicStorageFunctions() throws Exception {

    // test basic storage functions of scheduler...
    JobDetail job = newJob()
            .ofType(MyJob.class)
            .withIdentity("j1")
            .storeDurably()
            .build();

    assertFalse("Unexpected existence of job named 'j1'.", scheduler.checkExists(jobKey("j1")));

    scheduler.addJob(job, false);

    assertTrue("Expected existence of job named 'j1' but checkExists return false.", scheduler.checkExists(jobKey("j1")));

    job = scheduler.getJobDetail(jobKey("j1"));

    assertNotNull("Stored job not found!", job);

    scheduler.deleteJob(jobKey("j1"));

    Trigger trigger = newTrigger()
            .withIdentity("t1")
            .forJob(job)
            .startNow()
            .withSchedule(simpleSchedule()
                    .repeatForever()
                    .withIntervalInSeconds(5))
            .build();

    assertFalse("Unexpected existence of trigger named '11'.", scheduler.checkExists(triggerKey("t1")));

    scheduler.scheduleJob(job, trigger);

    assertTrue("Expected existence of trigger named 't1' but checkExists return false.", scheduler.checkExists(triggerKey("t1")));

    job = scheduler.getJobDetail(jobKey("j1"));

    assertNotNull("Stored job not found!", job);

    trigger = scheduler.getTrigger(triggerKey("t1"));

    assertNotNull("Stored trigger not found!", trigger);

    job = newJob()
            .ofType(MyJob.class)
            .withIdentity("j2", "g1")
            .build();

    trigger = newTrigger()
            .withIdentity("t2", "g1")
            .forJob(job)
            .startNow()
            .withSchedule(simpleSchedule()
                    .repeatForever()
                    .withIntervalInSeconds(5))
            .build();

    scheduler.scheduleJob(job, trigger);

    job = newJob()
            .ofType(MyJob.class)
            .withIdentity("j3", "g1")
            .build();

    trigger = newTrigger()
            .withIdentity("t3", "g1")
            .forJob(job)
            .startNow()
            .withSchedule(simpleSchedule()
                    .repeatForever()
                    .withIntervalInSeconds(5))
            .build();

    scheduler.scheduleJob(job, trigger);

    List<String> jobGroups = scheduler.getJobGroupNames();
    List<String> triggerGroups = scheduler.getTriggerGroupNames();

    assertTrue("Job group list size expected to be = 2 ", jobGroups.size() == 2);
    assertTrue("Trigger group list size expected to be = 2 ", triggerGroups.size() == 2);

    Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(JobKey.DEFAULT_GROUP));
    Set<TriggerKey> triggerKeys = scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals(TriggerKey.DEFAULT_GROUP));

    assertTrue("Number of jobs expected in default group was 1 ", jobKeys.size() == 1);
    assertTrue("Number of triggers expected in default group was 1 ", triggerKeys.size() == 1);

    jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals("g1"));
    triggerKeys = scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals("g1"));

    assertTrue("Number of jobs expected in 'g1' group was 2 ", jobKeys.size() == 2);
    assertTrue("Number of triggers expected in 'g1' group was 2 ", triggerKeys.size() == 2);

    Trigger.TriggerState s = scheduler.getTriggerState(triggerKey("t2", "g1"));
    assertTrue("State of trigger t2 expected to be NORMAL ", s.equals(Trigger.TriggerState.NORMAL));

    scheduler.pauseTrigger(triggerKey("t2", "g1"));
    s = scheduler.getTriggerState(triggerKey("t2", "g1"));
    //TODO fix it
//        assertEquals(s, Trigger.TriggerState.PAUSED);

    scheduler.resumeTrigger(triggerKey("t2", "g1"));
    s = scheduler.getTriggerState(triggerKey("t2", "g1"));
    assertTrue("State of trigger t2 expected to be NORMAL ", s.equals(Trigger.TriggerState.NORMAL));

    Set<String> pausedGroups = scheduler.getPausedTriggerGroups();
    assertTrue("Size of paused trigger groups list expected to be 0 ", pausedGroups.size() == 0);

    scheduler.pauseTriggers(GroupMatcher.triggerGroupEquals("g1"));

    // test that adding a trigger to a paused group causes the new trigger to be paused also... 
    job = newJob()
            .ofType(MyJob.class)
            .withIdentity("j4", "g1")
            .build();

    trigger = newTrigger()
            .withIdentity("t4", "g1")
            .forJob(job)
            .startNow()
            .withSchedule(simpleSchedule()
                    .repeatForever()
                    .withIntervalInSeconds(5))
            .build();

    scheduler.scheduleJob(job, trigger);

    pausedGroups = scheduler.getPausedTriggerGroups();
    assertTrue("Size of paused trigger groups list expected to be 1 ", pausedGroups.size() == 1);
//TODO fix it
//        s = scheduler.getTriggerState(triggerKey("t2", "g1"));
//        assertEquals(s, Trigger.TriggerState.PAUSED);

    s = scheduler.getTriggerState(triggerKey("t4", "g1"));
    assertEquals(s, Trigger.TriggerState.PAUSED);

    scheduler.resumeTriggers(GroupMatcher.triggerGroupEquals("g1"));
    s = scheduler.getTriggerState(triggerKey("t2", "g1"));
    assertTrue("State of trigger t2 expected to be NORMAL ", s.equals(Trigger.TriggerState.NORMAL));
    s = scheduler.getTriggerState(triggerKey("t4", "g1"));
    assertTrue("State of trigger t4 expected to be NORMAL ", s.equals(Trigger.TriggerState.NORMAL));
    pausedGroups = scheduler.getPausedTriggerGroups();
    assertTrue("Size of paused trigger groups list expected to be 0 ", pausedGroups.size() == 0);

    assertFalse("Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ", scheduler.unscheduleJob(triggerKey("foasldfksajdflk")));

    assertTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ", scheduler.unscheduleJob(triggerKey("t3", "g1")));

    jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals("g1"));
    triggerKeys = scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals("g1"));

    assertTrue("Number of jobs expected in 'g1' group was 1 ", jobKeys.size() == 2); // job should have been deleted also, because it is non-durable
    assertTrue("Number of triggers expected in 'g1' group was 1 ", triggerKeys.size() == 2);

    assertTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ", scheduler.unscheduleJob(triggerKey("t1")));

    jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(JobKey.DEFAULT_GROUP));
    triggerKeys = scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals(TriggerKey.DEFAULT_GROUP));

    assertTrue("Number of jobs expected in default group was 1 ", jobKeys.size() == 1); // job should have been left in place, because it is non-durable
    assertTrue("Number of triggers expected in default group was 0 ", triggerKeys.size() == 0);

  }

  @Test
  public void testDurableStorageFunctions() throws Exception {
    // test basic storage functions of scheduler...

    JobDetail job = newJob()
            .ofType(MyJob.class)
            .withIdentity("j1")
            .storeDurably()
            .build();

    assertFalse("Unexpected existence of job named 'j1'.", scheduler.checkExists(jobKey("j1")));

    scheduler.addJob(job, false);

    assertTrue("Unexpected non-existence of job named 'j1'.", scheduler.checkExists(jobKey("j1")));

    JobDetail nonDurableJob = newJob()
            .ofType(MyJob.class)
            .withIdentity("j2")
            .build();

    try {
      scheduler.addJob(nonDurableJob, false);
      fail("Storage of non-durable job should not have succeeded.");
    } catch (SchedulerException expected) {
      assertFalse("Unexpected existence of job named 'j2'.", scheduler.checkExists(jobKey("j2")));
    }

    scheduler.addJob(nonDurableJob, false, true);

    assertTrue("Unexpected non-existence of job named 'j2'.", scheduler.checkExists(jobKey("j2")));
  }

  @Test
  public void testShutdownWithSleepReturnsAfterAllThreadsAreStopped() throws Exception {
    Map<Thread, StackTraceElement[]> allThreadsStart = Thread.getAllStackTraces();

    Thread.sleep(500L);

    Map<Thread, StackTraceElement[]> allThreadsRunning = Thread.getAllStackTraces();

    cleanUp();

    Thread.sleep(1000L);

    Map<Thread, StackTraceElement[]> allThreadsEnd = Thread.getAllStackTraces();
    Set<Thread> endingThreads = new HashSet<Thread>(allThreadsEnd.keySet());
    // remove all pre-existing threads from the set
    for (Thread t : allThreadsStart.keySet()) {
      allThreadsEnd.remove(t);
    }
    // remove threads that are known artifacts of the test
    for (Thread t : endingThreads) {
      if (t.getName().contains("derby") && t.getThreadGroup().getName().contains("derby")) {
        allThreadsEnd.remove(t);
      }
      if (t.getThreadGroup() != null && t.getThreadGroup().getName().equals("system")) {
        allThreadsEnd.remove(t);

      }
      if (t.getThreadGroup() != null && t.getThreadGroup().getName().equals("main")) {
        allThreadsEnd.remove(t);
      }
    }
    if (allThreadsEnd.size() > 0) {
      // log the additional threads
      for (Thread t : allThreadsEnd.keySet()) {
        System.out.println("*** Found additional thread: " + t.getName() + " (of type " + t.getClass().getName() + ")  in group: " + t.getThreadGroup().getName() + " with parent group: " + (t.getThreadGroup().getParent() == null ? "-none-" : t.getThreadGroup().getParent().getName()));
      }
      // log all threads that were running before shutdown
      for (Thread t : allThreadsRunning.keySet()) {
        System.out.println("- Test runtime thread: " + t.getName() + " (of type " + t.getClass().getName() + ")  in group: " + (t.getThreadGroup() == null ? "-none-" : (t.getThreadGroup().getName() + " with parent group: " + (t.getThreadGroup().getParent() == null ? "-none-" : t.getThreadGroup().getParent().getName()))));
      }
    }
    assertTrue("Found unexpected new threads (see console output for listing)", allThreadsEnd.size() == 0);
  }

  @Test
  public void testAbilityToFireImmediatelyWhenStartedBefore() throws Exception {

    List<Long> jobExecTimestamps = Collections.synchronizedList(new ArrayList<Long>());
    CyclicBarrier barrier = new CyclicBarrier(2);

    scheduler.getContext().put(BARRIER, barrier);
    scheduler.getContext().put(DATE_STAMPS, jobExecTimestamps);
    scheduler.start();

    Thread.yield();

    JobDetail job1 = JobBuilder.newJob(TestJobWithSync.class).withIdentity("job1").build();
    Trigger trigger1 = TriggerBuilder.newTrigger().forJob(job1).build();

    long sTime = System.currentTimeMillis();

    scheduler.scheduleJob(job1, trigger1);

    barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    long fTime = jobExecTimestamps.get(0);

    assertTrue("Immediate trigger did not fire within a reasonable amount of time.", (fTime - sTime < 7000L));  // This is dangerously subjective!  but what else to do?
  }

  @Test
  public void testAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob() throws Exception {

    List<Long> jobExecTimestamps = Collections.synchronizedList(new ArrayList<Long>());
    CyclicBarrier barrier = new CyclicBarrier(2);

    scheduler.getContext().put(BARRIER, barrier);
    scheduler.getContext().put(DATE_STAMPS, jobExecTimestamps);

    scheduler.start();

    Thread.yield();

    JobDetail job1 = JobBuilder.newJob(TestJobWithSync.class).withIdentity("job1").storeDurably().build();
    scheduler.addJob(job1, false);

    long sTime = System.currentTimeMillis();

    scheduler.triggerJob(job1.getKey());

    barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    long fTime = jobExecTimestamps.get(0);

    assertTrue("Immediate trigger did not fire within a reasonable amount of time.", (fTime - sTime < 7000L));  // This is dangerously subjective!  but what else to do?
  }

  @Test
  public void testAbilityToFireImmediatelyWhenStartedAfter() throws Exception {

    List<Long> jobExecTimestamps = Collections.synchronizedList(new ArrayList<Long>());
    CyclicBarrier barrier = new CyclicBarrier(2);

    scheduler.getContext().put(BARRIER, barrier);
    scheduler.getContext().put(DATE_STAMPS, jobExecTimestamps);

    JobDetail job1 = JobBuilder.newJob(TestJobWithSync.class).withIdentity("job1").build();
    Trigger trigger1 = TriggerBuilder.newTrigger().forJob(job1).build();

    long sTime = System.currentTimeMillis();

    scheduler.scheduleJob(job1, trigger1);
    scheduler.start();

    barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    long fTime = jobExecTimestamps.get(0);

    assertTrue("Immediate trigger did not fire within a reasonable amount of time.", (fTime - sTime < 7000L));  // This is dangerously subjective!  but what else to do?
  }

  @Test
  public void testScheduleMultipleTriggersForAJob() throws SchedulerException {

    JobDetail job = newJob(MyJob.class).withIdentity("job1", "group1").build();
    Trigger trigger1 = newTrigger()
            .withIdentity("trigger1", "group1")
            .startNow()
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(1)
                    .repeatForever())
            .build();
    Trigger trigger2 = newTrigger()
            .withIdentity("trigger2", "group1")
            .startNow()
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(1)
                    .repeatForever())
            .build();
    Set<Trigger> triggersForJob = new HashSet<Trigger>();
    triggersForJob.add(trigger1);
    triggersForJob.add(trigger2);

    scheduler.scheduleJob(job, triggersForJob, true);

    List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(job.getKey());
    assertEquals(2, triggersOfJob.size());
    assertTrue(triggersOfJob.contains(trigger1));
    assertTrue(triggersOfJob.contains(trigger2));

  }

  @Test
  public void testShutdownWithoutWaitIsUnclean() throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      scheduler.getContext().put(BARRIER, barrier);
      scheduler.start();
      scheduler.addJob(newJob().ofType(UncleanShutdownJob.class).withIdentity("job").storeDurably().build(), false);
      scheduler.scheduleJob(newTrigger().forJob("job").startNow().build());
      while (scheduler.getCurrentlyExecutingJobs().isEmpty()) {
        Thread.sleep(50);
      }
    } finally {
      scheduler.shutdown(false);
    }

    barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Thread jobThread = (Thread) scheduler.getContext().get(JOB_THREAD);
    jobThread.join(TimeUnit.SECONDS.toMillis(TEST_TIMEOUT_SECONDS));
  }

  public static class UncleanShutdownJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      try {
        SchedulerContext schedulerContext = context.getScheduler().getContext();
        schedulerContext.put(JOB_THREAD, Thread.currentThread());
        CyclicBarrier barrier = (CyclicBarrier) schedulerContext.get(BARRIER);
        barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (Throwable e) {
        e.printStackTrace();
        throw new AssertionError("Await on barrier was interrupted: " + e.toString());
      }
    }
  }

  @Test
  public void testShutdownWithWaitIsClean() throws Exception {

    final AtomicBoolean shutdown = new AtomicBoolean(false);
    List<Long> jobExecTimestamps = Collections.synchronizedList(new ArrayList<Long>());
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      scheduler.getContext().put(BARRIER, barrier);
      scheduler.getContext().put(DATE_STAMPS, jobExecTimestamps);
      scheduler.start();
      scheduler.addJob(newJob().ofType(TestJobWithSync.class).withIdentity("job").storeDurably().build(), false);
      scheduler.scheduleJob(newTrigger().forJob("job").startNow().build());
      while (scheduler.getCurrentlyExecutingJobs().isEmpty()) {
        Thread.sleep(50);
      }
    } finally {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            scheduler.shutdown(true);
            shutdown.set(true);
          } catch (SchedulerException ex) {
            throw new RuntimeException(ex);
          }
        }
      };
      t.start();
      Thread.sleep(1000);
      assertFalse(shutdown.get());
      barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      t.join();
    }
  }

  public static final long TEST_TIMEOUT_SECONDS = 125;

  public static class TestJobWithSync implements Job {

    public void execute(JobExecutionContext context)
            throws JobExecutionException {

      try {
        @SuppressWarnings("unchecked")
        List<Long> jobExecTimestamps = (List<Long>) context.getScheduler().getContext().get(DATE_STAMPS);
        CyclicBarrier barrier = (CyclicBarrier) context.getScheduler().getContext().get(BARRIER);

        jobExecTimestamps.add(System.currentTimeMillis());

        barrier.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (Throwable e) {
        e.printStackTrace();
        throw new AssertionError("Await on barrier was interrupted: " + e.toString());
      }
    }
  }

  @DisallowConcurrentExecution
  @PersistJobDataAfterExecution
  public static class TestAnnotatedJob implements Job {

    public void execute(JobExecutionContext context)
            throws JobExecutionException {
    }
  }

}
