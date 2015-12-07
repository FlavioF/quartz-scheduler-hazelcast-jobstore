package com.flaviof.quartz;

import com.flaviof.quartz.jobstore.hazelcast.HazelcastJobStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import java.util.Properties;
import org.joda.time.DateTime;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import static org.quartz.Scheduler.DEFAULT_GROUP;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.BeforeMethod;

public class QuartzTest extends AbstractTest {

  public static int jobExecs = 0;

  Scheduler scheduler;

  @BeforeClass
  public void setUp()
    throws SchedulerException, InterruptedException {

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

  @AfterClass
  public void tearDown()
    throws SchedulerException {

    cleanUp();
    hazelcastInstance.shutdown();
    scheduler.shutdown();
  }

  @BeforeMethod
  public void cleanUp()
    throws SchedulerException {

    scheduler.clear();
    MyJob.count = 0;
    MyJob.jobKeys.clear();
    MyJob.triggerKeys.clear();

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

}
