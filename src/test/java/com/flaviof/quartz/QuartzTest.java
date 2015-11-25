package com.flaviof.quartz;

import com.flaviof.quartz.jobstore.hazelcast.HazelcastJobStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.Properties;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class QuartzTest {

  Scheduler scheduler;

  @BeforeClass
  public void setUp()
    throws SchedulerException, InterruptedException {

    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
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
  public void tearDown() {

  }

  @Test()
  public void testAcquireNextTrigger()
    throws Exception {
  }
}
