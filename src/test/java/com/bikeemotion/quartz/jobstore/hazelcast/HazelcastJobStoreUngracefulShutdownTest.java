package com.bikeemotion.quartz.jobstore.hazelcast;

import static org.quartz.Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.testng.annotations.Test;

import com.bikeemotion.quartz.AbstractTest;
import com.hazelcast.core.HazelcastInstance;

/**
 * Tests running {@link HazelcastJobStore} on two different nodes. If one node crashes during
 * {@link JobStore#acquireNextTriggers(long, int, long)}, the second node should still be able
 * to get the triggers.
 * <p>
 * This test is unstable in a way that it might succeed when it should in fact fail. However, it
 * will never fail when it should succeed.
 * </p>
 * <p>
 * The reason it is unstable is because we need to time our termination of Hazelcast so it is done
 * right in the middle of {@link JobStore#acquireNextTriggers(long, int, long)}, and that doesn't
 * always happen.
 * </p>
 * 
 * @author Anton Johansson
 */
public class HazelcastJobStoreUngracefulShutdownTest extends AbstractTest {

  @Test
  public void testOneOfTwoInstancesCrashing()
    throws Exception {

    // Build node 1
    HazelcastInstance hazelcast1 = createMulticastEnabledHazelcastInstance("testOneOfTwoInstancesCrashing");
    HazelcastJobStore.setHazelcastClient(hazelcast1);
    HazelcastJobStore jobstore1 = createJobStore("jobstore1");
    jobstore1.setShutdownHazelcastOnShutdown(false);
    jobstore1.initialize(null, new SampleSignaler());

    // Build node 2
    HazelcastInstance hazelcast2 = createMulticastEnabledHazelcastInstance("testOneOfTwoInstancesCrashing");
    HazelcastJobStore.setHazelcastClient(hazelcast2);
    HazelcastJobStore jobstore2 = createJobStore("jobstore2");
    jobstore2.setShutdownHazelcastOnShutdown(false);
    jobstore2.initialize(null, new SampleSignaler());

    // Add a job and its trigger to the scheduler
    JobDetail job = JobBuilder.newJob(TestJob.class).withIdentity("job1", "jobGroup1").build();
    OperableTrigger trigger = buildAndComputeTrigger("trigger1", "triggerGroup1", job, new Date().getTime());
    trigger.setMisfireInstruction(MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);
    jobstore1.storeJobAndTrigger(job, (OperableTrigger) trigger);
    long firstFireTime = new Date(trigger.getNextFireTime().getTime()).getTime();

    // Create a thread for acquiring next triggers on node 1
    Thread acquireThread = new Thread(() -> {
      try {
        List<OperableTrigger> triggers1 = jobstore1.acquireNextTriggers(firstFireTime + 500, 1, 0L);
        triggers1.forEach(jobstore1::releaseAcquiredTrigger);
      } catch (JobPersistenceException e) {
        throw new RuntimeException(e);
      }
    }, "acquireThread");

    // Create a thread for terminating Hazelcast on node 1
    Thread terminateThread = new Thread(hazelcast1.getLifecycleService()::terminate, "terminateThread");

    // Start acquiring next triggers and right after start terminating Hazelcast
    acquireThread.start();
    Thread.sleep(2);
    terminateThread.start();

    // Wait a bit
    Thread.sleep(2000);

    // Acquire next triggers on node 2, we should get our trigger here!
    List<OperableTrigger> triggers2 = jobstore2.acquireNextTriggers(firstFireTime + 500, 1, 0L);
    assertEquals(triggers2.size(), 1, "Should find 1 trigger on node 2 after node 1 crashed.");
  }
}
