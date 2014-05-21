package org.ameausoone;

import java.util.Date;

import org.quartz.DateBuilder;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerException;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class TestHazelcastJobStore {

	private HazelcastInstance hazelcastInstance;
	private JobStore hazelcastJobStore;
	private SampleSignaler fSignaler;
	private JobDetailImpl fJobDetail;

	@SuppressWarnings("deprecation")
	@BeforeClass
	public void setUp() throws SchedulerException {
		Config config = new Config();
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);

		this.fSignaler = new SampleSignaler();
		hazelcastJobStore = createJobStore("jobstore");
		ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
		loadHelper.initialize();

		hazelcastJobStore.initialize(loadHelper, this.fSignaler);
		hazelcastJobStore.schedulerStarted();

		this.fJobDetail = new JobDetailImpl("job1", "jobGroup1", MyJob.class);
		this.hazelcastJobStore.storeJob(this.fJobDetail, false);

	}

	protected JobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);

		return hazelcastJobStore;
	}

	@SuppressWarnings("deprecation")
	@Test
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
		hazelcastInstance.shutdown();
	}
}
