package org.ameausoone;

import org.quartz.SchedulerException;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
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

	}

	protected JobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);

		return hazelcastJobStore;
	}

	@Test
	public void simpleTest() {

	}

	@AfterClass
	public void cleanUp() {
		hazelcastJobStore.shutdown();
		hazelcastInstance.shutdown();
	}
}
