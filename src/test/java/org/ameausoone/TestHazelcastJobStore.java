package org.ameausoone;

import org.quartz.spi.JobStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class TestHazelcastJobStore {

	private HazelcastInstance hazelcastInstance;
	private JobStore hazelcastJobStore;

	@BeforeClass
	public void setUp() {
		Config config = new Config();
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		hazelcastJobStore = createJobStore("jobstore");
	}

	protected JobStore createJobStore(String name) {
		HazelcastJobStore hazelcastJobStore = new HazelcastJobStore();
		hazelcastJobStore.setInstanceName(name);

		return hazelcastJobStore;
	}

	@AfterClass
	public void cleanUp() {
		hazelcastInstance.shutdown();
		hazelcastJobStore.shutdown();
	}
}
