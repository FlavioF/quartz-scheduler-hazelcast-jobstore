package org.ameausoone.util;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class StartHazelcast {

	public static void main(String[] args) {
		Config config = new Config();
		config.setProperty("hazelcast.logging.type", "slf4j");
		HazelcastInstance newHazelcastInstance = Hazelcast
				.newHazelcastInstance(config);

	}

}
