package org.ameausoone;

import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.spi.SchedulerSignaler;

public class SampleSignaler implements SchedulerSignaler {
	volatile int fMisfireCount = 0;

	public void notifyTriggerListenersMisfired(Trigger trigger) {
		System.out.println("Trigger misfired: " + trigger.getKey() + ", fire time: " + trigger.getNextFireTime());
		fMisfireCount++;
	}

	public void signalSchedulingChange(long candidateNewNextFireTime) {
	}

	public void notifySchedulerListenersFinalized(Trigger trigger) {
	}

	public void notifySchedulerListenersJobDeleted(JobKey jobKey) {
	}

	public void notifySchedulerListenersError(String string, SchedulerException jpe) {
	}
}