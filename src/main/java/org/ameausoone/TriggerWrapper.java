package org.ameausoone;

import java.io.Serializable;
import java.util.Date;

import org.quartz.JobKey;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import com.google.common.base.Objects;

class TriggerWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	public final TriggerKey key;

	public final JobKey jobKey;

	public final OperableTrigger trigger;

	public TriggerState state = TriggerState.NORMAL;

	public final Long nextFireTime;

	public Long getNextFireTime() {
		return nextFireTime;
	}

	TriggerWrapper(OperableTrigger trigger) {
		if (trigger == null)
			throw new IllegalArgumentException("Trigger cannot be null!");
		this.trigger = trigger;
		key = trigger.getKey();
		this.jobKey = trigger.getJobKey();
		Date nextFireTime2 = trigger.getNextFireTime();
		if (nextFireTime2 != null) {
			this.nextFireTime = nextFireTime2.getTime();
		} else {
			nextFireTime = null;
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TriggerWrapper) {
			TriggerWrapper tw = (TriggerWrapper) obj;
			if (tw.key.equals(this.key)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	public OperableTrigger getTrigger() {
		return this.trigger;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("key", key) //
				.add("state", state) //
				.add("nextFireTime", nextFireTime) //
				.toString();
	}
}