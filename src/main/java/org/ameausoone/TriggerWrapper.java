package org.ameausoone;

import java.io.Serializable;

import org.quartz.JobKey;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

class TriggerWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	public final TriggerKey key;

	public final JobKey jobKey;

	public final OperableTrigger trigger;

	public TriggerState state = TriggerState.NORMAL;

	TriggerWrapper(OperableTrigger trigger) {
		if (trigger == null)
			throw new IllegalArgumentException("Trigger cannot be null!");
		this.trigger = trigger;
		key = trigger.getKey();
		this.jobKey = trigger.getJobKey();
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
}