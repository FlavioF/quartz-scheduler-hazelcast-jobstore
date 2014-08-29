package org.ameausoone;

public enum TriggerState {
	NONE, NORMAL, PAUSED, COMPLETE, ERROR, BLOCKED, ACQUIRED, WAITING;

	public static org.quartz.Trigger.TriggerState toClassicTriggerState(TriggerState state) {
		switch (state) {
		case PAUSED:
			return org.quartz.Trigger.TriggerState.PAUSED;
		case COMPLETE:
			return org.quartz.Trigger.TriggerState.COMPLETE;
		case ERROR:
			return org.quartz.Trigger.TriggerState.ERROR;
		case BLOCKED:
			return org.quartz.Trigger.TriggerState.BLOCKED;
		case NORMAL:
		case ACQUIRED:
		case WAITING:
			return org.quartz.Trigger.TriggerState.NORMAL;
		default:
			return org.quartz.Trigger.TriggerState.NORMAL;
		}
	}
}