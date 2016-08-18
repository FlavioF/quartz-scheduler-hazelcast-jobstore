package com.bikeemotion.quartz.jobstore.hazelcast;

public enum TriggerState {
  NONE,
  NORMAL,
  PAUSED,
  COMPLETE,
  ERROR,
  BLOCKED,
  PAUSED_BLOCKED,
  ACQUIRED,
  WAITING,
  STATE_COMPLETED;

  public static org.quartz.Trigger.TriggerState toClassicTriggerState(
      TriggerState state) {

    switch (state) {
    case PAUSED:
      return org.quartz.Trigger.TriggerState.PAUSED;
    case COMPLETE:
      return org.quartz.Trigger.TriggerState.COMPLETE;
    case ERROR:
      return org.quartz.Trigger.TriggerState.ERROR;
    case BLOCKED:
    case PAUSED_BLOCKED:
      return org.quartz.Trigger.TriggerState.BLOCKED;
    case NORMAL:
    case ACQUIRED:
    case WAITING:
    case STATE_COMPLETED:
      return org.quartz.Trigger.TriggerState.NORMAL;
    default:
      return org.quartz.Trigger.TriggerState.NORMAL;
    }
  }
}