package com.flaviof.quart.jobstore.hazelcast;

import java.io.Serializable;
import java.util.Date;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

class TriggerWrapper implements Serializable {

  private static final long serialVersionUID = 1L;

  public final TriggerKey key;

  public final JobKey jobKey;

  public final OperableTrigger trigger;

  private final TriggerState state;

  private final Long nextFireTime;

  public Long getNextFireTime() {

    return nextFireTime;
  }

  private TriggerWrapper(OperableTrigger trigger, TriggerState state) {

    if (trigger == null)
      throw new IllegalArgumentException("Trigger cannot be null!");
    this.trigger = trigger;
    key = trigger.getKey();
    this.jobKey = trigger.getJobKey();
    this.state = state;
    Date nextFireTime2 = trigger.getNextFireTime();
    if (nextFireTime2 != null) {
      this.nextFireTime = nextFireTime2.getTime();
    } else {
      nextFireTime = null;
    }
  }

  public static TriggerWrapper newTriggerWrapper(OperableTrigger trigger) {

    return newTriggerWrapper(trigger, TriggerState.NORMAL);
  }

  public static TriggerWrapper newTriggerWrapper(TriggerWrapper tw,
      TriggerState state) {

    return new TriggerWrapper(tw.trigger, state);
  }

  public static TriggerWrapper newTriggerWrapper(OperableTrigger trigger,
      TriggerState state) {

    TriggerWrapper tw = new TriggerWrapper(trigger, state);
    return tw;
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

  public TriggerState getState() {

    return state;
  }

  @Override
  public String toString() {

    return "TriggerWrapper{" + "trigger=" + trigger + ", state=" + state + ", nextFireTime=" + nextFireTime + '}';
  }

}