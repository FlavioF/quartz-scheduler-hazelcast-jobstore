package com.bikeemotion.quartz.jobstore.hazelcast;

import java.io.Serializable;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TriggerWrapper implements Serializable {
  
    private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private static final long serialVersionUID = 1L;

    public final TriggerKey key;

    public final JobKey jobKey;

    public final OperableTrigger trigger;

    private TriggerState state;

    public Long getNextFireTime() {

        return trigger == null || trigger.getNextFireTime() == null
                ? null
                : trigger.getNextFireTime().getTime();
    }

    private TriggerWrapper(OperableTrigger trigger, TriggerState state) {

        if (trigger == null) {
            throw new IllegalArgumentException("Trigger cannot be null!");
        }
        this.trigger = trigger;
        key = trigger.getKey();
        this.jobKey = trigger.getJobKey();
        this.state = state;
        
        // Change to normal if acquired is not released in 5 seconds
        if(state == TriggerState.ACQUIRED){
         executor.schedule( () -> { acquiredToNormal(); }, 5, TimeUnit.SECONDS);
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
    
    private void acquiredToNormal(){
      if(this.state==TriggerState.ACQUIRED){
        this.state = TriggerState.NORMAL;
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

    public TriggerState getState() {

        return state;
    }

    @Override
    public String toString() {

        return "TriggerWrapper{" + "trigger=" + trigger + ", state=" + state + ", nextFireTime=" + getNextFireTime() + '}';
    }

}
