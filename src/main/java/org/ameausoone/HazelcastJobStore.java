package org.ameausoone;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

public class HazelcastJobStore implements JobStore {

	private String instanceName;

	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		// TODO Auto-generated method stub

	}

	public void schedulerStarted() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	public void schedulerPaused() {
		// TODO Auto-generated method stub

	}

	public void schedulerResumed() {
		// TODO Auto-generated method stub

	}

	public void shutdown() {
		// TODO Auto-generated method stub

	}

	public boolean supportsPersistence() {
		// TODO Auto-generated method stub
		return false;
	}

	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isClustered() {
		// TODO Auto-generated method stub
		return false;
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public void storeJob(JobDetail newJob, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean replaceTrigger(TriggerKey triggerKey,
			OperableTrigger newTrigger) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getNumberOfJobs() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getNumberOfTriggers() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getNumberOfCalendars() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> getJobGroupNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseAll() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public void resumeAll() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		// TODO Auto-generated method stub

	}

	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		// TODO Auto-generated method stub

	}

	public void setInstanceId(String schedInstId) {
		// TODO Auto-generated method stub

	}

	public void setInstanceName(String name) {
		this.instanceName = name;

	}

	public void setThreadPoolSize(int poolSize) {
		// TODO Auto-generated method stub

	}

}
