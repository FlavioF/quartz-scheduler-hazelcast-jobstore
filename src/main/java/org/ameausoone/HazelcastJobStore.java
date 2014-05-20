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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastJobStore implements JobStore {

	private final Logger logger = LoggerFactory
			.getLogger(HazelcastJobStore.class);

	private String instanceName;

	private SchedulerSignaler schedSignaler;

	private volatile boolean schedulerRunning = false;

	private ClassLoadHelper classLoadHelper;

	private long misfireThreshold;

	private HazelcastInstance hazelcastClient;

	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {

		this.schedSignaler = signaler;
		this.classLoadHelper = loadHelper;
		ClientConfig clientConfig = new ClientConfig();
		// ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
		clientConfig.addAddress("127.0.0.1:5701");
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
	}

	public void schedulerStarted() throws SchedulerException {
		logger.info("Start HazelcastJobStore [" + instanceName + "]");
		// TODO Auto-generated method stub
	}

	public long getMisfireThreshold() {
		return misfireThreshold;
	}

	/**
	 * The number of milliseconds by which a trigger must have missed its
	 * next-fire-time, in order for it to be considered "misfired" and thus have
	 * its misfire instruction applied.
	 * 
	 * @param misfireThreshold
	 *            the new misfire threshold
	 */
	@SuppressWarnings("UnusedDeclaration")
	public void setMisfireThreshold(long misfireThreshold) {
		if (misfireThreshold < 1) {
			throw new IllegalArgumentException(
					"Misfire threshold must be larger than 0");
		}
		this.misfireThreshold = misfireThreshold;
	}

	public void schedulerPaused() {
		schedulerRunning = false;

	}

	public void schedulerResumed() {
		schedulerRunning = true;

	}

	public void shutdown() {
		hazelcastClient.shutdown();
	}

	public boolean supportsPersistence() {
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
