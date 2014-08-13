package org.ameausoone;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.Collection;
import java.util.HashSet;
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
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

/**
 * @author Antoine Méausoone
 * 
 */
public class HazelcastJobStore implements JobStore {

	/**
	 * Suffix to identify job's map in Hazelcast storage.
	 */
	private static final String JOB_MAP_KEY = "-JobMap";

	private static final String JOBKEY_BY_GROUP_MAP_KEY = "-JobByGroupMap";

	private static final String TRIGGERKEY_BY_GROUP_MAP_KEY = "-TriggerByGroupMap";
	/**
	 * Suffix to identify trigger's map in Hazelcast storage.
	 */
	private static final String TRIGGER_MAP_KEY = "-TriggerMap";

	private static final String CALENDAR_MAP_KEY = "-CalendarMap";

	private final Logger logger = LoggerFactory.getLogger(HazelcastJobStore.class);

	private String instanceName;

	private SchedulerSignaler schedSignaler;

	private volatile boolean schedulerRunning = false;

	private ClassLoadHelper classLoadHelper;

	private long misfireThreshold;

	private HazelcastInstance hazelcastClient;

	public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {

		this.schedSignaler = signaler;
		this.classLoadHelper = loadHelper;
		System.setProperty("hazelcast.logging.type", "slf4j");
		ClientConfig clientConfig = new ClientConfig();
		// clientConfig.addAddress("127.0.0.1:5701");
		ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
		networkConfig.addAddress("127.0.0.1:5701");

		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
	}

	public void schedulerStarted() throws SchedulerException {
		logger.info("Start HazelcastJobStore [" + instanceName + "]");
	}

	public long getMisfireThreshold() {
		return misfireThreshold;
	}

	/**
	 * The number of milliseconds by which a trigger must have missed its next-fire-time, in order for it to be
	 * considered "misfired" and thus have its misfire instruction applied.
	 * 
	 * @param misfireThreshold
	 *            the new misfire threshold
	 */
	@SuppressWarnings("UnusedDeclaration")
	public void setMisfireThreshold(long misfireThreshold) {
		if (misfireThreshold < 1) {
			throw new IllegalArgumentException("Misfire threshold must be larger than 0");
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

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<JobKey, JobDetail> getJobMap() {
		return hazelcastClient.getMap(instanceName + JOB_MAP_KEY);
	}

	private MultiMap<String, JobKey> getJobKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + JOBKEY_BY_GROUP_MAP_KEY);
	}

	private MultiMap<String, TriggerKey> getTriggerKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + TRIGGERKEY_BY_GROUP_MAP_KEY);
	}

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<TriggerKey, Trigger> getTriggerMap() {
		return hazelcastClient.getMap(instanceName + TRIGGER_MAP_KEY);
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		storeJob(newJob, false);
		storeTrigger(newTrigger, false);

	}

	public void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		JobKey jobKey = newJob.getKey();
		boolean containsKey = jobMap.containsKey(jobKey);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException(newJob);
		} else {
			jobMap.put(jobKey, newJob);
			getJobKeyByGroupMap().put(jobKey.getGroup(), jobKey);
		}
	}

	public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		getJobKeyByGroupMap().remove(jobKey.getGroup(), jobKey);
		return jobMap.remove(jobKey) != null;
	}

	public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
		boolean jobExists = false;
		for (JobKey jobKey : jobKeys) {
			boolean removeJob = removeJob(jobKey);
			if (removeJob) {
				jobExists = true;
			}
		}
		return jobExists;
	}

	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> iMap = getJobMap();
		return iMap.get(jobKey);
	}

	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		IMap<TriggerKey, Trigger> triggerMap = getTriggerMap();
		TriggerKey triggerKey = newTrigger.getKey();
		boolean containsKey = triggerMap.containsKey(triggerKey);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException(newTrigger);
		} else {
			triggerMap.put(triggerKey, newTrigger);
			getTriggerKeyByGroupMap().put(triggerKey.getGroup(), triggerKey);
		}
	}

	public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, Trigger> triggerMap = getTriggerMap();
		getTriggerKeyByGroupMap().remove(triggerKey.getGroup(), triggerKey);
		return triggerMap.remove(triggerKey) != null;
	}

	public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
		boolean triggerExists = false;
		for (TriggerKey triggerKey : triggerKeys) {
			boolean removetrigger = removeTrigger(triggerKey);
			if (removetrigger) {
				triggerExists = true;
			}
		}
		return triggerExists;
	}

	public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
		newTrigger.setKey(triggerKey);
		boolean checkExists = checkExists(triggerKey);
		storeTrigger(newTrigger, true);
		return checkExists;
	}

	public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, Trigger> triggerMap = getTriggerMap();
		return (OperableTrigger) triggerMap.get(triggerKey);
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		return jobMap.containsKey(jobKey);
	}

	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, Trigger> triggerMap = getTriggerMap();
		return triggerMap.containsKey(triggerKey);
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		for (JobKey jobKey : jobMap.keySet()) {
			removeJob(jobKey);
		}

		IMap<TriggerKey, Trigger> triggerMap = getTriggerMap();
		for (TriggerKey triggerKey : triggerMap.keySet()) {
			removeTrigger(triggerKey);
		}

		IMap<String, Calendar> calendarMap = getCalendarMap();
		for (String calName : calendarMap.keySet()) {
			removeCalendar(calName);
		}

	}

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<String, Calendar> getCalendarMap() {
		IMap<String, Calendar> calendarMap = hazelcastClient.getMap(instanceName + CALENDAR_MAP_KEY);
		return calendarMap;
	}

	public void storeCalendar(String calName, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		IMap<String, Calendar> calendarMap = getCalendarMap();
		boolean containsKey = calendarMap.containsKey(calName);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException("Calendar with name '" + calName + "' already exists.");
		} else {
			calendarMap.put(calName, calendar);
		}

	}

	public boolean removeCalendar(String calName) throws JobPersistenceException {
		// TODO implement
		// "If removal of the Calendar would result in Triggers pointing to non-existent calendars, then a JobPersistenceException will be thrown."
		return getCalendarMap().remove(calName) != null;
	}

	public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
		IMap<String, Calendar> calendarMap = getCalendarMap();
		return calendarMap.get(calName);
	}

	public int getNumberOfJobs() throws JobPersistenceException {
		return getJobMap().size();
	}

	public int getNumberOfTriggers() throws JobPersistenceException {
		return getTriggerMap().size();
	}

	public int getNumberOfCalendars() throws JobPersistenceException {
		return getCalendarMap().size();
	}

	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
		Set<JobKey> outList = null;
		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		String groupNameCompareValue = matcher.getCompareToValue();
		switch (operator) {
		case EQUALS:
			Collection<JobKey> jobKeys = getJobKeyByGroupMap().get(groupNameCompareValue);
			if (jobKeys != null) {
				outList = new HashSet<JobKey>();

				for (JobKey jobKey : jobKeys) {

					if (jobKey != null) {
						outList.add(jobKey);
					}
				}
			}
			break;
		default:
			for (String groupName : getJobKeyByGroupMap().keySet()) {
				if (operator.evaluate(groupName, groupNameCompareValue)) {
					if (outList == null) {
						outList = new HashSet<JobKey>();
					}
					for (JobKey jobKey : getJobKeyByGroupMap().get(groupName)) {
						if (jobKey != null) {
							outList.add(jobKey);
						}
					}
				}
			}
		}

		return outList == null ? java.util.Collections.<JobKey> emptySet() : outList;
	}

	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		Set<TriggerKey> outList = null;
		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		String groupNameCompareValue = matcher.getCompareToValue();
		switch (operator) {
		case EQUALS:
			Collection<TriggerKey> triggerKeys = getTriggerKeyByGroupMap().get(groupNameCompareValue);
			if (triggerKeys != null) {
				outList = newHashSet();

				for (TriggerKey triggerKey : triggerKeys) {

					if (triggerKey != null) {
						outList.add(triggerKey);
					}
				}
			}
			break;
		default:
			for (String groupName : getTriggerKeyByGroupMap().keySet()) {
				if (operator.evaluate(groupName, groupNameCompareValue)) {
					if (outList == null) {
						outList = newHashSet();
					}
					for (TriggerKey triggerKey : getTriggerKeyByGroupMap().get(groupName)) {
						if (triggerKey != null) {
							outList.add(triggerKey);
						}
					}
				}
			}
		}
		return outList == null ? java.util.Collections.<TriggerKey> emptySet() : outList;
	}

	public List<String> getJobGroupNames() throws JobPersistenceException {
		return newArrayList(getJobKeyByGroupMap().keySet());
	}

	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		Set<TriggerKey> triggerKeys = getTriggerMap().keySet();
		Set<String> groupNames = newHashSet();
		for (TriggerKey triggerKey : triggerKeys) {
			groupNames.add(triggerKey.getGroup());
		}
		return newArrayList(groupNames);
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		return newArrayList(getCalendarMap().keySet());
	}

	public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
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

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void pauseAll() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	public void resumeAll() throws JobPersistenceException {
		// TODO Auto-generated method stub
	}

	public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		// TODO Auto-generated method stub

	}

	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
			CompletedExecutionInstruction triggerInstCode) {
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
