package org.ameausoone;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.ameausoone.TriggerState.ACQUIRED;
import static org.ameausoone.TriggerState.NORMAL;
import static org.ameausoone.TriggerState.PAUSED;
import static org.ameausoone.TriggerState.STATE_COMPLETED;
import static org.ameausoone.TriggerState.WAITING;
import static org.ameausoone.TriggerState.toClassicTriggerState;
import static org.ameausoone.TriggerWrapper.newTriggerWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
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

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;
import com.hazelcast.query.SqlPredicate;

/**
 * @author Antoine Méausoone
 * 
 */
public class HazelcastJobStore implements JobStore {

	private final Logger log = LoggerFactory.getLogger(HazelcastJobStore.class);

	private String instanceName;

	private SchedulerSignaler schedSignaler;

	private volatile boolean schedulerRunning = false;

	private ClassLoadHelper classLoadHelper;

	private long misfireThreshold;

	private HazelcastInstance hazelcastClient;

	protected String instanceId;

	public static final DateTimeFormatter FORMATTER = ISODateTimeFormat.basicDateTimeNoMillis();

	public String getInstanceId() {
		return instanceId;
	}

	public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {

		this.schedSignaler = signaler;
		this.classLoadHelper = loadHelper;
		System.setProperty("hazelcast.logging.type", "slf4j");
		ClientConfig clientConfig = new ClientConfig();
		ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
		networkConfig.addAddress("127.0.0.1:5701");

		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		getTriggerByKeyMap().addIndex("nextFireTime", true);
	}

	public void schedulerStarted() throws SchedulerException {
		log.info("Start HazelcastJobStore [" + instanceName + "]");
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
		// C/C from RAMJobStore
		return 5;
	}

	public boolean isClustered() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<JobKey, JobDetail> getJobMap() {
		return hazelcastClient.getMap(instanceName + '-' + "JobMap");
	}

	/**
	 * JobKeys by group name return a Multimap which map a groupName to a List of JobKeys
	 */
	private MultiMap<String, JobKey> getJobKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + '-' + "JobByGroupMap");
	}

	/**
	 * @return Map which contains Trigger by TriggerKey.
	 */
	private IMap<TriggerKey, TriggerWrapper> getTriggerByKeyMap() {
		return hazelcastClient.getMap(instanceName + '-' + "TriggerMap");
	}

	/**
	 * TriggerKeys by Trigger group name
	 */
	private MultiMap<String, TriggerKey> getTriggerKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + '-' + "TriggerByGroupMap");
	}

	private ISet<String> getHZPausedTriggerGroups() {
		return hazelcastClient.getSet(instanceName + '-' + "PausedTriggerGroupSet");
	}

	private ISet<String> getPausedJobGroups() {
		return hazelcastClient.getSet(instanceName + '-' + "PausedJobGroupSet");
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		storeJob(newJob, false);
		storeTrigger(newTrigger, false);

	}

	public void storeJob(JobDetail job, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetail newJob = (JobDetail) job.clone();
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		JobKey newJobKey = newJob.getKey();
		boolean containsKey = jobMap.containsKey(newJobKey);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException(newJob);
		}
		jobMap.lock(newJobKey);
		try {
			jobMap.put(newJobKey, newJob);
			getJobKeyByGroupMap().put(newJobKey.getGroup(), newJobKey);
		} finally {
			jobMap.unlock(newJobKey);
		}
	}

	public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
			throws ObjectAlreadyExistsException, JobPersistenceException {

		if (!replace) {
			for (Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
				JobDetail jobDetail = e.getKey();
				if (checkExists(jobDetail.getKey()))
					throw new ObjectAlreadyExistsException(jobDetail);
				for (Trigger trigger : e.getValue()) {
					if (checkExists(trigger.getKey()))
						throw new ObjectAlreadyExistsException(trigger);
				}
			}
		}
		// do bulk add...
		for (Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
			storeJob(e.getKey(), true);
			for (Trigger trigger : e.getValue()) {
				storeTrigger((OperableTrigger) trigger, true);
			}
		}

	}

	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (found) {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				removeTrigger(trigger.getKey());
			}
			getJobKeyByGroupMap().remove(jobKey.getGroup(), jobKey);
			return jobMap.remove(jobKey) != null;
		}
		return false;

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
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		if (jobKey == null)
			return null;
		if (jobMap.containsKey(jobKey)) {
			return jobMap.get(jobKey);
		}
		return null;
	}

	public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		OperableTrigger newTrigger = (OperableTrigger) trigger.clone();
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		TriggerKey triggerKey = newTrigger.getKey();
		triggerMap.lock(triggerKey);
		try {
			boolean containsKey = triggerMap.containsKey(triggerKey);
			if (containsKey && !replaceExisting)
				throw new ObjectAlreadyExistsException(newTrigger);

			if (retrieveJob(newTrigger.getJobKey()) == null)
				throw new JobPersistenceException("The job (" + newTrigger.getJobKey()
						+ ") referenced by the trigger does not exist.");

			boolean shouldBePausedByJobGroup = getPausedJobGroups().contains(newTrigger.getJobKey().getGroup());
			boolean shouldBePausedByTriggerGroup = getHZPausedTriggerGroups().contains(triggerKey.getGroup());
			boolean shouldBePaused = shouldBePausedByJobGroup || shouldBePausedByTriggerGroup;
			TriggerState state = shouldBePaused ? PAUSED : NORMAL;

			TriggerWrapper tw = newTriggerWrapper(newTrigger, state);
			storeTriggerWrapper(tw);

			getTriggerKeyByGroupMap().put(triggerKey.getGroup(), triggerKey);
		} finally {
			triggerMap.unlock(triggerKey);
		}
	}

	private void storeTriggerWrapper(TriggerWrapper tw) {
		getTriggerByKeyMap().put(tw.key, tw);
	}

	public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// TODO implement remove orphanedJob
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		boolean found = triggerMap.containsKey(triggerKey);
		if (found) {
			triggerMap.lock(triggerKey);
			try {
				getTriggerKeyByGroupMap().remove(triggerKey.getGroup(), triggerKey);
				triggerMap.remove(triggerKey);
			} finally {
				triggerMap.unlock(triggerKey);
			}
		}
		return found;
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
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		TriggerWrapper triggerWrapper = triggerMap.get(triggerKey);
		if (triggerWrapper == null)
			return null;
		return (OperableTrigger) triggerWrapper.getTrigger();
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		return jobMap.containsKey(jobKey);
	}

	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		return triggerMap.containsKey(triggerKey);
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		for (TriggerKey triggerKey : getTriggerByKeyMap().keySet()) {
			removeTrigger(triggerKey);
		}

		ISet<String> hzPausedTriggerGroups = getHZPausedTriggerGroups();
		for (String group : hzPausedTriggerGroups) {
			hzPausedTriggerGroups.remove(group);
		}

		IMap<JobKey, JobDetail> jobMap = getJobMap();
		for (JobKey jobKey : jobMap.keySet()) {
			removeJob(jobKey);
		}

		ISet<String> pausedJobGroups = getPausedJobGroups();
		for (String pausedJobGroup : pausedJobGroups) {
			pausedJobGroups.remove(pausedJobGroup);
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
		IMap<String, Calendar> calendarMap = hazelcastClient.getMap(instanceName + "CalendarMap");
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
		return getTriggerByKeyMap().size();
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
		Set<TriggerKey> triggerKeys = getTriggerByKeyMap().keySet();
		Set<String> groupNames = newHashSet();
		for (TriggerKey triggerKey : triggerKeys) {
			groupNames.add(triggerKey.getGroup());
		}
		return newArrayList(groupNames);
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		return newArrayList(getCalendarMap().keySet());
	}

	public List<OperableTrigger> getTriggersForJob(final JobKey jobKey) throws JobPersistenceException {
		// TODO IMap.values(Predicate predicate) is certainly more efficient, but didn't find how to fix : "Caused by:
		// java.io.NotSerializableException: org.ameausoone.HazelcastJobStore."
		if (jobKey == null)
			return Collections.emptyList();
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		Collection<TriggerWrapper> values = triggerMap.values();
		List<OperableTrigger> outList = Lists.newArrayList();
		for (TriggerWrapper tw : values) {
			if (jobKey.equals(tw.getTrigger().getJobKey()))
				outList.add((OperableTrigger) tw.getTrigger());
		}
		return outList;
	}

	public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// IMap<TriggerKey, TriggerState> triggerStateMap = getTriggerStateMap();
		IMap<TriggerKey, TriggerWrapper> triggerByKeyMap = getTriggerByKeyMap();

		triggerByKeyMap.lock(triggerKey);
		try {
			TriggerWrapper tw = triggerByKeyMap.get(triggerKey);
			storeTriggerWrapper(newTriggerWrapper(tw, PAUSED));
		} finally {
			triggerByKeyMap.unlock(triggerKey);
		}
	}

	public org.quartz.Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		triggerMap.lock(triggerKey);
		org.quartz.Trigger.TriggerState result = org.quartz.Trigger.TriggerState.NONE;
		try {
			TriggerWrapper tw = triggerMap.get(triggerKey);
			if (tw != null)
				result = toClassicTriggerState(tw.getState());
		} finally {
			triggerMap.unlock(triggerKey);
		}
		return result;
	}

	public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		triggerMap.lock(triggerKey);
		try {
			TriggerWrapper tw = triggerMap.get(triggerKey);
			storeTriggerWrapper(TriggerWrapper.newTriggerWrapper(tw, NORMAL));
		} finally {
			triggerMap.unlock(triggerKey);
		}
	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		List<String> pausedGroups = new LinkedList<String>();

		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		switch (operator) {
		case EQUALS:
			if (getHZPausedTriggerGroups().add(matcher.getCompareToValue())) {
				pausedGroups.add(matcher.getCompareToValue());
			}
			break;
		default:
			for (String group : getTriggerKeyByGroupMap().keySet()) {
				if (operator.evaluate(group, matcher.getCompareToValue())) {
					if (getHZPausedTriggerGroups().add(matcher.getCompareToValue())) {
						pausedGroups.add(group);
					}
				}
			}
		}

		for (String pausedGroup : pausedGroups) {
			Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.triggerGroupEquals(pausedGroup));
			for (TriggerKey key : keys) {
				pauseTrigger(key);
			}
		}
		return pausedGroups;
	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		Set<String> resumeGroups = new HashSet<String>();
		Set<TriggerKey> keys = getTriggerKeys(matcher);
		ISet<String> pausedTriggerGroups = getHZPausedTriggerGroups();

		for (TriggerKey triggerKey : keys) {
			resumeGroups.add(triggerKey.getGroup());
			TriggerWrapper tw = getTriggerByKeyMap().get(triggerKey);
			OperableTrigger trigger = tw.getTrigger();
			String jobGroup = trigger.getJobKey().getGroup();
			if (getPausedJobGroups().contains(jobGroup)) {
				continue;
			}
			resumeTrigger(triggerKey);
		}
		for (String group : resumeGroups) {
			pausedTriggerGroups.remove(group);
		}
		return new ArrayList<String>(resumeGroups);
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (!found)
			return;
		jobMap.lock(jobKey);
		try {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				pauseTrigger(trigger.getKey());
			}
		} finally {
			jobMap.unlock(jobKey);
		}
	}

	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (!found)
			return;
		jobMap.lock(jobKey);
		try {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				resumeTrigger(trigger.getKey());
			}
		} finally {
			jobMap.unlock(jobKey);
		}

	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
		List<String> pausedGroups = new LinkedList<String>();
		ISet<String> pausedJobGroups = getPausedJobGroups();
		StringMatcher.StringOperatorName operator = groupMatcher.getCompareWithOperator();
		MultiMap<String, JobKey> jobKeyByGroupMap = getJobKeyByGroupMap();
		switch (operator) {
		case EQUALS:
			if (pausedJobGroups.add(groupMatcher.getCompareToValue())) {
				pausedGroups.add(groupMatcher.getCompareToValue());
			}
			break;
		default:
			for (String jobGroup : jobKeyByGroupMap.keySet()) {
				if (operator.evaluate(jobGroup, groupMatcher.getCompareToValue())) {
					if (pausedJobGroups.add(jobGroup)) {
						pausedGroups.add(jobGroup);
					}
				}
			}
		}

		for (String groupName : pausedGroups) {
			for (JobKey jobKey : getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
				pauseJob(jobKey);
			}
		}
		return pausedGroups;
	}

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
		Set<String> resumeGroups = new HashSet<String>();

		Set<JobKey> jobKeys = getJobKeys(matcher);
		ISet<String> pausedJobGroups = getPausedJobGroups();

		for (JobKey jobKey : jobKeys) {
			resumeGroups.add(jobKey.getGroup());
			resumeJob(jobKey);
		}
		for (String group : resumeGroups) {
			pausedJobGroups.remove(group);
		}
		return new ArrayList<String>(resumeGroups);
	}

	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		return new HashSet<String>(getHZPausedTriggerGroups());
	}

	public void pauseAll() throws JobPersistenceException {
		List<String> triggerGroupNames = getTriggerGroupNames();
		for (String triggerGroup : triggerGroupNames) {
			pauseTriggers(GroupMatcher.triggerGroupEquals(triggerGroup));
		}
	}

	public void resumeAll() throws JobPersistenceException {
		List<String> triggerGroupNames = getTriggerGroupNames();
		for (String triggerGroup : triggerGroupNames) {
			resumeTriggers(GroupMatcher.triggerGroupEquals(triggerGroup));
		}
	}

	/**
	 * @see org.quartz.spi.JobStore#acquireNextTriggers(long, int, long)
	 * 
	 * @param noLaterThan
	 *            highest value of <code>getNextFireTime()</code> of the triggers (exclusive)
	 * @param timeWindow
	 *            highest value of <code>getNextFireTime()</code> of the triggers (inclusive)
	 * @param maxCount
	 *            maximum number of trigger keys allow to acquired in the returning list.
	 */
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
			throws JobPersistenceException {

		List<OperableTrigger> result = new ArrayList<OperableTrigger>();
		// Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
		Set<TriggerWrapper> excludedTriggers = new HashSet<TriggerWrapper>();
		long firstAcquiredTriggerFireTime = 0;

		IMap<TriggerKey, TriggerWrapper> triggerByKeyMap = getTriggerByKeyMap();
		long noEarlierThan = getMisfireTime();
		long noLaterThanWithTimeWindow = noLaterThan + timeWindow;
		if (noLaterThanWithTimeWindow < noEarlierThan)
			return Lists.newArrayList();

		String sqlPredicate = "nextFireTime between " + noEarlierThan + " and " + noLaterThanWithTimeWindow
				+ " and state != ACQUIRED";

		Collection<TriggerWrapper> timeTriggers = triggerByKeyMap.values(new SqlPredicate(sqlPredicate));
		sqlPredicate = "nextFireTime between " + FORMATTER.print(noEarlierThan) + " and "
				+ FORMATTER.print(noLaterThanWithTimeWindow) + " and state != ACQUIRED";
		log.debug("sqlPredicate : [{}]", sqlPredicate);
		log.debug("timeTriggers : [{}]", timeTriggers);
		Iterator<TriggerWrapper> iterator = timeTriggers.iterator();
		while (true) {
			TriggerWrapper tw;

			try {
				tw = iterator.next();
				if (tw == null)
					break;
			} catch (java.util.NoSuchElementException nsee) {
				break;
			}

			if (tw.trigger.getNextFireTime() == null) {
				continue;
			}

			if (applyMisfire(tw)) {
				if (tw.trigger.getNextFireTime() != null) {
					storeTriggerWrapper(newTriggerWrapper(tw, NORMAL));
				}
				continue;
			}

			if (tw.getTrigger().getNextFireTime().getTime() > noLaterThan + timeWindow) {
				storeTriggerWrapper(newTriggerWrapper(tw, NORMAL));
				break;
			}

			// TODO implement job with DisallowConcurrentExecution

			// If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result,
			// then
			// put it back into the timeTriggers set and continue to search for next trigger.
			// JobKey jobKey = tw.trigger.getJobKey();
			// JobDetail job = jobsByKey.get(tw.trigger.getJobKey()).jobDetail;
			// if (job.isConcurrentExectionDisallowed()) {
			// if (acquiredJobKeysForNoConcurrentExec.contains(jobKey)) {
			// excludedTriggers.add(tw);
			// continue; // go to next trigger in store.
			// } else {
			// acquiredJobKeysForNoConcurrentExec.add(jobKey);
			// }
			// }

			OperableTrigger trig = (OperableTrigger) tw.trigger.clone();
			trig.setFireInstanceId(getFiredTriggerRecordId());
			storeTriggerWrapper(newTriggerWrapper(trig, ACQUIRED));
			result.add(trig);
			if (firstAcquiredTriggerFireTime == 0)
				firstAcquiredTriggerFireTime = tw.trigger.getNextFireTime().getTime();

			if (result.size() == maxCount)
				break;
		}

		// If we did excluded triggers to prevent ACQUIRE state due to DisallowConcurrentExecution, we need to add
		// them back to store.
		if (excludedTriggers.size() > 0)
			timeTriggers.addAll(excludedTriggers);
		return result;
	}

	protected boolean applyMisfire(TriggerWrapper tw) throws JobPersistenceException {

		long misfireTime = System.currentTimeMillis();
		if (getMisfireThreshold() > 0) {
			misfireTime -= getMisfireThreshold();
		}

		Date tnft = tw.trigger.getNextFireTime();
		if (tnft == null || tnft.getTime() > misfireTime
				|| tw.trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
			return false;
		}

		Calendar cal = null;
		if (tw.trigger.getCalendarName() != null) {
			cal = retrieveCalendar(tw.trigger.getCalendarName());
		}

		this.schedSignaler.notifyTriggerListenersMisfired((OperableTrigger) tw.trigger.clone());

		tw.trigger.updateAfterMisfire(cal);

		if (tw.trigger.getNextFireTime() == null) {
			storeTriggerWrapper(newTriggerWrapper(tw, STATE_COMPLETED));
			schedSignaler.notifySchedulerListenersFinalized(tw.trigger);
		} else if (tnft.equals(tw.trigger.getNextFireTime())) {
			return false;
		}

		return true;
	}

	protected long getMisfireTime() {
		long misfireTime = System.currentTimeMillis();
		if (getMisfireThreshold() > 0) {
			misfireTime -= getMisfireThreshold();
		}

		return (misfireTime > 0) ? misfireTime : 0;
	}

	private static long ftrCtr = System.currentTimeMillis();

	protected synchronized String getFiredTriggerRecordId() {
		return getInstanceId() + ftrCtr++;
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		TriggerKey triggerKey = trigger.getKey();
		lock(triggerKey);
		try {
			storeTriggerWrapper(newTriggerWrapper(trigger, WAITING));
		} finally {
			unlock(triggerKey);
		}

	}

	private void lock(final TriggerKey triggerKey) {
		getTriggerByKeyMap().lock(triggerKey);
	}

	private void unlock(final TriggerKey triggerKey) {
		getTriggerByKeyMap().unlock(triggerKey);
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
