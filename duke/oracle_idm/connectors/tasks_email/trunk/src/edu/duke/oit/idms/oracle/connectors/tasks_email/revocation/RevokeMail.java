package edu.duke.oit.idms.oracle.connectors.tasks_email.revocation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;
import edu.duke.oit.idms.oracle.connectors.tasks_email.TasksData;

/**
 * Revoke Provisioned email once grace period has ended.
 * @author Michael Meadows (mm310)
 *
 */
public class RevokeMail extends SchedulerBaseTask {

	private String connectorName = "REVOKE_PROVISIONING";
	private long eDiscoveryKey;
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private TasksData tasksData;
	
	protected void execute() {
		
		tcProvisioningOperationsIntf moProvUtility = null;
		tcUserOperationsIntf moUserUtility = null;
		tcObjectOperationsIntf moObjectUtility = null;
		
		try {
			moProvUtility = (tcProvisioningOperationsIntf) super.getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");
			moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			moObjectUtility = (tcObjectOperationsIntf) super.getUtility("Thor.API.Operations.tcObjectOperationsIntf");
			tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
								super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

			String[] oimResultsAttributes = {"USR_UDF_UID", "USR_UDF_NETIDSTATUS", "USR_UDF_NETIDSTATUSDATE"};
			EmailUsers users = new EmailUsers(connectorName, moProvUtility, moUserUtility, moObjectUtility, moITResourceUtility, oimResultsAttributes);

			tasksData = TasksData.getInstance(connectorName);
			eDiscoveryKey = getEDiscoveryKey();
			if (eDiscoveryKey == -1) {
				logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Could not parse group key for eDiscovery OIM group."));
				return;
			}
			
			HashMap<String, long []> userKeysByResource = getUserKeysToRevoke(users, moUserUtility);
			
			if (userKeysByResource.isEmpty()) {
				logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "No users met criteria for email revocation."));
				
				return;
			}
			
			HashMap<String, Integer> revokeCount = revokeMailForUsers(moUserUtility, userKeysByResource, users.getResourceIterator());
			Iterator<String> resourceIterator = users.getResourceIterator();
			StringBuffer countSB = new StringBuffer();
			int total = 0;
			while (resourceIterator.hasNext()) {
				String resource = resourceIterator.next();
				if (revokeCount.containsKey(resource)) {
					int count = revokeCount.get(resource);
					countSB.append(resource + ":" + count + " ");
					total += count;
				}
			}
			logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "Revoked " + total + " expired mail users: " + countSB));
			
		} catch (ParseException e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Could not retrieve user keys for revocation"), e);
			if (moObjectUtility != null) moObjectUtility.close();
			if (moUserUtility != null) moUserUtility.close();
			if (moProvUtility != null) moProvUtility.close();
			return;
		} catch (tcAPIException e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Count not initialize tcAPI Interfaces needed to instantiate EmailUsers instance."));
			if (moObjectUtility != null) moObjectUtility.close();
			if (moUserUtility != null) moUserUtility.close();
			if (moProvUtility != null) moProvUtility.close();
			return;
		} catch (Exception e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", e.getMessage()), e);
			if (moObjectUtility != null) moObjectUtility.close();
			if (moUserUtility != null) moUserUtility.close();
			if (moProvUtility != null) moProvUtility.close();
			return;
		}
		moObjectUtility.close();
		moUserUtility.close();
		moProvUtility.close();
	}
	
	private HashMap<String, long []> getUserKeysToRevoke(EmailUsers emailUsers, tcUserOperationsIntf moUserUtility)
			throws NumberFormatException, ParseException {

		// process the hashmap of all OIM users that are provisioned the resource
		Set<String> netids = emailUsers.getUsers().keySet();
		Iterator<String> netidsIterator = netids.iterator();
		HashMap<String, HashSet<Long> > resourceUserKeys = new HashMap<String, HashSet<Long> >();
		HashSet<Long> userKeys = new HashSet<Long>();
		while (netidsIterator.hasNext()) {
			String netID = netidsIterator.next();
			if (!emailUsers.getUsers().containsKey(netID))
				continue;
			HashMap<String, String> userMap = emailUsers.getUsers().get(netID);

			if (!userMap.containsKey("USR_UDF_NETIDSTATUS")) {
				logger.info(EmailUsers.addLogEntry(connectorName, "ERROR", "Null value found for NetIDStatus for NetID: " + netID));
				continue;
			}				
			String netIDStatus = userMap.get("USR_UDF_NETIDSTATUS");

			if (!userMap.containsKey("Users.Key")) {
				logger.info(EmailUsers.addLogEntry(connectorName, "ERROR", "Null value found for Users.key for NetID: " + netID));
				continue;
			}
			String userKey = userMap.get("Users.Key");
			
			String netIDStatusDate = null;
			if (!userMap.containsKey("USR_UDF_NETIDSTATUSDATE")) {
				HashMap<String, String> mhSearchCriteria = new HashMap<String, String>();
		    	mhSearchCriteria.put("Users.Key", userKey);
				if (!updateNetIDStatusDate(moUserUtility, mhSearchCriteria)) {
					logger.info(EmailUsers.addLogEntry(connectorName, "ERROR", "NetID:" + netID + " is set to inactive, and NetIDStatusDate cannot be updated, skipping user . . ."));
				}
				continue;
			}
			netIDStatusDate = userMap.get("USR_UDF_NETIDSTATUSDATE");

			if (netIDStatus.equals("inactive") && isGracePeriodExpired(netIDStatusDate) && !eDiscovery(Long.parseLong(userMap.get("Users.Key")))) {
				long key = Long.parseLong(userKey);
				
				Iterator<String> resourceIterator = emailUsers.getResourceIterator();
				while (resourceIterator.hasNext()) {
					String resource = resourceIterator.next();
					String resourceStatus = resource + "_STATUS";
					if (userMap.containsKey(resourceStatus) && !userMap.get(resourceStatus).equals("Revoked")) {
						if (resourceUserKeys.containsKey(resource)) {
							resourceUserKeys.get(resource).add(key);
						} else {
							HashSet<Long> keySet = new HashSet<Long>();
							keySet.add(key);
							resourceUserKeys.put(resource, keySet);
						}
					}
				}
			}
		}

		HashMap<String, long []> resourceUserKeyArrays = new HashMap<String, long []>();
		Iterator<String> resourceIterator = emailUsers.getResourceIterator(); 
		while (resourceIterator.hasNext()) {
			String resource = resourceIterator.next();
			if (resourceUserKeys.containsKey(resource)) {
				long [] keyArray = ArrayUtils.toPrimitive(resourceUserKeys.get(resource).toArray(new Long [userKeys.size()]));
				resourceUserKeyArrays.put(resource, keyArray);
				logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", resource + ":" + keyArray.length));
			}
			
		}
		return resourceUserKeyArrays;
	}
	
	private boolean isGracePeriodExpired(String netIDStatusDate) throws ParseException {
		int gracePeriod = Integer.parseInt(tasksData.getProperty("graceperiod"));
		Calendar today = Calendar.getInstance();
		today.add(Calendar.DATE, -gracePeriod);
		Date deadline = today.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date statusDate = null;
		statusDate = sdf.parse(netIDStatusDate);

		return (statusDate.before(deadline));
	}

	private boolean updateNetIDStatusDate(tcUserOperationsIntf moUserUtility, HashMap<String, String> mhSearchCriteria) {
		Map<String, String> oimAttributes = new HashMap<String,String>();
		tcResultSet results = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String todayString = sdf.format(new Date());
		oimAttributes.put("USR_UDF_NETIDSTATUSDATE", todayString);

    	try {
			results = moUserUtility.findUsers(mhSearchCriteria);
			if (results.getRowCount() != 1) {
				logger.info(EmailUsers.addLogEntry(connectorName, "ERROR", "Retrieved more than one result for OIM query of Users.Key:" + mhSearchCriteria.get("Users.Key")));
				return false;
			}

			moUserUtility.updateUser(results, oimAttributes);

		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public long [] getObjectKeys(tcUserOperationsIntf moUserUtility, long [] userKeys, String resourceName) throws Exception {
		long [] objectKeys = new long [userKeys.length];
		for (int i = 0; i < userKeys.length; ++i) {
			long userKey = userKeys[i];
			tcResultSet results = moUserUtility.getObjects(userKey);
			for (int rowIndex = 0; rowIndex < results.getRowCount(); ++rowIndex) {
				results.goToRow(rowIndex);
				long objectInstanceKey = results.getLongValue("Users-Object Instance For User.Key");
				String [] cols = results.getColumnNames();
				for (int columnIndex = 0; columnIndex < cols.length; ++columnIndex) {
					if (cols[columnIndex].equals(resourceName))
							objectKeys[columnIndex] = objectInstanceKey;
				}
			}
		}
		return objectKeys;
	}
	
	private boolean eDiscovery(long userKey) {
		tcUserOperationsIntf moUserUtility = null;
		
		try {
			moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			tcResultSet results = moUserUtility.getGroups(userKey);

			for (int i = 0; i < results.getRowCount(); ++i) {
				results.goToRow(i);
				EmailUsers.addLogEntry(connectorName, "DEBUG", "results.getLongValue(Groups.Key):" + results.getLongValue("Groups.Key") + " : eDiscoveryKey:" + eDiscoveryKey);
				if (results.getLongValue("Groups.Key") == eDiscoveryKey)
					return true;
			}
		} catch (tcAPIException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Can't retrieve Groups for user: " + userKey + ": " + e.getMessage());
			logger.info(msg);
		} catch (tcUserNotFoundException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "User not found for User Key: " + userKey);
			logger.info(msg);
		} catch (tcColumnNotFoundException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Can't retrieve Groups for user: " + userKey + ": " + e.getMessage());
			logger.info(msg);
		}
		moUserUtility.close();
		return false;
	}
	
	private long getEDiscoveryKey() {
		tcGroupOperationsIntf moGroupUtility = null;
		String groupName = null;
		
		try {
			moGroupUtility = (tcGroupOperationsIntf) super.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
			groupName = tasksData.getProperty("ediscovery");
			Map<String, String> groupAttributes = new HashMap<String, String>();
			groupAttributes.put("Groups.Group Name", groupName);
			tcResultSet eDiscoveryKeySet = moGroupUtility.findGroups(groupAttributes);
			if (eDiscoveryKeySet.getRowCount() != 1) {
				String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Expected 1 result for eDiscovery group name, received: " + eDiscoveryKeySet.getRowCount());
				logger.info(msg);
			}
			eDiscoveryKeySet.goToRow(0);
			moGroupUtility.close();
			return (eDiscoveryKeySet.getLongValue("Groups.Key"));
		} catch (tcAPIException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Could not initialize tcGroupOperationIntf to retrieve eDiscovery Group Key: " + e.getMessage());
			logger.info(msg);
		} catch (tcColumnNotFoundException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Group Key not found for Group, verify name in properties file: " + groupName);
			logger.info(msg);
		}
		moGroupUtility.close();
		return -1;
	}
	
/**
 * Sets the status of all tasks under Resource to Canceled.
 * TODO - for Production, in Design Console under Process Definition, set 'Create User' Task to Object Status Mapping to Revoked for Cancelled
 * 				Also, undo must be set for Create User as well as Delete User
 * @param moUserUtility
 * @param userKeys
 * @param resourceName
 */
	public HashMap<String, Integer> revokeMailForUsers(tcUserOperationsIntf moUserUtility, HashMap<String, long []> resourceUserKeys, Iterator<String> resourceIterator){
		
		HashMap<String, Integer> revokeCounts = new HashMap<String, Integer>();
		while (resourceIterator.hasNext()) {
			String resource = resourceIterator.next();
			if (resourceUserKeys.containsKey(resource)) {
				
				long [] userKeys = resourceUserKeys.get(resource);
				for (int i = 0; i < userKeys.length; ++i) {
					long userKey = userKeys[i];
					tcResultSet results;
					try {
						if (revokeCounts.containsKey(resource)) {
							revokeCounts.put(resource, revokeCounts.get(resource) + 1);
						} else {
							revokeCounts.put(resource, 1);
						}
						results = moUserUtility.getObjects(userKey);
						for (int rowIndex = 0; rowIndex < results.getRowCount(); ++rowIndex) {
							results.goToRow(rowIndex);
							if (results.getStringValue("Objects.Name").equals(resource)) {
								long objectInstanceKey = results.getLongValue("Users-Object Instance For User.Key");
								moUserUtility.revokeObject(userKey, objectInstanceKey);
							}
						}
					} catch (Exception e) {
						logger.info(EmailUsers.addLogEntry(connectorName, "ERROR", "Could not revoke resource: " + resource + " from user key:" + userKey));
					}	
				}
			}
		}
		return revokeCounts;
	}
}
