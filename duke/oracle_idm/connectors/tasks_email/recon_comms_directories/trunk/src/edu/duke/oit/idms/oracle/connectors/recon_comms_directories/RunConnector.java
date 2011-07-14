package edu.duke.oit.idms.oracle.connectors.recon_comms_directories;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcObjectNotFoundException;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

public class RunConnector extends SchedulerBaseTask {
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);

	//Name of the current connector
	public static String connectorName = "COMMSDIR_RECONCILIATION";
	public static String resourceName = "COMMSDIR_PROVISIONING2";

	@Override
	protected void execute() {
		logger.info(addLogEntry("BEGIN", "Starting reconciliation for resource: " + resourceName));
		
		tcUserOperationsIntf moUserUtility = null;
		try {
			moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		} catch (tcAPIException e1) {
			logger.info(addLogEntry("FAILED", "Could not get instance of tcUserOperationsIntf."));
			return;
		}

		
		HashSet<Long> provisionedUserKeys = null;	
		try {
			provisionedUserKeys = getProvisionedUserKeys();
		} catch (Exception e) {
			logger.info(addLogEntry("FAILED", "Could not retrieve users provisioned with resource: " + resourceName), e);
			return;
		}
		
		HashSet<Long> ACMailboxExistsUserKeys = null;
		try {
			ACMailboxExistsUserKeys = getACMailBoxExistsKeys(moUserUtility);
		} catch (Exception e) {
			logger.info(addLogEntry("FAILED", "Could not retrieve users with duACMailboxExists field set."), e);
			return;
		}

		provisionedUserKeys.removeAll(ACMailboxExistsUserKeys);
		
		int usersUpdated = updateACMailboxExists(moUserUtility, provisionedUserKeys);
		
		logger.info(addLogEntry("SUCCESS", "Updated duACMailboxExists for " + usersUpdated + " users with resource: " + resourceName));
		if (moUserUtility != null) moUserUtility.close();
		return;
	}
	
	
	private int setDuACMailBoxExists(tcUserOperationsIntf utility, long userKey, String value) throws tcAPIException, 
			tcUserNotFoundException, tcStaleDataUpdateException {
		String [] returnAttributes = {"Users.Key"};
		HashMap<String, String> filterAttributes = new HashMap<String, String>();
		filterAttributes.put("Users.Status", "Active");
		filterAttributes.put("Users.Key", String.valueOf(userKey));
		tcResultSet user = utility.findUsersFiltered(filterAttributes, returnAttributes);
		if (user.getRowCount() == 1) {
			HashMap<String, String> userValues = new HashMap<String, String>();
			userValues.put("USR_UDF_ACMAILBOXEXISTS", value);
			utility.updateUser(user, userValues);
			logger.info(addLogEntry("INFO", "Finished setDuACMailBoxExists for user: " + userKey));
			return 1;
		} else {
			return 0;
		}
	}
	
	/**
	 * If user is provisioned and does not have duACMailBoxExists set, then set it
	 * @param moUserUtility
	 * @param provisionedKeys
	 * @param mboxExistsKeys
	 * @return
	 */
	private int updateACMailboxExists(tcUserOperationsIntf moUserUtility, HashSet<Long> keys) {
		int count = 0;
		
		Iterator<Long> keyIterator = keys.iterator();
		while (keyIterator.hasNext()) {
			Long provisionedKey = keyIterator.next();
			try {
				 count += setDuACMailBoxExists(moUserUtility, provisionedKey, "1");
			} catch (Exception e) {
				logger.info(addLogEntry("WARN", "Could not set duACMailboxExists for User Key:" + provisionedKey), e);
			}
		}
		
		return count;
	}
	
	/**
	 * Retrieve User Keys for Users with duACMailboxExists set (equal "1")
	 * @return
	 * @throws tcAPIException
	 * @throws tcColumnNotFoundException 
	 */
	private HashSet<Long> getACMailBoxExistsKeys(tcUserOperationsIntf moUserUtility) throws tcAPIException, tcColumnNotFoundException {
		HashSet<Long> userKeys = new HashSet<Long>();
		String [] returnAttributes = {"Users.Key"};
		HashMap<String, String> filterAttributes = new HashMap<String, String>();
		filterAttributes.put("Users.Status", "Active");
		filterAttributes.put("USR_UDF_ACMAILBOXEXISTS", "1");

		tcResultSet users = moUserUtility.findUsersFiltered(filterAttributes, returnAttributes);
		for (int i = 0; i < users.getRowCount(); ++i) {
			users.goToRow(i);
			userKeys.add(users.getLongValue("Users.Key"));
		}
		
		return userKeys;
	}

	/**
	 * Retrieve User Keys for Users with the resource provisioned in OIM
	 * @return
	 * @throws tcAPIException
	 * @throws tcColumnNotFoundException
	 * @throws tcObjectNotFoundException
	 */
	public HashSet<Long> getProvisionedUserKeys() throws tcAPIException, tcColumnNotFoundException, tcObjectNotFoundException {
		tcObjectOperationsIntf moObjectUtility = (tcObjectOperationsIntf) super.getUtility("Thor.API.Operations.tcObjectOperationsIntf");
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("Objects.Name", resourceName);
		tcResultSet objectKeySet = moObjectUtility.findObjects(attributes);
		HashSet<Long> userKeys = null;
		if (objectKeySet.getRowCount() == 1) {
			long objectKey = objectKeySet.getLongValue("Objects.Key");
			attributes.clear();
			attributes.put("Objects.Object Status.Status", "Provisioned");
			tcResultSet users = moObjectUtility.getAssociatedUsers(objectKey, attributes);
			userKeys = new HashSet<Long>();
			for (int i = 0; i < users.getRowCount(); ++i) {
				users.goToRow(i);
				userKeys.add(users.getLongValue("Users.Key"));
			}
		}
		moObjectUtility.close();
		return userKeys;
	}
	
	/**
	 * Formats and returns string for consistent message logging
	 * @param status
	 * @param message
	 * @return
	 */
	public static String addLogEntry(String status, String message) {
		return (connectorName + ": " + status + " - " + message);
	}
	
	public void dumpTcResultSet(tcResultSet results) throws Exception {
		String [] columnNames = results.getColumnNames();
		for (int i = 0; i < results.getRowCount(); ++i) {
			StringBuffer logEntry = new StringBuffer();
			for (int z = 0; z < columnNames.length; ++z)
				logEntry.append("|" + columnNames[z] + ": " + results.getStringValue(columnNames[z]));
			logger.info(addLogEntry("DEBUG", "tcResultSet:" + logEntry));
		}
	}
}
