package edu.duke.oit.idms.oracle.connectors.tasks_email;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

import Thor.API.tcResultSet;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

public class EmailUsers {

	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private HashSet<String> resourceNames;
	private HashMap<String, HashMap<String, String> > users;
	private long objectKey;
	private Connection conn;
	private PreparedStatement psProcessInstanceKeyTasks = null;

	/**
	 * Retrieves a list of users and specified user attributes from OIM for the specified resource. The following defaults are retrieved as well: netID, dukeID, the 
	 * 		current status of the resource (either Revoked or Provisioned), and the most recent status change of the resource
	 * @param moProvUtility
	 * @param moUserUtility
	 * @param moObjectUtility
	 * @param moITResourceUtility 
	 * @param resourceName
	 * @param partialResultsAttributes
	 * @throws Exception
	 */
	public EmailUsers(String connectorName, tcProvisioningOperationsIntf moProvUtility, tcUserOperationsIntf moUserUtility,
			tcObjectOperationsIntf moObjectUtility, tcITResourceInstanceOperationsIntf moITResourceUtility, String [] partialResultsAttributes) {

		logger.info(addLogEntry(connectorName, "BEGIN", "Start of EmailUsers HashMap generation"));			
		resourceNames = new HashSet<String>();
		resourceNames.add("COMMSDIR_PROVISIONING2");
		resourceNames.add("EXCHANGE_PROVISIONING");
		HashMap<String, tcResultSet> usersByResource = new HashMap<String, tcResultSet>();

		Iterator<String> resourceNameIterator = resourceNames.iterator();
		while (resourceNameIterator.hasNext()) {
			String resourceName = resourceNameIterator.next();
			HashMap<String, String> attributes = new HashMap<String, String>();
			attributes.put("Objects.Name", resourceName);

			try {
				tcResultSet objectKeySet = moObjectUtility.findObjects(attributes);
				objectKey = objectKeySet.getLongValue("Objects.Key");
				tcResultSet objectUserSet = moObjectUtility.getAssociatedUsers(objectKey, new HashMap<String, String>());
				usersByResource.put(resourceName, objectUserSet);
			} catch (Exception e) {
				throw new RuntimeException(addLogEntry(connectorName, "ERROR", "Could not retrieve EmailUsers with resource: " + resourceName + " from OIM."), e);
			}
		}

		try {
			createDatabaseConnection(moITResourceUtility);
		} catch (Exception e) {
			throw new RuntimeException(addLogEntry(connectorName, "ERROR", "Could not connect to OIM Database to retrieve object info for EmailUsers."), e);
		}			
		
		HashMap<String, HashMap<String,String> > usersByUserID = null;
		try {	
			usersByUserID = getUsersByUserID(connectorName, usersByResource);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			if (psProcessInstanceKeyTasks != null) {
				try {
					psProcessInstanceKeyTasks.close();
				} catch (SQLException e) {
					// ignore
				}
			}

			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}

		try {
			users = getEmailUsers(connectorName, moUserUtility, usersByUserID, partialResultsAttributes);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		logger.info(addLogEntry(connectorName, "INFO", "Returning " + users.size() + " active users."));
	}

	private HashMap<String, HashMap<String,String> > getUsersByUserID (String connectorName, HashMap<String, tcResultSet> usersByResource) throws Exception {
		
		HashMap<String, HashMap<String,String> > usersByUserID = new HashMap<String, HashMap<String,String> >();
		Iterator<String> resourceNameIterator = resourceNames.iterator();
		while (resourceNameIterator.hasNext()) {
			String resourceName = resourceNameIterator.next();
			String userID = "";
			tcResultSet objectUserSet = usersByResource.get(resourceName);

			try {
				if (objectUserSet == null || objectUserSet.getRowCount() == 0) {
					logger.info(addLogEntry(connectorName, "WARN", "No users found with resource: " + resourceName));
					continue;
				}

				for (int i = 0; i < objectUserSet.getRowCount(); ++i) {
					objectUserSet.goToRow(i);
					userID = objectUserSet.getStringValue("Users.User ID");				
					String objectStatus = objectUserSet.getStringValue("Objects.Object Status.Status");

					if (objectStatus.equals("Provisioned") || objectStatus.equals("Revoked")) {
						long processKey = Long.parseLong(objectUserSet.getStringValue("Process Instance.Key"));
						Date lastDate = getTaskSuccessDateUsingSql(processKey);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
						String lastDateString = null;

						if (lastDate != null)
							lastDateString = sdf.format(lastDate);
						HashMap<String, String> userMap = new HashMap<String, String>();
						if (!(userID == null || userID.equals("")))
							userMap.put("Users.User ID", userID);

						String resourceStatus = "";
						if (!(objectStatus == null || objectStatus.equals(""))) {
							resourceStatus = resourceName + "_STATUS";
							userMap.put(resourceStatus, objectStatus);
						} else {
							continue;
						}

						String resourceDate = "";
						if (!(lastDateString == null || lastDateString.equals(""))) {
							resourceDate = resourceStatus + "_DATE";
							userMap.put(resourceDate, lastDateString);
						} else {
							continue;
						}

						if (!usersByUserID.containsKey(userID)) {
							usersByUserID.put(userID, userMap);
						} else {
							if (!usersByUserID.get(userID).containsKey(resourceDate) ||
									userMap.get(resourceDate).compareTo(usersByUserID.get(userID).get(resourceDate)) > 0)
								usersByUserID.get(userID).putAll(userMap);
						}
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException(addLogEntry(connectorName, "ERROR", "Could not retrieve object status date for resource: " + resourceName + " for EmailUser with ID: " + userID
						+ " from OIM database."), e);
			} catch (Exception e) {
				String exceptionName = e.getClass().getSimpleName();
				if (exceptionName.contains("tcAPIException") || exceptionName.contains("tcColumnNotFoundException")) {
					throw new RuntimeException(addLogEntry(connectorName, "ERROR", "Could not retrieve object status for resource: " + resourceName + " for EmailUser with ID: " + userID), e);
				} else {
					logger.info(addLogEntry(connectorName, "DEBUG", "Unhandled exception caught: " + e.getClass().getCanonicalName() + ":" + e.getMessage()));
				}
			}
		}
		return usersByUserID;
	}

	private HashMap<String, HashMap<String,String> > getEmailUsers
			(String connectorName, tcUserOperationsIntf moUserUtility, HashMap<String, HashMap<String,String> > usersByUserID, String [] partialResultsAttributes) throws Exception {
		
		HashMap<String, HashMap<String,String> > emailUsers = new HashMap<String, HashMap<String,String> >();
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("Users.Status", "Active");
		String [] oimResultsAttributes = new String[partialResultsAttributes.length + 2];
		int j = 0;
		for ( ; j < partialResultsAttributes.length; ++j)
			oimResultsAttributes[j] = partialResultsAttributes[j];
		oimResultsAttributes[j] = "USR_UDF_UID";
		oimResultsAttributes[++j] = "Users.User ID";
		tcResultSet userResultSet;
		try {
			userResultSet = moUserUtility.findUsersFiltered(attributes, oimResultsAttributes);
			for (int i = 0; i < userResultSet.getRowCount(); ++i) {
				userResultSet.goToRow(i);
				String userID = userResultSet.getStringValue("Users.User ID");
				HashMap<String, String> userWithResource = usersByUserID.remove(userID);

				if (userWithResource == null)
					continue;

				String [] columnNames = userResultSet.getColumnNames();
				for (int k = 0; k < columnNames.length; ++k) {
					String key = columnNames[k];
					String value = userResultSet.getStringValue(key);
					if (!key.equals("Users.Row Version"))
						if (!(value == null || value.equals("")))
							userWithResource.put(key, value);
				}

				if (userWithResource.containsKey("USR_UDF_UID")) {
					String netid = userWithResource.get("USR_UDF_UID");
					emailUsers.put(netid, userWithResource);
				}
			}
		} catch (Exception e) {
			String exceptionName = e.getClass().getSimpleName();
			if (exceptionName.contains("tcAPIException") || exceptionName.contains("tcColumnNotFoundException")) {
				throw new RuntimeException(addLogEntry(connectorName, "ERROR", "Could not retrieve set of all active users from OIM"), e);
			} else {
				logger.info(addLogEntry(connectorName, "DEBUG", "Unhandled exception caught: " + e.getClass().getCanonicalName() + ":" + e.getMessage()), e);
			}
		}
		return emailUsers;
	}

	private Date getTaskSuccessDateUsingSql(long processKey) throws SQLException {

		ResultSet rs = null;
		Date latest = null;

		try {
			psProcessInstanceKeyTasks.setLong(1, processKey);
			rs = psProcessInstanceKeyTasks.executeQuery();
			while (rs.next()) {
				Date actualEnd = rs.getTimestamp("actual_end_date");
				if (latest == null || actualEnd.compareTo(latest) > 0) {
					latest = actualEnd;
				}
			}

		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
		return latest;
	}

	public HashMap<String, HashMap<String, String> > getUsers () {
		return users;
	}

	public long getObjectKey() {
		return objectKey;
	}
	
	public Iterator<String> getResourceIterator() {
		return Collections.unmodifiableSet(resourceNames).iterator();
	}

	public void dumpTcResultSet(String connectorName, tcResultSet results) throws Exception {
		String [] columnNames = results.getColumnNames();
		for (int i = 0; i < results.getRowCount(); ++i) {
			StringBuffer logEntry = new StringBuffer();
			for (int z = 0; z < columnNames.length; ++z)
				logEntry.append("|" + columnNames[z] + ": " + results.getStringValue(columnNames[z]));
			logger.info(addLogEntry(connectorName, "DEBUG", "tcResultSet:" + logEntry));
		}
	}

	public void dumpEmailUsers(String connectorName) {
		
		logger.info(addLogEntry(connectorName, "DEBUG", "EmailUsers contains " + users.size() + " total users."));
		Set<String> netids = users.keySet();
		Iterator<String> netidIterator = netids.iterator();
		HashMap<String, Integer> count = new HashMap<String, Integer>();
		while (netidIterator.hasNext()) {
			String netid = netidIterator.next();
			HashMap<String, String> user = users.get(netid);
			Iterator<String> resourceNameIterator = resourceNames.iterator();
			while (resourceNameIterator.hasNext()) {
				String resourceName = resourceNameIterator.next();
				String resourceStatus = resourceName + "_STATUS";
				if (user.containsKey(resourceStatus)) {
					String status = user.get(resourceStatus);
					String resourceKey = resourceName + "_" + status;
					if (count.containsKey(resourceKey)) {
						count.put(resourceKey, count.get(resourceKey) + 1);
					} else {
						count.put(resourceKey, 1);
					}
				}
			}
			logger.info(addLogEntry(connectorName, "DEBUG", user.toString()));
		}
		Iterator<String> countKeyIterator = count.keySet().iterator();
		while (countKeyIterator.hasNext()) {
			String countName = countKeyIterator.next();
			logger.info(addLogEntry(connectorName, "DEBUG", countName + ":" + count.get(countName)));
		}
	}

	private void createDatabaseConnection(tcITResourceInstanceOperationsIntf moITResourceUtility) throws Exception {

		Map<String, String> parameters = new HashMap<String, String>();
		Map<String, String> resourceMap = new HashMap<String, String>();
		resourceMap.put("IT Resources.Name", "OIM_DATABASE");
		tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
		long resourceKey = moResultSet.getLongValue("IT Resources.Key");

		moResultSet = null;
		moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
		for (int i = 0; i < moResultSet.getRowCount(); i++) {
			moResultSet.goToRow(i);
			String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
			String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
			parameters.put(name, value);
		}

		Class.forName(parameters.get("driver"));

		Properties props = new Properties();
		props.put("user", parameters.get("username"));
		props.put("password", parameters.get("password"));
		if (parameters.get("connectionProperties") != null && !parameters.get("connectionProperties").equals("")) {
			String[] additionalPropsArray = ((String)parameters.get("connectionProperties")).split(",");
			for (int i = 0; i < additionalPropsArray.length; i++) {
				String[] keyValue = additionalPropsArray[i].split("=");
				props.setProperty(keyValue[0], keyValue[1]);
			}
		}
		conn = DriverManager.getConnection(parameters.get("url"), props);
		psProcessInstanceKeyTasks = conn.prepareStatement("select actual_end_date from process_instance_key_tasks_v where status='C' and (task_name = 'Create User' or task_name='Change uid Attribute' or task_name = 'Delete User') and process_instance_key = ?");
	}

	/**
	 * Ensure consistent log format. For each log entry, include the name of the connector, the type of message, and the message text
	 * @param status
	 * @param message
	 * @return
	 */

	public static String addLogEntry(String connector, String status, String message) {
		return (connector + ": " + status + " - " + message);
	}
}
