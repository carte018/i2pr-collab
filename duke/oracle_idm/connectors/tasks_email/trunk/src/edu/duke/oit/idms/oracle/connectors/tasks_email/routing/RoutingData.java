package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;
import edu.duke.oit.idms.oracle.connectors.tasks_email.TasksData;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcGroupNotFoundException;
import Thor.API.Operations.tcGroupOperationsIntf;

public class RoutingData {
	
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private TasksData tasksData;
	private DepartmentRestrictionsData deptRestrictionsData;
	private HashSet<Long> groupMembers;

	/**
	 * @param resourceName
	 * @param moProvUtility
	 * @param moUserUtility
	 * @param moObjectUtility
	 * @param moITResourceUtility
	 * @throws Exception
	 */
	public RoutingData (String connectorName, String resourceName, tcGroupOperationsIntf moGroupUtility) throws Exception {
		
		if (resourceName.equals("") || resourceName == null) {
			String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Can't instantiate RoutingData with null resourceName value");
			logger.info(msg);
		} else {
			tasksData = TasksData.getInstance(resourceName);		
			deptRestrictionsData = DepartmentRestrictionsData.getInstance();
			String groupName = tasksData.getProperty("group");
			groupMembers = addGroupMembers(connectorName, groupName, moGroupUtility);
			if (groupMembers == null || groupMembers.isEmpty()) {
				throw new RuntimeException(EmailUsers.addLogEntry(connectorName, "WARNING", "Could not populate group for: " + groupName));
			}
		}
	}
	
	private HashSet<Long> addGroupMembers(String connectorName, String groupName, tcGroupOperationsIntf moGroupUtility) {
		HashSet<Long> userKeys = new HashSet<Long>();
		try {
			Map<String, String> groupAttributes = new HashMap<String, String>();
			groupAttributes.put("Groups.Group Name", groupName);
			tcResultSet groupKeySet = moGroupUtility.findGroups(groupAttributes);
			if (groupKeySet.getRowCount() != 1) {
				logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", "Expected 1 result for eDiscovery group name, received: " + groupKeySet.getRowCount()));
			}
			groupKeySet.goToRow(0);
			long groupKey =  (groupKeySet.getLongValue("Groups.Key"));
			tcResultSet memberUsers = moGroupUtility.getAllMemberUsers(groupKey);
			for (int i = 0; i < memberUsers.getRowCount(); ++i) {
				memberUsers.goToRow(i);
				userKeys.add(memberUsers.getLongValue("Users.Key"));				
			}
		} catch (tcAPIException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Could not initialize tcGroupOperationIntf to retrieve eDiscovery Group Key: " + e.getMessage());
			logger.info(msg);
		} catch (tcColumnNotFoundException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Group Key not found for Group, verify name in properties file: " + groupName);
			logger.info(msg);
		} catch (tcGroupNotFoundException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "WARNING", "Group Key not found for Group, verify name in properties file: " + groupName);
			logger.info(msg);
		}
		return userKeys;
	}
	
	public TasksData getTasksData() {
		return tasksData;
	}
		
	public DepartmentRestrictions getDeptRestrictionsClass(String dept) throws InstantiationException, IllegalAccessException {
		return deptRestrictionsData.getDeptRestrictionsClass(dept);
	}
	
	public DepartmentRestrictionsData getRestrictionsData() {
		return deptRestrictionsData;
	}
	
	public HashSet<Long> getGroupMembers() {
		return groupMembers;
	}
}
