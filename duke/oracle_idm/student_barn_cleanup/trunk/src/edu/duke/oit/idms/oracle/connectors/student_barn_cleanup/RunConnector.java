package edu.duke.oit.idms.oracle.connectors.student_barn_cleanup;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.HashSet;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;



public class RunConnector extends SchedulerBaseTask {

	  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	  
	  private String connectorName = "STUDENT_BARN_CLEANUP";

	  private tcUserOperationsIntf moUserUtility;
	  	  	  	  
		protected void execute() {
			logger.info(connectorName + ": Starting Task");
			Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
			tcResultSet isPendingPWActivationSet=null,allSet=null;
			HashSet<String> targets = new HashSet<String>();
			String [] getAttributes = {"USR_UDF_UID","Users.User ID","USR_UDF_BARNEXPIRATIONDATE"};
			
			// get a tcUserOperationsIntf object
			  try {
				  moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			  } catch (tcAPIException e) {
				  logger.error(connectorName + ": Unable to instantiate tcUserOperationsIntf");
				  return;
			  }

			  // Perform a search through OIM to find users who meet the selection criteria.
			  // Start by identifying the selection criteria.
			  
			  mhSearchCriteria.clear();
			  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","people");
			  mhSearchCriteria.put("Users.Status", "Active");
			  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
			  mhSearchCriteria.put("USR_UDF_UID", "*");
			  mhSearchCriteria.put("USR_UDF_NETIDSTATUS","pending pw activation");
			  mhSearchCriteria.put("USR_UDF_IS_STUDENTNETID","1");
			  try {
			  isPendingPWActivationSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception getting pending user list - " + e.getMessage());
				  return; // don't continue if you can't find users
			  }
			  
			  mhSearchCriteria.clear();
			  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","people");
			  mhSearchCriteria.put("Users.Status","Active");
			  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
			  mhSearchCriteria.put("USR_UDF_IS_STUDENTNETID","1");
			  mhSearchCriteria.put("USR_UDF_UID","*");
			  try {
				  allSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception getting full user list - " + e.getMessage());
				  return; // don't continue if you can't find users
			  }
			  
			  try {
				  for (int i = 0; i < allSet.getRowCount(); i++) {
					  allSet.goToRow(i);
					  if (allSet.getStringValue("USR_UDF_BARNEXPIRATIONDATE").equals("PROCESSED")) {
						  // already cleaned
						  logger.info(connectorName + ": Skipping " + allSet.getStringValue("Users.User ID") + " because BARNEXPIRATIOINDATE is PROCESSED");
					  } else {
						  targets.add(allSet.getStringValue("Users.User ID"));
					  }
				  }
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception getting Unique IDs from set of all students " + e.getMessage());
				  return;
			  }
			  
			  try {
				  for (int i = 0; i < isPendingPWActivationSet.getRowCount(); i++) {
					  isPendingPWActivationSet.goToRow(i);
					  targets.remove(isPendingPWActivationSet.getStringValue("Users.User ID"));
				  }
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception removing Unique IDs from set of all student Unique IDs " + e.getMessage());
				  return;  // don't process if we don't have the right list of users 
			  }
			  
			  Iterator<String> iter = targets.iterator();
			  while (iter.hasNext()) {
				  String cur = (String) iter.next();
				  HashMap<String,String> setValues = new HashMap<String,String>();
				  setValues.put("USR_UDF_BARNEXPIRATIONDATE", "PROCESSED");
				  mhSearchCriteria.clear();
				  mhSearchCriteria.put("Users.User ID", cur);
				  tcResultSet moResultSet = null;
				  try {
					  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
					  if (moResultSet != null && moResultSet.getRowCount() == 1) {
						  	moUserUtility.updateUser(moResultSet, setValues);
						  	logger.info(connectorName + ": Updated expiration date for " + cur);
					  }
				  } catch (Exception e) {
					  logger.error(connectorName + ": Failed to update expiration value for " + cur + " with " + e.getMessage() + " exception");
					  throw new RuntimeException(e);  // if barn can't be created, rethrow so we'll fail and force manual restart
				  }
			  }
			  logger.info(connectorName + ": Completed run");
		}
			  
}

