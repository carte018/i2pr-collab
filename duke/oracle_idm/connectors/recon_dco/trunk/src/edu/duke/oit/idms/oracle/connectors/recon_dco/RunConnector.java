package edu.duke.oit.idms.oracle.connectors.recon_dco;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;
import java.util.Hashtable;
import edu.duke.oit.idms.oracle.util.AttributeData;
import java.io.*;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.exceptions.OIMUserNotFoundException;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Exceptions.tcUserNotFoundException;



/**
 * This is a task that queries an event log in the Grouper database and uses
 * that to update the group memberships in OIM.
 * @author shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  /*
   * Some information about the input file we'll process
   * This will all be overridden by atain the DCO_RECONCILIATION 
   * object in OIM, but we need defaults just in case.
   */
  // Server where the input file comes to us from
  private String sourceServer = "howes.oit.duke.edu";
  // Source file name on sourceServer
  private String sourceFile = "/servers/idms/work/dirxml/dukecard/dukecard.dat";
  // SSH key used to transfer files from sourceServer
  private String transferKeyFile = "/export/home/oracle/howes-access";
  // SSH user on sourceServer as whom to transfer file
  private String transferUser = "root";
  // Location on local machine of target file
  private String destFile = "/export/home/oracle/dco";

  //Name of the current connector
  
  private String connectorName = "DCO_RECONCILIATION";

  private tcUserOperationsIntf moUserUtility;

  // Some data stream objects
  
  File targetFile = null;
  FileInputStream fstream = null;
  Process proc = null;
  
  // and the central HashMap
  
  HashMap hashMap = null;
  
  // and the OIM central HashMap
  HashMap oimMap = null;

  Map dukeidToUserKeyCache = null;
  Map groupNameToGroupKeyCache = new HashMap();
  
  /** 
   * Initialize the file parameters from the IT Resource in OIM,
   * acquire a copy of the source file, and then load it into a 
   * HashMap indexing uniqueID values against DukeCardNumber values
   * and return the HashMap.
   */
  
  private HashMap getInFileData() throws Exception {
	    Map parameters = new HashMap();
	    HashMap retval = new HashMap();

	    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
	    super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

	    Map resourceMap = new HashMap();
	    resourceMap.put("IT Resources.Name", "DCO_RECONCILIATION");
	    tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
	    long resourceKey = moResultSet.getLongValue("IT Resources.Key");

	    moResultSet = null;
	    moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
	    for (int i = 0; i < moResultSet.getRowCount(); i++) {
	      moResultSet.goToRow(i);
	      String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
	      String value = moResultSet
	      .getStringValue("IT Resources Type Parameter Value.Value");
	      parameters.put(name, value);
	    }

	    if (parameters.containsKey("sourceServer")) {
	    	sourceServer = (String)parameters.get("sourceServer");
	    } 
	    if (parameters.containsKey("sourceFile")) {
	    	sourceFile = (String)parameters.get("sourceFile");
	    }
	    if (parameters.containsKey("transferKeyFile")) {
	    	transferKeyFile = (String)parameters.get("transferKeyFile");
	    }
	    if (parameters.containsKey("transferUser")) {
	    	transferUser = (String)parameters.get("transferUser");
	    }
	    if (parameters.containsKey("destFile")) {
	    	destFile = (String)parameters.get("destFile");
	    }
	    
	    try {
	    	String cmds[] = {"/usr/bin/scp","-i",transferKeyFile,transferUser + "@" + sourceServer + ":" + sourceFile,destFile};
	    	//proc = Runtime.getRuntime().exec("/usr/bin/scp -i " + transferKeyFile + " " + transferUser + "@" + sourceServer + ":" + sourceFile + " " + destFile);
	    	proc = Runtime.getRuntime().exec(cmds);
	    	proc.waitFor();
	    	} catch (Exception e) {
	    	throw new RuntimeException("Exception in scp: " + e.getMessage(),e);
	    	}
	    	
	    targetFile = new File(destFile);

	    // At this point, we either successfully retrieved the file or we didn't, but we can begin the process
	    // of loading up data from the file anyway and see where we land.
	    //
	    int LinesInFile = 0;
	    try {
	    	fstream = new FileInputStream(destFile);
	    } catch (Exception e) {
	    	// This is OK, but we'll need to log the problem
	    	// and bail out gracefully
	    	logger.warn(connectorName + ": Input file not found -- no update from DCO this hour");
	    	return(retval);
	    }
	    DataInputStream input = new DataInputStream(fstream);
	    
	    BufferedReader file = new BufferedReader(new InputStreamReader(input));
	    String s = null;
	    String uniqueID = null;
	    
	    while ((s = file.readLine()) != null) {
	    	if (s.matches("TRAILER.*")) {
	    		// This is the trailer line -- parse it and validate the file, throwing an exception
	    		// if the file fails to check out properly.
	    		try {
	    			int count = Integer.parseInt(s.substring(21));
	    			if (count != LinesInFile) {
	    				throw new RuntimeException(connectorName + ": Input file failed validation");
	    			}
	    		} catch (Exception e) {
	    			throw new RuntimeException(connectorName + ": Failure of input file validation" + e.getMessage(),e);
	    		}
	    		// Otherwise, return what we have
	    		return retval;
	    	}
	    	// Otherwise, this is a data line we need to add to the hashmap
	    	LinesInFile = LinesInFile + 1;  // increment line count
	    	try {
	    		retval.put(s.substring(73,80),s.substring(0,9) + s.substring(20,21));
	    	} catch (Exception e) {
	    		throw new RuntimeException(connectorName + ": Failed to load input file: " + e.getMessage(),e);
	    	}
	    }
	    // If we get here, we should return null (but we won't)
	    return null;
  }
  
  // Return a hashmap containing the DukeID and DukeCardNumber of all users in OIM.  
  // We use this for the big comparison that leads to updates of DukeCards in OIM by the reconciler.
  private HashMap getAllOIMUsers() {
	  // Need code here
	  Hashtable mhSearchCriteria = new Hashtable();
	  AttributeData attributeData = AttributeData.getInstance();
	  mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"),"*");
	  mhSearchCriteria.put("Users.Status","Active");
	  tcResultSet moResultSet;
	  String[] getFields = {"USR_UDF_DUKECARDNBR","Users.User ID","USR_UDF_HAS_DUKECARD"};
	  HashMap retval = new HashMap();
	  try {
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
		  if (moResultSet.getRowCount() == 0) {
			  throw new RuntimeException(connectorName + ": No results from dukeID search");
		  } 
	  } catch (tcAPIException e) {
			  throw new RuntimeException(e);
	  }
	  try {
	  for (int i=0; i < moResultSet.getRowCount(); i++) {
		  moResultSet.goToRow(i);
		  String id = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeID"));
		  String n = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeCardNbr"));
		  if (n != null) {
			  retval.put(id,n);
		  } else {
			  retval.put(id,"none");
		  }
	  }
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
	  return retval;
  }
 
  
  private boolean setDukeCard(String dukeID,String cardNumber) throws Exception {
	  // Need code here
	  tcResultSet moResultSet;
	  Hashtable mhSearchCriteria = new Hashtable();
	  AttributeData attributeData = AttributeData.getInstance();
	  String[] getFields = {"USR_UDF_DUKECARDNBR","Users.User ID","USR_UDF_HAS_DUKECARD"};


	  mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"),dukeID);
	  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
	  if (moResultSet.isEmpty() || moResultSet.getRowCount() > 1) {
		  // If the user doesn't exist, or if there are too many of them, simply return true
		  return true;
	  }
	
	  // Otherwise, we need to set the actual value
	  HashMap setValues = new HashMap<String,String>();

	  try {
		  setValues.put(attributeData.getOIMAttributeName("duDukeCardNbr"),cardNumber);
		  setValues.put("USR_UDF_HAS_DUKECARD","1");
		  moUserUtility.updateUser(moResultSet,setValues);
	  } catch (tcStaleDataUpdateException e) {
		  // As in other reconcilers -- if we get this exception it may mean that we've collided
		  // with another update on the same user and need to fall back and retry.
		  // 
		  logger.warn(connectorName + ": Received stale update exception while updating " + dukeID + " Retrying once.");
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
		  moUserUtility.updateUser(moResultSet,setValues);
	  }
	  logger.info(connectorName + ": Updated " + dukeID + " to " + cardNumber);
	  return true;
  }
  
  private boolean unSetDukeCard(String dukeID) throws Exception {
	  // Need code here
	  tcResultSet moResultSet;
	  Hashtable mhSearchCriteria = new Hashtable();
	  AttributeData attributeData = AttributeData.getInstance();
	  String[] getFields = {"USR_UDF_DUKECARDNBR","Users.User ID","USR_UDF_HAS_DUKECARD"};


	  mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"),dukeID);
	  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
	  if (moResultSet.isEmpty() || moResultSet.getRowCount() > 1) {
		  // If the user doesn't exist, or if there are too many of them, simply return true
		  return true;
	  }
	
	  // Otherwise, we need to unset the actual values
	  HashMap setValues = new HashMap<String,String>();

	  try {
		  setValues.put(attributeData.getOIMAttributeName("duDukeCardNbr"),"");
		  setValues.put("USR_UDF_HAS_DUKECARD","0");
		  moUserUtility.updateUser(moResultSet,setValues);
	  } catch (tcStaleDataUpdateException e) {
		  // As in other reconcilers -- if we get this exception it may mean that we've collided
		  // with another update on the same user and need to fall back and retry.
		  // 
		  logger.warn(connectorName + ": Received stale update exception while updating " + dukeID + " Retrying once.");
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
		  moUserUtility.updateUser(moResultSet,setValues);
	  }
	  logger.info(connectorName + ": Removed card for " + dukeID);
	  return true;
  }
  
  private boolean hasDukeCard(String dukeID) throws Exception {
	  // Need code here
	  tcResultSet moResultSet;
	  Hashtable mhSearchCriteria = new Hashtable();
	  AttributeData attributeData = AttributeData.getInstance();
	  String[] getFields = {"USR_UDF_DUKECARDNBR","Users.User ID","USR_UDF_HAS_DUKECARD"};


	  mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"),dukeID);
	  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
	  if (moResultSet.isEmpty() || moResultSet.getRowCount() > 1) {
		  // If the user doesn't exist, or if there are too many of them, simply return false
		  return false;
	  }
	
	  // Otherwise, we need to check the actual value
	  try {
		  if (moResultSet.getStringValue("USR_UDF_HAS_DUKECARD").equals("1")) {
			  return true;
		  } else {
			  return false;
		  }
	
	  } catch (Exception e) {
		  return false;
	  }
  }
  
  private void cleanfile() {
	  try {
	    	targetFile.renameTo(new File("/tmp/oldDCO"));
	    } catch (Exception e) {
	    	logger.warn(connectorName + ": Failed to rename to /tmp/oldDCO -- continuing");
	    }
  }

  protected void execute() {

    logger.info(connectorName + ": Starting task.");
    int changeCount = 0;
    int hashCount = 0;
    
    try {
    	if ((hashMap = getInFileData()) != null) {
    		// logger.info(connectorName + ": Loaded infile");
    	} else {
    		logger.error(connectorName + ": Failed to load input file");
    		//throw new RuntimeException(connectorName + ": No input file loaded");
    		return;  // if nothing to do, do nothing
    	}
    } catch (Exception e) {
    	logger.error(connectorName + ": Exception retrieving master file -- bailing out");
    	return;
    }
    
    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf");
      return;
    }
    // Get information about all the users in the database
    try {
    	oimMap = getAllOIMUsers();
    } catch (Exception e) {
    	// If we throw an exception here, pass it up the chain
    	throw new RuntimeException(e);
    }
    
    // At this point, we have two hashes -- one containing the dukecardnbr values from 
    // the dukecard system and one containing the users we have in OIM with their existing card
    // numbers.  
    // 
    // We walk the list of users in the DCO list, comparing the card numbers to those in the 
    // OIM database.  Where they don't match, we call setDukeCard() to update the value in 
    // OIM and report to the log.  Where they match, we do nothing (and for testing, report to
    // the log.
    //
    // This all goes in a try/catch block because the setDukeCard() method throws exceptions.
    //
    String oimKey = null;
    
    	Iterator dcoIter = hashMap.keySet().iterator();
    	try {
    	while (dcoIter.hasNext()) {
    		// for each key in the dco hashmap
    		String dcoKey = (String) dcoIter.next();
    		String dcoCard = (String) hashMap.get(dcoKey);
    		hashCount += 1;  // in case the hash table is empty
    		if (oimMap.containsKey(dcoKey)) {
    			// The OIM database has such a user
    			if (! oimMap.get(dcoKey).equals(dcoCard)) {
    				// logger.info(connectorName + ": Calling setDukeCard on " + dcoKey);
    				setDukeCard(dcoKey,dcoCard);
    				changeCount += 1;
    				// logger.info(connectorName + ": Called setDukeCard on " + dcoKey);
    			} else {
    				// testing for now
    				// logger.info(connectorName + ": Skipping " + dcoKey + " because value matches");
    			}
    			// And remove the key from the oimMAP
    			oimMap.remove(dcoKey);
    		} else {
    			// testing for now
    			// logger.info(connectorName + ": Skipping " + dcoKey + " because user not in OIM");
    		}
    		if (changeCount > 1000) {
    			throw new RuntimeException("Exiting -- more than 1000 changes processed in a single run -- if this is expected, please run the reconcilation manually");
    		}
    	}
    	
    	Iterator oimIter = oimMap.keySet().iterator();
    	
    	int countok = -1;
    	int countskip = -1;
    	int countmiss = -1;
    	
    	while (oimIter.hasNext() && hashCount > 0) {
    		// for each key in the oimMap hashmap, verify that 
    		// there is no matching key in the dcoMap (which there
    		// should not be at this point) and if the user has a 
    		// card number, remove it.
    		oimKey = (String) oimIter.next();
    		if (hashMap.containsKey(oimKey)) {
    			// there must be some misunderstanding
    			countmiss += 1;
    		} else {
    			if (oimMap.get(oimKey).equals("") || oimMap.get(oimKey).equals("none")) {
    				// there isn't anything to do here
    				countskip += 1;
    			} else {
    				// remove the value
    				unSetDukeCard(oimKey);
    				countok += 1;
    				changeCount += 1;
    			}
    		}
    		// if (countok % 10000 == 0 || countskip % 10000 == 0 || countmiss % 10000 == 0) {
    		//	logger.info(connectorName + ": Processed " + countok + " changes, " + countskip + " skips and " + countmiss + " misses");
    		// }
    		if (changeCount > 1000) {
    			throw new RuntimeException("Exiting -- more than 1000 changes processed in a single update -- if this is expected, please run reconciliation manually");
    		}
    	}
    	} catch (tcUserNotFoundException n) {
    	    	// This is OK -- log a warning and move on
    	    	logger.warn(connectorName + ": Received user not found exception for user " + oimKey);
    	} catch (Exception e) {
    	    	throw new RuntimeException(e);
    	}
 
 
    
    // Now clean up the file by renaming it
    cleanfile();
    
    // And log our end
    
    logger.info(connectorName + ": Ending task.");
  }

}