package edu.duke.oit.idms.oracle.connectors.recon_comms;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.Set;
import edu.duke.oit.idms.oracle.util.AttributeData;
import java.io.*;

import Thor.API.tcResultSet;
import com.thortech.xl.dataaccess.tcDataProvider;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.TaskDefinitionOperationsIntf;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.SortControl;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;


import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.exceptions.OIMUserNotFoundException;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Exceptions.tcColumnNotFoundException;



/**
 * This is a reconcilation connector that handles reconciling 
 * mailbox statuses in the comms directories with duACMailboxExists
 * attributes in the OIM and elsewhere.  We essentially synchronize 
 * the comms directories' mailUserStatus values to OIM so that if 
 * a mailUserStatus value exists and is not equal to "removed" in the 
 * comms directory, we ensure the duACMailboxExists value in OIM is
 * set to true, and likewise, if the comms directory has no 
 * entry, has no mailUserStatus, or has a mailUserStatus of "removed",
 * we ensure that the duACMailboxExists value in OIM is false.
 * 
 * On transitions from true to false, we trigger the comms
 * provisioner's revoke method to set the OIM user object into an 
 * unprovisioned state, so that subsequent provisioning events will 
 * fire if the user ever returns and is re-provisioned for a new 
 * mailbox.  Otherwise, once the user leaves, any future return 
 * would leave the user unable to be re-provisioned.
 * @author rob
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
 
  //Name of the current connector
  public static String connectorName = "COMMS_RECONCILIATION";

  // OIM user utility object for finding user data
  private tcUserOperationsIntf moUserUtility;
  
  // LDAP HashMap for purposes of holding comms dir data
  private HashMap<String,Boolean> ldapMap = null;
  
  // OIM HashMa for purposes of holding OIM attributes
  private HashMap<String,String> OIMMap = null;
  
  // Params pulled from the OIM IT resource
  private Map parameters = new HashMap();
  
  /**
   * Method to retrieve connection information from the OIM IT resource
   * and use it to establish connection to the comms directories, then
   * mine the comms directories for mailUserStatus attributes for 
   * all the users in the directory and return the results in a HashMap.
   * 
   * This HashMap is then later used to perform the comparison against
   * OIM.
   * 
   * We need to retrieve duDukeID (for purposes of comparing against 
   * OIM, where that's the user's Users.User ID value), and mailUserStatus
   * (for purposes of setting the state of the user for the comparison).
   * 
   * Return HashMap contains one hash for each user, indexed by duDukeID,
   * with a Boolean value that's FALSE if the user has no mailbox 
   * (the mailUserStatus value doesn't exist or is "removed") and TRUE
   * otherwise.
   */
  private HashMap<String,Boolean> getLDAPData() {
	  
	  
	  AttributeData attributeData = AttributeData.getInstance();
	  Attributes attributes = null;
	  SearchResult ldapResult = null;
	  LdapContext context = null;
	  NamingEnumeration results = null;
	  NamingEnumeration<SearchResult> iresults = null;
	  Boolean hasmailbox = false;
	  HashMap returnValue = new HashMap();
	  tcITResourceInstanceOperationsIntf moITResourceUtility = null;
	  tcResultSet moResultSet = null;
	  long resourceKey;
	  SearchResult resval = null;
	  Attributes ra = null;
	  String uniqueID = null;
	  String status = null;
	  
	  // Get handle for retrieving configuration from OIM
	  // For the IT Resource
	  try {
	  moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
	  super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
	  } catch (Exception e) {
		  throw new RuntimeException(connectorName + " Failed to retrieve LDAP configuration from OIM - check OIM IT Resource "+ e.getMessage(), e);
	  }
	  // Get the parameters for the LDAP connection from OIM
	  Map resourceMap = new HashMap();
	  resourceMap.put("IT Resources.Name","COMMS_RECONCILIATION");
	  try {
	  moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
	  resourceKey = moResultSet.getLongValue("IT Resources.Key");
	  } catch (Exception e) {
		  throw new RuntimeException(connectorName + " Unable to get IT Resource from factory - check OIM IT Resource" + e.getMessage(), e);
	  }
	  moResultSet = null;
	  try {
	  moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
	  for (int i = 0; i < moResultSet.getRowCount(); i++) {
		  moResultSet.goToRow(i);
		  String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
		  String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
		  parameters.put(name, value);
	  }
	  } catch (Exception e) {
		  throw new RuntimeException(connectorName + "Unable to get attributes from OIM IT resource - check IT Resource in OIM " + e.getMessage(), e);
	  } 
	  // Start by getting a connection to the LDAP
	  Hashtable<String,String> environment = new Hashtable<String,String>();
	  environment.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
	  environment.put(Context.PROVIDER_URL, (String) parameters.get("ldapURL"));
	  environment.put(Context.SECURITY_AUTHENTICATION, "simple");
	  environment.put(Context.SECURITY_PRINCIPAL, (String) parameters.get("ldapDN"));
	  environment.put(Context.SECURITY_CREDENTIALS, (String) parameters.get("ldapPW"));
	  environment.put(Context.SECURITY_PROTOCOL,"ssl");
	  
	  try {
	  context = new InitialLdapContext(environment, null);
	  
	  // If connection fails, retry one time before failing
	  
	  if (context == null) {
		  // Retry connection one more time after 20 seconds
		  Thread.sleep(20000);
		  context = new InitialLdapContext(environment, null);
		  // and fail if the context is still null -- return null
		  // hash and log an error to generate email notification
		  if (context == null) {
			  logger.error(connectorName + " Failed to make LDAP connection to commsdirs during comms reconciliation - reconciliation cannot continue");
			  return(null);
			  
		  }
	  }
	  } catch (javax.naming.NamingException e) {
		  // in this case, we excepted on something in LDAP
		  logger.error(connectorName + " Caught exception from JNDI during LDAP connection -- returning NULL " + e.getMessage());
		  e.printStackTrace();
		  return(null);
	  } catch (java.lang.InterruptedException e) {
		  logger.warn(connectorName + " Interrupt caught before timeout for retry of LDAP connection -- returning NULL");
		  return(null);
	  }
	  
	  // At this point, we should have a connection in the context
			  
	  // List of attributes to be retrieved from LDAP
	  String[] attrs = {"duDukeID","mailUserStatus"};
	  
	  // SearchControls to build the query
	  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs, false, false);
	  
	  // And execute the query in a try to catch throwables
	  try {
		  iresults = context.newInstance(null).search("o=Comms,dc=duke,dc=edu","(duDukeID=*)",cons);
	  } catch (NamingException e) {
		  	throw new RuntimeException(connectorName + " Failed LDAP search on bad connection: " + e.getMessage(),e);
	  }
	  
	  while (iresults.hasMoreElements()) {
		  try {
		  resval = iresults.next();
		  } catch (javax.naming.NamingException e) {
			  // Somehow, we had more elements, but when we went to get the next one, it wasn't there
			  // Assume this is a sign that the LDAP response was corrupt
			  logger.error(connectorName + " Corrupt LDAP search results -- throwing exception ");
			  throw new RuntimeException(connectorName + " Failed to find next result in LDAP search result set, even though hasMore was true "+ e.getMessage(),e);
		  }
		  ra = resval.getAttributes();
		  try {
		  uniqueID = (String) ra.get("duDukeID").get();
		  } catch (javax.naming.NamingException e) {
			  // this one is fatal
			  logger.error(connectorName + " DukeID was missing from user found by Unique ID -- very strange ");
			  throw new RuntimeException(connectorName + " User found by unique ID does not have a unique ID "+ e.getMessage(),e);
		  }
		  try {
		  if (ra.get("mailUserStatus") != null) {
			  status = (String) ra.get("mailUserStatus").get();
			  if (status != null && ! status.equals("") && !status.equals("removed")) {
				  hasmailbox = true;
			  } else {
				  hasmailbox = false;
			  }
		  } else {
			  hasmailbox = false;
		  }
		  } catch (Exception e) {
			  // In this case, we somehow failed to figure out whether the mailbox exists or not
			  // Since existence has less permanent effect on the world than non,
			  // we treat this as an existent case but log the anomaly
			  logger.warn(connectorName + " Exception reading mailUserStatus for user " + uniqueID + " so returning with hasmailbox = true for failsafe");
			  hasmailbox = true;
		  }
		  returnValue.put(uniqueID,hasmailbox);
	  }
	  
	  return(returnValue);
  }
  
  /**
   * Method to acquire the duACMailboxExists attributes for all 
   * users listed in the OIM database, and return the result as 
   * a large HashMap.  This map is from String to String, and 
   * indexed by unique ID.
   * 
   * The map is used for the comparison process by which we determine
   * whether to update duACMailboxExists for a user (and whether to 
   * trigger the revocation process for provisioning for the mailbox).
   * 
   */
  private HashMap<String,String> getOIMData() {
	  Hashtable mhPeopleSearchCriteria = new Hashtable();
	  Hashtable mhAccountsSearchCriteria = new Hashtable();
	  Hashtable mhTestSearchCriteria = new Hashtable();
	  mhPeopleSearchCriteria.put("USR_UDF_ENTRYTYPE","people"); // get people
	  mhAccountsSearchCriteria.put("USR_UDF_ENTRYTYPE","accounts");
	  mhTestSearchCriteria.put("USR_UDF_ENTRYTYPE","test");
	  mhPeopleSearchCriteria.put("Users.Status","Active");
	  mhAccountsSearchCriteria.put("Users.Status","Active");
	  mhTestSearchCriteria.put("Users.Status","Active");
	  
	  tcResultSet moResultSet;
	  HashMap retval = new HashMap();
	  String[] attrsToGet = {"Users.User ID","USR_UDF_ACMAILBOXEXISTS"};
	  
	  try {
		  moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		  
		  // Do the work for people first, since they're the bulk of us
		  try {
		  moResultSet = moUserUtility.findUsersFiltered(mhPeopleSearchCriteria,attrsToGet);
		  } catch (Exception e) {
			  logger.error(connectorName + " Exception trying to find all people in the OIM database " + e.getMessage());
			  return null;  // null return to signal failure
		  }
		  if (moResultSet.getRowCount() < 1) {
			  logger.error(connectorName + " Failed to find any people in OIM -- something must be wrong here");
			  return null;
		  }
		  // Otherwise, we have at least one user
		  // add to the hashmap
		  for (int i = 0; i < moResultSet.getRowCount(); i++) {
			  moResultSet.goToRow(i);
			  retval.put(moResultSet.getStringValue("Users.User ID"),moResultSet.getStringValue("USR_UDF_ACMAILBOXEXISTS"));
		  }
	  } catch (Exception e) {
		  // Exception whiel retrieving OIM user data
		  logger.error(connectorName + " Unexpected exception while retrieving OIM user information set " + e.getMessage());
		  throw new RuntimeException(connectorName + " Exception while retrieving OIM user (people) information set " + e.getMessage(),e);
	  }
	  try {
		  moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		  
		  // Do the work for people first, since they're the bulk of us
		  try {
		  moResultSet = moUserUtility.findUsersFiltered(mhAccountsSearchCriteria,attrsToGet);
		  } catch (Exception e) {
			  logger.error(connectorName + " Exception trying to find all accounts in the OIM database " + e.getMessage());
			  return null;  // null return to signal failure
		  }
		  if (moResultSet.getRowCount() < 1) {
			  logger.error(connectorName + " Failed to find any accounts in OIM -- something must be wrong here");
			  return null;
		  }
		  // Otherwise, we have at least one user
		  // add to the hashmap
		  for (int i = 0; i < moResultSet.getRowCount(); i++) {
			  moResultSet.goToRow(i);
			  retval.put(moResultSet.getStringValue("Users.User ID"),moResultSet.getStringValue("USR_UDF_ACMAILBOXEXISTS"));
		  }
	  } catch (Exception e) {
		  // Exception whiel retrieving OIM user data
		  logger.error(connectorName + " Unexpected exception while retrieving OIM user information set " + e.getMessage());
		  throw new RuntimeException(connectorName + " Exception while retrieving OIM user (accounts) information set " + e.getMessage(),e);
	  }
	  try {
		  moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		  
		  // Do the work for people first, since they're the bulk of us
		  try {
		  moResultSet = moUserUtility.findUsersFiltered(mhTestSearchCriteria,attrsToGet);
		  } catch (Exception e) {
			  logger.error(connectorName + " Exception trying to find all tests in the OIM database " + e.getMessage());
			  return null;  // null return to signal failure
		  }
		  if (moResultSet.getRowCount() < 1) {
			  logger.error(connectorName + " Failed to find any tests in OIM -- something must be wrong here");
			  return null;
		  }
		  // Otherwise, we have at least one user
		  // add to the hashmap
		  for (int i = 0; i < moResultSet.getRowCount(); i++) {
			  moResultSet.goToRow(i);
			  retval.put(moResultSet.getStringValue("Users.User ID"),moResultSet.getStringValue("USR_UDF_ACMAILBOXEXISTS"));
		  }
	  } catch (Exception e) {
		  // Exception whiel retrieving OIM user data
		  logger.error(connectorName + " Unexpected exception while retrieving OIM user information set " + e.getMessage());
		  throw new RuntimeException(connectorName + " Exception while retrieving OIM user (test) information set " + e.getMessage(),e);
	  }
	  return(retval);
  }
	  
  /**
   * This is where it all happens
   * 
   * Logic here is as follows
   * (1) Get all user information out of the comms directories
   * (2) Get all user information out of the OIM
   * (3) For each user in the comms directories
   * 	(4) If the user exists in OIM
   * 		(5) If status in comms is true and duacmailboxexists = 1
   * 			(6) continue, removing user from OIM list
   * 		(7) If status in comms is false and duacmailboxexists = 0
   * 			(8) continue, removing user from OIM list
   * 		(9) If status in comms is true and duacmailboxexists = 0
   * 			(10) set duacmailboxexists = 1, remove user from OIM list
   * 		(11) if status in comms is false and duacmailboxexists = 1
   * 			(12) set duacmailboxexists = 0, remove user from OIM list, and call revokeMailbox
   * (14) For each user left in the OIM list
   * 	(15) if user does not apear in the comms directories
   * 		(16) if duacmailboxexists = 1
   * 			(17) set duacmailboxexists = 0, remove user from OIM list, call revokeMailbox
   * 
   * Really not as complex as it looks.
   */
  
  public void execute() {
	  
	  // Get the LDAP user data first
	  ldapMap = getLDAPData();
	  
	  // Check whether the response was null and die if it was
	  if (ldapMap == null) {
		  logger.error(connectorName + " getLDAPData failed to return data -- bailing out");
		  return;
	  }
	  
	  // Get OIM user data next
	  OIMMap = getOIMData();
	  
	  // Check whether the response was null and die if it was
	  if (OIMMap == null) {
		  logger.error(connectorName + " getOIMData failed to return user data -- bailing out");
		  return;
	  }
	  
	  // Now run logic from 3-12
	  Set ldapKeys =  ldapMap.keySet();
	  Iterator iter = ldapKeys.iterator();
	  while (iter.hasNext()) {
		  // for each key in the ldapMap hash
		  String lk = (String) iter.next();
		  
		  // Check whether OIM has this user somewhere
		  if (OIMMap.containsKey(lk)) {
			  // OIM has this user, so proceed
			  Boolean lb = (Boolean) ldapMap.get(lk);
			  if (lb.booleanValue() && OIMMap.get(lk) != null && OIMMap.get(lk).equals("1")) {
				  // has mailbox and exists is 1 -- leave it alone
				  OIMMap.remove(lk);
				  continue;
			  }
			  if ((lb.booleanValue() && OIMMap.get(lk) != null && OIMMap.get(lk).equals("0")) || (lb.booleanValue() && OIMMap.get(lk) != null && OIMMap.get(lk).equals(""))) {
				  // has mailbox and exists is 0 -- set exists
				  // this should be very unlikely, but...
				  HashMap<String,String> doSearchCriteria = new HashMap<String,String>();
				  doSearchCriteria.put("Users.User ID",lk);
				  doSearchCriteria.put("Users.Status","Active");
				  String[] doattrs = {"Users.User ID"};
				  
				  try{
				  tcResultSet doResultSet = moUserUtility.findUsersFiltered(doSearchCriteria,doattrs);
				 
				  if (doResultSet.getRowCount() != 1) {
					  logger.error(connectorName + " Search in OIM for user " + lk + " returned " + doResultSet.getRowCount() + " results - something is wrong");
					  OIMMap.remove(lk);
					  continue;
				  } else {
					  HashMap<String,String> attrsForOIM = new HashMap<String,String>();
					  attrsForOIM.put("USR_UDF_ACMAILBOXEXISTS","1");
					  try {
					  moUserUtility.updateUser(doResultSet,attrsForOIM);
					  logger.info(connectorName + " Added duacmailboxexists to user " + lk);
					  } catch ( Exception e) {
						  // Failed the update 
						  logger.error(connectorName + " Failed add of duacmailboxexists for user " + lk + " due to exception: " + e.getMessage() + " but moving on");
					  }
					  OIMMap.remove(lk);
					  continue;
				  }
				  } catch (Exception e) {
					  // we failed to get the user from OIM, so nothing to do here -- go back to our homes
					  // except we log it, since we appear to have failed finding a user who is there
					  logger.error(connectorName + " User in OIMMap not present in OIM or setting duACMailboxExists failed -- very odd, but nothing to do here -- might want to investigate " + lk + " though");
					  OIMMap.remove(lk); // remove it anyway
					  continue;
				  }
			  }
			  if (! lb.booleanValue() && OIMMap.get(lk) != null && OIMMap.get(lk).equals("1")) {
				  // mailbox no longer exists in LDAP, but is on in OIM
				  // turn it off in OIM now
				  HashMap<String,String> doSearchCriteria = new HashMap<String,String>();
				  doSearchCriteria.put("Users.User ID",lk);
				  doSearchCriteria.put("Users.Status","Active");
				  String[] doattrs = {"Users.User ID","Users.Key"};
				  
				  try {
				  tcResultSet doResultSet = moUserUtility.findUsersFiltered(doSearchCriteria,doattrs);
				  if (doResultSet.getRowCount() != 1) {
					  logger.error(connectorName + " Search in OIM for user " + lk + " returned " + doResultSet.getRowCount() + " results - something is wrong");
					  OIMMap.remove(lk);
					  continue;
				  } else {
					  HashMap<String,String> attrsForOIM = new HashMap<String,String>();
					  attrsForOIM.put("USR_UDF_ACMAILBOXEXISTS","");
					  // need to call revokeMailbox() here, once it's written
					  revokeMailbox(doResultSet.getLongValue("Users.Key"));
					  try{
					  moUserUtility.updateUser(doResultSet,attrsForOIM);
					  logger.info(connectorName + " Removed duacmailboxexists from user " + lk);
					  } catch (Exception e) {
						  logger.error(connectorName + " Failed removing duacmailboxexists for " + lk + " please investigate -- moving on past " + e.getMessage());
					  }
					  OIMMap.remove(lk);
					  continue;
				  }
				  } catch (Exception e) {
					  // We missed on the findUsersFiltered in some fashion, so log an error and move on
					  logger.error(connectorName + " User in OIMap does not exist or failed removing duacmailboxexists for " + lk + " very odd, but nothing to do here -- please investigate");
					  OIMMap.remove(lk); // remove anyway again
					  continue;
				  }
			  } 
			  // Fallthru case, then, is mailbox doesn't exist and the user doesn't have duacmailboxexists
			  // so we leave the fallthru alone, but remove the OIM entry anyway
			  OIMMap.remove(lk); // remove anyway
			  continue;
		  } else {
			  // this user exists in LDAP but not in OIM
			  // we should note that and move on
			  logger.warn(connectorName + " User " + lk + " present in comms directory but missing in OIM.  Nothing to do, but be aware this is strange");
			  // no need to remove, since we're not present in the OIMMap
			  OIMMap.remove(lk);  // but remove anyway
		  }
	  }
	  // Now do the same thing with what's left in the OIMMap, but in reverse
	  Set OIMKeys =  OIMMap.keySet();
	  iter = OIMKeys.iterator();
	  while (iter.hasNext()) {
		  // for each key in the ldapMap hash
		  String ok = (String) iter.next();
		  // At this point, no one left in the OIM list should be in
		  // the comms list, but we need to be certain, so...
		  if (! ldapMap.containsKey(ok)) {
			  // This is correct -- we're here because the LDAP doesn't contain this user anymore
			  if (OIMMap.get(ok) != null && OIMMap.get(ok).equals("1")) {
				  // We need to undo a duacmailboxexists setting here
				  HashMap<String,String> doSearchCriteria = new HashMap<String,String>();
				  doSearchCriteria.put("Users.User ID",ok);
				  doSearchCriteria.put("Users.Status","Active");
				  String[] doattrs = {"Users.User ID","Users.Key"};
				  
				  try {
				  tcResultSet doResultSet = moUserUtility.findUsersFiltered(doSearchCriteria,doattrs);
				  if (doResultSet.getRowCount() != 1) {
					  logger.error(connectorName + " Search in OIM for user " + ok + " returned " + doResultSet.getRowCount() + " results - something is wrong");
					  continue;
				  } else {
					  HashMap<String,String> attrsForOIM = new HashMap<String,String>();
					  attrsForOIM.put("USR_UDF_ACMAILBOXEXISTS","");
					  // need to call revokeMailbox() here, once it's written
					  revokeMailbox(doResultSet.getLongValue("Users.Key"));
					  try {
					  moUserUtility.updateUser(doResultSet,attrsForOIM);
					  logger.info(connectorName + " Removed duacmailboxexists from user " + ok);
					  } catch(Exception e) {
						  // same as above
						  logger.error(connectorName + " caught eception when trying to remove duacmailboxexists from " + ok + " moving on, but you probably need to investigate " + e.getMessage());
					  }
				  }
				  } catch (Exception e) {
					  // same story as above
					  logger.error(connectorName + " Failed to find OIM user " + ok + " or otherwise lost on the OIM search -- continuing, but you should investigate");
				  }
			  }
			  // otherwise, nothing to do -- we're empty in OIM and there is no mailbox
			  // so we're all set.
		  } else {
			  // Something is awry -- we have the user in the LDAP but we got to this branch of the code
			  // indicating that we fell through some sort of wormhole.
			  logger.error(connectorName + " Impossible situation -- comms dir entry cannot exist, yet it does for user " + ok + " -- doing nothing with this case -- please investigate");
		  }
	  }

  }
  
  private void revokeMailbox(long userKey) {
	  //revokeProvisioning(dataProvider, userKey, "COMMSDIR_CREATEMAILBOX");
	  tcUserOperationsIntf rUserUtility = null;
	  tcProvisioningOperationsIntf rProvUtility = null;
	  TaskDefinitionOperationsIntf rTaskUtility = null;
	  
	  try {
		  tcDataProvider dataProvider = this.getDataBase();
		  rProvUtility = (tcProvisioningOperationsIntf)tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcProvisioningOperationsIntf");
		  rUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  rTaskUtility = (TaskDefinitionOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.TaskDefinitionOperationsIntf");
	  } catch (Exception e) {
		  logger.error(connectorName + " Failed to get prov/task/user utility for revoking mailbox -- returning");
		  return;
	  }
	  
	  HashMap<String, Long> taskkeys = null;
	  tcResultSet rResultSet = null;
	  
	  try {
	  rResultSet = rUserUtility.getObjects(userKey);
	  } catch (Exception e) {
		  logger.warn(connectorName + " Exception getting objects for user during revocation process -- returning, but you may want to investigate the exception: " + e.getMessage());
	  }
	  try {
	  long processInstanceKey = -1;
	  for (int i = 0; i < rResultSet.getRowCount(); i++) {
		  rResultSet.goToRow(i);
		  String objectName = rResultSet.getStringValue("Objects.Name");
		  String objectStatus = rResultSet.getStringValue("Objects.Object Status.Status");
		  if (objectName != null && objectName.equals("COMMSDIR_CREATEMAILBOX") && objectStatus != null && objectStatus.equals("Provisioned")) {
			  processInstanceKey = rResultSet.getLongValue("Process Instance.Key");
			  break;
		  }
	  }
	  if (processInstanceKey == -1) {
		  logger.warn(connectorName + " Attempted to revoke mailbox provisioning for users key " + userKey + " but user is not provisioned -- returning");
		  return;
	  }
	  
	  long taskKey = getTaskKeys(rTaskUtility,processInstanceKey);
	  
	  long taskInstanceKey = rProvUtility.addProcessTaskInstance(taskKey, processInstanceKey);
	  logger.info(connectorName + " revoked mailbox for users key " + userKey);
	  } catch (Exception e){
		 logger.warn(connectorName + " Was unable to revoke mailbox provisioning for user with users key " + userKey + ". Returning, but you need to investigate further");
		 return;
	  }
  }

  private static long getTaskKeys(TaskDefinitionOperationsIntf moTaskUtility,
          long processInstanceKey) throws tcAPIException, tcColumnNotFoundException {
                
        HashMap<String, String> filter = new HashMap<String, String>();
        filter.put("Process Definition.Tasks.Task Name", "Force Revoke");
        tcResultSet moResultSet = moTaskUtility.getTaskDetail(processInstanceKey, filter);
        
        return moResultSet.getLongValue("Process Definition.Tasks.Key");
        
     }

  
  //close the class
}

