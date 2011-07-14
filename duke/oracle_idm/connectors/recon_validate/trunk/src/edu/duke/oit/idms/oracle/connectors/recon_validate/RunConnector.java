package edu.duke.oit.idms.oracle.connectors.recon_validate;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;
import java.util.Hashtable;
import edu.duke.oit.idms.oracle.util.AttributeData;
import java.io.*;
import java.sql.*;

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
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;

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
 * This is a scheduled task to perform validation of reconned data in OIM
 * @author rob
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
 
  //Name of the current connector
  
  protected static String connectorName = "VALIDATE_RECONCILIATION";

  private tcUserOperationsIntf  moUserUtility; 
  
  private String[] getlist = {"uid"};
  private String[] otherlist = {"cn"};
  private HashSet uniqueIDs = new HashSet();
  private LdapContext context = null;
  
  private Map parameters = new HashMap();



  private AttributeData attributeData = AttributeData.getInstance();
  private ValidateDataImpl provisioningData = ValidateDataImpl.getInstance();

  // Some data stream objects
  
  File userFile = null;
  FileInputStream inStream = null;
  File outputFile = null;
  FileOutputStream outStream = null;
  String valUsersFile = null;
  String valOutputFile = null;
  
  // and the central LDAP HashMap
  
  HashMap ldapMAP = null;
  
  // and the OIM central HashMap
  HashMap oimMap = null;

  Map dukeidToUserKeyCache = null;
  Map groupNameToGroupKeyCache = new HashMap();
  
  /** 
   * Initialize the file parameters from the IT resource in OIM, 
   * acquire a copy of the user file, and read it to get a list of 
   * Unique IDs into the uniqueIDs array.  Returns nothing.
   */
  
  private void getUniqueIDs() throws Exception {
	    HashMap retval = new HashMap();

	    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
	    super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

	    Map resourceMap = new HashMap();
	    resourceMap.put("IT Resources.Name", "VALIDATE_RECONCILIATION");
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

	    if (parameters.containsKey("valUsersFile")) {
	    	valUsersFile = (String)parameters.get("valUsersFile");
	    } 
	    if (parameters.containsKey("valOutputFile")) {
	    	valOutputFile = (String)parameters.get("valOutputFile");
	    }

	    // At this point, we either successfully retrieved the file or we didn't, but we can begin the process
	    // of loading up data from the file anyway and see where we land.
	    //
	    
	    inStream = new FileInputStream(valUsersFile);
	    DataInputStream input = new DataInputStream(inStream);
	    
	    BufferedReader file = new BufferedReader(new InputStreamReader(input));
	    String s = null;
	    String uniqueID = null;
	    
	    while ((s = file.readLine()) != null) {
	    	// s contains another unique ID to add
	    	uniqueIDs.add(s);  // Add the unique ID to the set
	    }
	    logger.info(connectorName + " : Input file contains " + uniqueIDs.size() + " entries");
  }
  
  
  // Get a user's OIM attribute values into a hashMap and return
  // the hashMap. Acquires the list of attributes from a faux 
  // provisioning configuration for VALIDATE_RECONCILIATION.
  // This is a hack, but it avoids having a whole separate tree of 
  // property files and separate set of property subclasses just for 
  // this one purpose.
  
  private String[] allSyncAttributes = null;
  private String[] allSyncAttributesOIMNames = null;
  
  private HashMap getUserOIMAttributes(String uniqueID) throws Exception  {
	  Hashtable mhSearchCriteria = new Hashtable();
	  mhSearchCriteria.put("Users.User ID",uniqueID);
	  tcResultSet moResultSet;
	  HashMap retval = new HashMap();
	  
	  
	  if (allSyncAttributesOIMNames == null) {
	  allSyncAttributes = provisioningData.getAllAttributes();
	  allSyncAttributesOIMNames = new String[allSyncAttributes.length];
	  for (int i = 0; i < allSyncAttributes.length; i++) {
		  allSyncAttributesOIMNames[i] = attributeData.getOIMAttributeName(allSyncAttributes[i]);
	  }
	  }
	  try {
	  moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
	  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,allSyncAttributesOIMNames);
	  } catch (Exception e) {
		  logger.error(connectorName + " Error - caught exception in findUsersFiltered" + e.getMessage());
		  return retval;
	  }
	  if (moResultSet.getRowCount() != 1) {
		  logger.info(connectorName + " Throwing exception for too many or too few entries found in OIM");
		  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueID = " + uniqueID);
	  }
	  
	  // We have a single user back with the appropriate attributes, so load them into a hash map
	  for (int i = 0; i < allSyncAttributes.length; i++) {
		  retval.put(allSyncAttributesOIMNames[i],moResultSet.getStringValue(allSyncAttributesOIMNames[i]));
	  }
	  return(retval);
  }
  
  // Find a user by the user's unique ID in the LDAP
  // Return the user's attributes as specified in the String[] passed
  // as our second argument.
  
  public SearchResult findLDAPEntryByUniqueID(String uniqueID, String[] attrs) {
		SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs, false, false);
		String[] requiredAttributes = {"uid","duLDAPKey","cn"};
		SearchControls initial = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, requiredAttributes, false, false);
		NamingEnumeration results = null;
		NamingEnumeration<SearchResult> iresults = null;
		String dn = null;
		
		try {
			try {
				iresults = context.newInstance(null).search("ou=People,dc=duke,dc=edu","(duDukeID=" + uniqueID + ")",initial);
			} catch (NamingException e) {
				throw new RuntimeException(connectorName + " Failed on bad LDAP connection: " + e.getMessage(),e);
			}
			if (iresults.hasMoreElements()) {
				SearchResult resval = iresults.next();
				Attributes retval = resval.getAttributes();
				String uidValue = (String) retval.get("duLDAPKey").get();
				dn = "duLDAPKey=" + uidValue + ",ou=People,dc=duke,dc=edu";
			} else {
				return null;
			}
		} catch (NamingException e) {
			throw new RuntimeException("Failed querying LDAP for user with uniqueID " + uniqueID + " message: " + e.getMessage(),e);
		}
		
		try {
			try {
				results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
			} catch (NamingException e) {
				// Maybe we just got disconnected 
				throw new RuntimeException(connectorName + " Failed on bad LDAP connection (2) : " + e.getMessage(),e);
			}
			if (results.hasMoreElements()) {
				return (SearchResult) results.next();
			} else {
				return null;
			}
		} catch (NamingException e) {
			throw new RuntimeException("Failed querying LDAP for " + dn + " with " + e.getMessage(), e);
		}
	}

  
  // Get a user's LDAP attributes from the service directories, 
  // returning an Attributes object of the values on the attributes.
  // Acquires its list from the same faux provisioning list
  // as the method above.  
  
  private Attributes getUserLDAPAttributes(String uniqueID) throws Exception {
	  AttributeData attributeData = AttributeData.getInstance();
	  tcResultSet moResultSet;
	  Attributes retval = null;
	  SearchResult ldapResult = null;
	  
      if (context == null) {

    	  Hashtable<String, String> environment = new Hashtable<String, String>();
    	  environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    	  environment.put(Context.PROVIDER_URL, (String) parameters.get("valProviderURL"));
    	  environment.put(Context.SECURITY_AUTHENTICATION, "simple");
    	  environment.put(Context.SECURITY_PRINCIPAL, (String) parameters.get("valUserDN"));
    	  environment.put(Context.SECURITY_CREDENTIALS, (String) parameters.get("valBINDPw"));
    	  environment.put(Context.SECURITY_PROTOCOL, "ssl");
      
    	  context = new InitialLdapContext(environment, null);
      }
	  if (context == null) {
		  logger.error(connectorName + ": Failed to get LDAP connection");
	  }
	  // logger.info(connectorName + " Getting LDAP Attributes for user " + uniqueID);
      //ldapResult = findLDAPEntryByUniqueID(uniqueID,provisioningData.getAllAttributes());
      if (allSyncAttributes != null) {
    	  ldapResult = findLDAPEntryByUniqueID(uniqueID,allSyncAttributes);
      } else {
    	  ldapResult = findLDAPEntryByUniqueID(uniqueID,provisioningData.getAllAttributes());
      }
	  retval = ldapResult.getAttributes();	  
	  
	  return(retval); 
  }
  
  
  protected void execute() {
	  
	  HashMap OIMAttrs = null;
	  Iterator oimIter = null;
	  String key = null;
	  String val = null;
	  String ldapKey = null;
	  String ldapVal = null;
	  String user = null;
	  String accum = null;
	  String misMatch = "";
	  Attributes LDAPAttrs = null;
	  Set OIMVals = null;
	  Set LDAPVals = null;
	  Connection con = null;
	  Statement stmt = null;
	  ResultSet res = null;
	  String[] special = {"cn","duaclblock","dublockmail","dudemoidhist","dudepartmentalemail","dudukeidhistory","dueligible","duprovisioning","dusponsor","duuserprivacyrequest","ou","telephonenumber","title"};
	  
	  
	  HashSet specialAttrs = new HashSet(Arrays.asList(special));
	  
	  int reconnects = 0; // track number of PR connection attempts
	  
	  AttributeData attributeData = AttributeData.getInstance();
	  try {
		  
		  DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		  con = DriverManager.getConnection("jdbc:oracle:thin:@SUPPRESSED:SUPPRESSED:SUPPRESSED","SUPPRESSED","SUPPRESSED");
		  
    logger.info(connectorName + ": Starting task.");
    
    getUniqueIDs();
    
    Iterator iter = uniqueIDs.iterator();
    
    while (iter.hasNext()) {
    	user = (String) iter.next();
    	OIMAttrs = getUserOIMAttributes(user);
    	LDAPAttrs = getUserLDAPAttributes(user);
    	if (LDAPAttrs == null) {
    		logger.error(connectorName + " Null LDAP attribute list returned");
    		return;
    	}
	  oimIter = OIMAttrs.keySet().iterator();
	  while (oimIter.hasNext()) {
		  key = (String) oimIter.next();
		  val = (String) OIMAttrs.get(key);
		  val = val.trim();
		  ldapKey = (String) attributeData.getLDAPAttributeName(key);
		  try {
			  // if (! ldapKey.toLowerCase().equals("cn") && ! ldapKey.toLowerCase().equals("duaclblock") && ! ldapKey.toLowerCase().equals("dublockmail") && ! ldapKey.toLowerCase().equals("dudempoidhist") && ! ldapKey.toLowerCase().equals("dudepartmentalemail") && ! ldapKey.toLowerCase().equals("dudukeidhistory") && ! ldapKey.toLowerCase().equals("dueligible") && ! ldapKey.toLowerCase().equals("duprovisioning") && ! ldapKey.toLowerCase().equals("dusponsor") && ! ldapKey.toLowerCase().equals("duuserprivacyrequest") && ! ldapKey.toLowerCase().equals("ou") && ! ldapKey.toLowerCase().equals("telephonenumber") && ! ldapKey.toLowerCase().equals("title")) {
			  if (! specialAttrs.contains(ldapKey.toLowerCase())) {
			  	  ldapVal = (String) LDAPAttrs.get(ldapKey).get();
				  ldapVal = ldapVal.trim();
			  } else {
				  String[] OIMArray = val.split("[|]");
				  OIMVals = new HashSet();
				  for (int ii = 0; ii < OIMArray.length; ii++) {
					  OIMVals.add(OIMArray[ii]);
				  }
				  
				  ldapVal = "";
				  NamingEnumeration ldapValEnum = (NamingEnumeration) LDAPAttrs.get(ldapKey).getAll();
				  LDAPVals = new HashSet();

				  while (ldapValEnum.hasMore()) {
					  LDAPVals.add(ldapValEnum.next());
				  }
				  LDAPVals.removeAll(OIMVals);
				  if (LDAPVals.isEmpty()) {
					  ldapVal = "";
					  val = "";
				  } else {
					  					  
					  Iterator li = LDAPVals.iterator();
					  String liv = null;
					  while (li.hasNext()) {
						  if (ldapVal.equals("")) {
							  liv = (String) li.next();
							  liv = liv.trim();
							  ldapVal += (String) liv;
						  } else {
							  liv = (String) li.next();
							  liv = liv.trim();
							  ldapVal += "|" + (String) liv;
						  }
					  }
				  }
			  }
		  } catch (NullPointerException e) {
			  // This is OK -- it means the LDAP value is empty, so set it to a null string
			  ldapVal = "";
		  }
		 
		  if (! ldapVal.trim().equals(val.trim())) {
				  // Check against person registry, and either log a warning
				  // or an error depending on the result from it.
			 		try {
			 	  res = null;
				  stmt = con.createStatement();
				  if (ldapKey.toLowerCase().equals("cn")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select value from staging_cn where prid = " + prid);
					  }
				  }
				  else if (ldapKey.toLowerCase().equals("dudukecardnbr")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select identifier from pr_identifier where type_cd = 'dukecard' and prid = " + prid);
					  }
				  } else if (ldapKey.toLowerCase().equals("duaclblock")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select value from staging_aclblock where prid = " + prid);
					  } 
				  } else if (ldapKey.toLowerCase().equals("telephonenumber")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select value from staging_telephonenumber where prid = " + prid);
					  } 
				  } else if (ldapKey.toLowerCase().equals("title")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select value from staging_title where prid = " + prid);
					  } 
				  } else if (ldapKey.toLowerCase().equals("ou")) {
					  res = stmt.executeQuery("select prid from pr_identifier where type_cd = 'dukeid' and identifier = " + user);
					  if (res.next()) {
						  String prid = res.getString(1);
						  res = stmt.executeQuery("select value from staging_ou where prid = " + prid);
					  } 
				  } else {
				  res = stmt.executeQuery("select " + ldapKey + " from OIM_RECON_VIEW where DUDUKEID = " + user);
				  }
				  int gotValue = 0;
				  try {
					  
					  // New handler for multivalued PR attributes
					  HashSet ls = new HashSet();
					  HashSet lr = new HashSet();
					  
					  while (res != null && res.next()) {
						  gotValue = 1;
						  lr.add(res.getString(1).trim());
					  }
					  for (String s : val.split("[|]")) {
						  ls.add(s.trim());
					  }
					  ls.removeAll(lr);
					  if (ls.isEmpty()) {
						  logger.info(connectorName + ": Ignoring difference between LDAP and OIM for user " + user + " in attribute " + ldapKey + " because values match in OIM and PR");
					  } else {
						  misMatch += connectorName + " : " + user + " uas a mismatch in " + ldapKey + " : LDAP = " + ldapVal + " while OIM = " + val + "\r\n";
					  }
					
			
				  } catch ( Exception e) {
					  // Ignore this case
				  }
			 		} catch (Exception e) {
			 			if (res == null) {
			 				logger.info(connectorName + "null PR value - trying to reconnect");
			 				if (reconnects > 10) {
			 					throw new RuntimeException("Failed to reconnect more than 10 times -- exiting");
			 				}
			 				if (con.isClosed()) {
			 					con = DriverManager.getConnection("jdbc:oracle:thin:@SUPPRESSED:SUPPRESSED:SUPPRESSED","SUPPRESSED","SUPPRESSED");
			 					if (con != null) {
			 						logger.info(connectorName + "Reconnect ");
			 					} else {
			 						logger.warn(connectorName + "Reconnect failed");
			 					}
			 					reconnects += 1;
			 				} else {
			 					con.close();
			 					con = DriverManager.getConnection("jdbc:oracle:thin:@SUPPRESSED:SUPPRESSED:SUPPRESSED","SUPPRESSED","SUPPRESSED");
			 					if (con != null) {
			 						logger.info(connectorName + "Reconnect ");
			 					} else {
			 						logger.warn(connectorName + "Reconnect failed");
			 					}
			 					reconnects += 1;
			 				}
			 			} else {
			 			misMatch += connectorName + " : " + user + " has a mismatch in " + ldapKey + " : LDAP = " + ldapVal + " while OIM = " + val + "\r\n";
			 			}
			 		}
		 }
	  }
    }
	  } catch (Exception e) {
		  throw new RuntimeException(e);
	  }
		 if (! misMatch.equals("")) {

		  logger.error(connectorName + "\r\n" + misMatch);
		  System.out.println(misMatch);
	  }
	  logger.info(connectorName + " Completed -- cf email for results if there are any");
  }
}
    
 
