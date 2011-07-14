package edu.duke.oit.idms.oracle.connectors.recon_grouper_to_win_ad;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import Thor.API.tcResultSet;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.exceptions.OIMUserNotFoundException;
import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;


/**
 * This is a task that queries an event log in the Grouper database and uses
 * that to update the group memberships in win.duke.edu.
 * @author shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private String userName = "groups";
  private String connectorName = "GROUPER_TO_WIN_AD_RECONCILIATION";

  private Connection conn;
  private LdapContext context;

  private ArrayList<String> groupFailures = new ArrayList<String>();
  private ArrayList<String> userFailures = new ArrayList<String>();
  
  private Pattern coursePattern = Pattern.compile("^siss:courses:(\\S+):(\\S+):(\\S+):(\\S+):(\\S+):(\\S+)$", Pattern.CASE_INSENSITIVE);

  Map<String, String> termCodeToTermNameCache = new HashMap<String, String>();
  
  String rootContainer = null;

  protected void execute() {

    logger.info(connectorName + ": Starting task.");

    // Process changes in Grouper sequentially
    ResultSet rs = null;
    ResultSet rs2 = null;
    PreparedStatement psSelect = null;
    PreparedStatement psDelete = null;
    PreparedStatement psUpdateToFailure = null;
    PreparedStatement psResetRetries = null;
    PreparedStatement psRemovePreviousAddDeleteMember = null;
    PreparedStatement psRenamePreviousAddDeleteMember = null;
    PreparedStatement psSelectTerms = null;

    try {
      conn = getDatabaseConnection();
      context = getLDAPConnection();
      
      psDelete = conn.prepareStatement("DELETE FROM " + userName + ".grp_event_log_for_oim WHERE consumer = 'grouper_ad' AND record_id=?");
      psSelect = conn.prepareStatement("SELECT record_id, group_name, action, field_1, status FROM " + userName + ".grp_event_log_for_oim WHERE consumer = 'grouper_ad' ORDER BY record_id");
      psSelectTerms = conn.prepareStatement("SELECT term_code, term_name FROM " + userName + ".grp_course_term_code_to_name");
      psUpdateToFailure = conn.prepareStatement("UPDATE " + userName + ".grp_event_log_for_oim SET status='FAILED', retry_time=? where consumer = 'grouper_ad' AND record_id=?");
      psResetRetries = conn.prepareStatement("UPDATE " + userName + ".grp_event_log_for_oim SET status='NEW', retry_time=null where consumer = 'grouper_ad' and status='FAILED' and retry_time < ?");
      psRemovePreviousAddDeleteMember = conn.prepareStatement("DELETE FROM " + userName + ".grp_event_log_for_oim WHERE consumer = 'grouper_ad' AND group_name = ? AND (action = 'add_member' or action = 'delete_member') AND record_id < ?");
      psRenamePreviousAddDeleteMember = conn.prepareStatement("UPDATE " + userName + ".grp_event_log_for_oim SET group_name = ? WHERE consumer = 'grouper_ad' AND group_name = ? AND (action = 'add_member' or action = 'delete_member') AND record_id < ?");
      
      // first cache term values
      rs = psSelectTerms.executeQuery();
      while (rs.next()) {
        termCodeToTermNameCache.put(rs.getString(1), rs.getString(2));
      }
      
      // second reset retries
      psResetRetries.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
      psResetRetries.executeUpdate();
      
      // now go through events
      rs2 = psSelect.executeQuery();
      while (rs2.next()) {
        long recordId = rs2.getLong(1);
        String groupName = rs2.getString(2);
        String action = rs2.getString(3);
        String field1 = rs2.getString(4);
        String status = rs2.getString(5);

        if (status.equals("FAILED")) {
          updateFailures(action, groupName, field1);
          continue;
        }

        // If an add_member or delete_member fails, we won't process anymore events for that user.
        // If a create, delete, or rename fails, we won't process anymore events for that group.
        if (mustSkipUserOrGroup(action, groupName, field1)) {
          logger.warn(connectorName + ": Skipping record " + recordId + " due to a previous failure with another record with the same user or group.");
          continue;
        }

        // this part is going to be in a separate try-catch block because
        // if an individual update fails, we don't want to stop processing
        boolean isSuccess = false;
        try {
          
          // note that we're assuming groups don't get moved to and from the duke stem.
          if (groupName.startsWith("duke:")) {
            if (action.equals("create")) {
              processCreate(groupName);
            } else if (action.equals("delete")) {
              // first delete previous add and delete members (they could be in a failure state)
              psRemovePreviousAddDeleteMember.setString(1, groupName);
              psRemovePreviousAddDeleteMember.setLong(2, recordId);
              psRemovePreviousAddDeleteMember.executeUpdate();
              
              processDelete(groupName);
            } else if (action.equals("rename")) {
              // first update previous add and delete members (they could be in a failure state)
              psRenamePreviousAddDeleteMember.setString(1, field1);
              psRenamePreviousAddDeleteMember.setString(2, groupName);
              psRenamePreviousAddDeleteMember.setLong(3, recordId);
              psRenamePreviousAddDeleteMember.executeUpdate();
              
              processRename(groupName, field1);
            } else if (action.equals("add_member")) {
              processAddMember(groupName, field1);
            } else if (action.equals("delete_member")) {
              processDeleteMember(groupName, field1);
            } else {
              throw new Exception("Unknown action for record id " + recordId);
            }
          }

          // If we get here, then the LDAP update succeeded or the update is not needed.
          isSuccess = true;
        } catch (OIMUserNotFoundException e2) {
          // let's not let this send an email
          logger.warn(connectorName + ": Error while processing record " + recordId + ".", e2);
        } catch (ADUserNotFoundException e2) {
          // let's not let this send an email
          logger.warn(connectorName + ": Error while processing record " + recordId + ".", e2);
        } catch (Exception e2) {
          logger.error(connectorName + ": Error while processing record " + recordId + ".", e2);
        }

        if (isSuccess) {
          // remove the event from the grouper db
          psDelete.setLong(1, recordId);
          psDelete.executeUpdate();
        } else {
          // mark the event as failed and retry in 24 hours
          psUpdateToFailure.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime() + 86400000));
          psUpdateToFailure.setLong(2, recordId);
          psUpdateToFailure.executeUpdate();
          updateFailures(action, groupName, field1);
        }
      }
    } catch (Exception e) {
      logger.error(connectorName + ": Error while running connector.", e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (rs2 != null) {
        try {
          rs2.close();
        } catch (SQLException e) {
          // this is okay
        }
      }

      if (psSelect != null) {
        try {
          psSelect.close();
        } catch (SQLException e) {
          // this is okay
        }
      }

      if (psDelete != null) {
        try {
          psDelete.close();
        } catch (SQLException e) {
          // this is okay
        }
      }

      if (psUpdateToFailure != null) {
        try {
          psUpdateToFailure.close();
        } catch (SQLException e) {
          // this is okay
        }
      }

      if (psResetRetries != null) {
        try {
          psResetRetries.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psRemovePreviousAddDeleteMember != null) {
        try {
          psRemovePreviousAddDeleteMember.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psRenamePreviousAddDeleteMember != null) {
        try {
          psRenamePreviousAddDeleteMember.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psSelectTerms != null) {
        try {
          psSelectTerms.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (context != null) {
        try {
          context.close();
        } catch (NamingException e) {
          // this is okay
        }
      }
    }

    logger.info(connectorName + ": Ending task.");
  }

  private boolean mustSkipUserOrGroup(String action, String groupName, String field1) {
    if (action.equals("create") || action.equals("delete")) {
      if (groupFailures.contains(groupName)) {
        return true;
      }
    } else if (action.equals("rename")) {
      if (groupFailures.contains(groupName) || groupFailures.contains(field1)) {
        return true;
      }
    } else {
      if (groupFailures.contains(groupName) || userFailures.contains(field1)) {
        return true;
      }
    }

    return false;
  }

  private void updateFailures(String action, String groupName, String field1) {
    if (action.equals("create") || action.equals("delete")) {
      groupFailures.add(groupName);
    } else if (action.equals("rename")) {
      groupFailures.add(groupName);
      groupFailures.add(field1);
    } else {
      userFailures.add(field1);
    }
  }

  /**
   * Get the connection parameters for the IT Resource GROUPER_RECONCILIATION.
   * Then returns a connection based on those parameters.
   * @return database connection
   * @throws Exception
   */
  private Connection getDatabaseConnection() throws Exception {
    Map<String, String> parameters = new HashMap<String, String> ();

    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
        super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

    Map<String, String>  resourceMap = new HashMap<String, String> ();
    resourceMap.put("IT Resources.Name", "GROUPER_RECONCILIATION");
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

    Class.forName((String)parameters.get("driver"));
    userName = (String)parameters.get("username");

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

    Connection conn = DriverManager.getConnection((String)parameters.get("url"), props);
    return conn;   
  }
  
  /**
   * Get the connection parameters for the IT Resource WINAD_PROVISIONING.
   * Then returns a connection based on those parameters.
   * @return ldap connection
   * @throws Exception
   */
  private LdapContext getLDAPConnection() throws Exception {
    Map<String, String> parameters = new HashMap<String, String> ();

    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
        super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

    Map<String, String>  resourceMap = new HashMap<String, String> ();
    resourceMap.put("IT Resources.Name", "WINAD_PROVISIONING");
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
    
    rootContainer = parameters.get("rootContainer");

    Hashtable<String, String> environment = new Hashtable<String, String>();
    environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
    environment.put(Context.SECURITY_AUTHENTICATION, "simple");
    environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
    environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
    environment.put("java.naming.ldap.factory.socket", edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());
    
    if (parameters.get("providerUrl").startsWith("ldaps://") || 
        parameters.get("providerUrl").endsWith(":636")) {
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
    }
    
    return new InitialLdapContext(environment, null);
  }
  
  /**
   * Translate based on OU structure being used in AD (colon separated)
   * @param groupName
   * @return new name
   */
  private String translateGroupName(String groupName) throws Exception {
    
    // first get rid of leading "duke:".
    if (!groupName.startsWith("duke:")) {
      throw new Exception("Group name doesn't start with 'duke:'");
    }
    
    groupName = groupName.substring(5);

    Matcher matcher = coursePattern.matcher(groupName);
    
    if (matcher.find()) {
      String termCode = matcher.group(5);
      String termName = termCodeToTermNameCache.get(termCode);
      
      if (termName == null || termName.equals("")) {
        throw new RuntimeException("Unable to map term code " + termCode + " to term name.");
      }
      
      groupName = "courses:" + termName + ":" + matcher.group(1) + matcher.group(2) 
          + ":sec" + matcher.group(3) + ":" + matcher.group(6);
    }
    
    return groupName;
  }
  
  /**
   * Get an LDAP entry
   * @param dn
   * @param attrs
   * @return SearchResult
   */
  private SearchResult findEntryByDn(String dn, String[] attrs) {
    SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      results = context.search(dn, "(objectClass=*)", cons);
      if (results.hasMoreElements()) {
        return results.next();
      } else {
        return null;
      }
    } catch (NameNotFoundException e) {
      return null;
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  private boolean isMember(String groupDn, String userDn) throws NamingException {
    SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = context.search(groupDn, "(member=" + userDn + ")", cons);
    if (results.hasMoreElements()) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Get an LDAP entry
   * @param attrName 
   * @param attrValue 
   * @return SearchResult
   */
  private SearchResult findEntryByAttr(String attrName, String attrValue) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      results = context.search(rootContainer, "(" + attrName + "=" + attrValue + ")", cons);
      if (results.hasMoreElements()) {
        return results.next();
      } else {
        return null;
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * @param oldDn
   * @param newDn
   */
  private void renameEntry(String oldDn, String newDn) {
    if (findEntryByDn(oldDn, new String[0]) == null && findEntryByDn(newDn, new String[0]) != null) {
      // apparently this was already renamed...
      return;
    }
    
    try {
      context.rename(oldDn, newDn);
      SimpleProvisioning.logger.info(connectorName + ": Renamed entry " + oldDn + " to " + newDn);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while renaming LDAP entry with DN: " + oldDn + ": " + e.getMessage(), e);    
    }
  }
  
  /**
   * @param dn
   * @param cn
   */
  private void createGroup(String dn, String cn, String sAMAccountName) {

    // add the object classes
    Attributes attributes = new BasicAttributes();
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("group");
    attributes.put(objectClassAttr);
    
    // add the cn attribute
    Attribute cnAttr = new BasicAttribute("cn");
    cnAttr.add(cn);
    attributes.put(cnAttr);
    
    // add the sAMAccountName attribute
    Attribute sAMAccountNameAttr = new BasicAttribute("sAMAccountName");
    sAMAccountNameAttr.add(sAMAccountName);
    attributes.put(sAMAccountNameAttr);
    
    try {
      context.createSubcontext(new CompositeName().add(dn), attributes);
      SimpleProvisioning.logger.info(connectorName + ": Created group " + dn);
    } catch (NameAlreadyBoundException e) {
      // if the group already exists, just ignore
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with DN: " + dn + ": " + e.getMessage(), e);    
    }
  }
  
  /**
   * @param dn
   * @param ou
   */
  private void createOu(String dn, String ou) {

    // add the object classes
    Attributes attributes = new BasicAttributes();
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("organizationalUnit");
    attributes.put(objectClassAttr);
    
    // add the ou attribute
    Attribute ouAttr = new BasicAttribute("ou");
    ouAttr.add(ou);
    attributes.put(ouAttr);
    
    try {
      context.createSubcontext(new CompositeName().add(dn), attributes);
      SimpleProvisioning.logger.info(connectorName + ": Created entry " + dn);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with DN: " + dn + ": " + e.getMessage(), e);
    }
  }
  
  private void updateEntryReplaceAttributes(String dn, Attributes replaceAttrs) throws Exception {
    try {
      context.modifyAttributes(dn, LdapContext.REPLACE_ATTRIBUTE, replaceAttrs);
      SimpleProvisioning.logger.info(connectorName + ": Updated entry " + dn);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while updating entry " + dn + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param result
   */
  private void deleteEntry(String dn) {
    try {
      context.destroySubcontext(dn);
      SimpleProvisioning.logger.info(connectorName + ": Deleted entry " + dn);
    } catch (NameNotFoundException e) {
      // if the entry doesn't exist, just ignore
    } catch (NamingException e) {
      throw new RuntimeException("Failed while deleting from LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * @param groupName
   */
  private void deleteEmptyOus(String groupName) {
    String[] parts = groupName.split(":");
    
    // we're going to work through the stems from the deepest  
    // OU backwards to find out which OUs need to be deleted.
    for (int i = parts.length - 2; i >= 0; i--) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }

      // check if this stem has children
      String dn = getLDAPOuDnFromStemName(stemName);
      NamingEnumeration<SearchResult> results = findChildEntriesByDn(dn, new String[0]);
      if (results == null) {
        continue;
      } else if (results.hasMoreElements()) {
        return;
      } else {
        deleteEntry(dn);
      }
    }
  }
  
  /**
   * Get child entries
   * @param dn
   * @param attrs
   * @return NamingEnumeration
   */
  private NamingEnumeration<SearchResult> findChildEntriesByDn(String dn, String[] attrs) {
    SearchControls cons = new SearchControls(SearchControls.ONELEVEL_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      results = context.search(dn, "(objectClass=*)", cons);
      return results;
    } catch (NameNotFoundException e) {
      return null;
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * @param stemName
   * @return string
   */
  private String getLDAPOuDnFromStemName(String stemName) {
    
    String[] parts = stemName.split(":");
    String dn = "ou=" + parts[parts.length - 1];

    for (int i = parts.length - 2; i >= 0; i--) {
      dn += ",ou=" + parts[i];
    }
    
    dn += ",ou=groups," + rootContainer;
    
    return dn;
  }
  
  /**
   * @param groupName
   * @return string
   */
  private String getLDAPGroupDnFromGroupName(String groupName) {
    
    String[] parts = groupName.split(":");
    String dn = "cn=" + parts[parts.length - 1];

    for (int i = parts.length - 2; i >= 0; i--) {
      dn += ",ou=" + parts[i];
    }
    
    dn += ",ou=groups," + rootContainer;
    
    return dn;
  }

  private void createOusForGroup(String groupName) {
    String[] parts = groupName.split(":");
    
    // we're going to work through the stems from the deepest  
    // OU backwards to find out which OUs need to be created.
    int stemExists = -1;
    for (int i = parts.length - 2; i >= 0; i--) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }

      // check if this stem exists in ldap
      String dn = getLDAPOuDnFromStemName(stemName);
      SearchResult result = findEntryByDn(dn, new String[0]);
      
      if (result != null) {
        stemExists = i;
        break;
      }
    }

    // now we'll create some stems...
    for (int i = stemExists + 1; i < parts.length - 1; i++) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }
      
      // create OU in ldap
      String dn = getLDAPOuDnFromStemName(stemName);
      String ou = parts[i];
      createOu(dn, ou);
    }
  }
  
  private void processCreate(String groupName) throws Exception {
    try {
      String sAMAccountName = "grouper-" + groupName.replaceAll(":", "-");
      String translatedGroupName = translateGroupName(groupName);
      String[] parts = translatedGroupName.split(":");

      // first create OUs
      createOusForGroup(translatedGroupName);
      
      // okay now that we have the OUs created, we'll create the group.
      String dn = getLDAPGroupDnFromGroupName(translatedGroupName);
      createGroup(dn, parts[parts.length - 1], sAMAccountName);
      
      logger.info(connectorName + ": Created group " + groupName);
    } catch (Exception e) {
      throw new Exception("Error while creating group " + groupName, e);
    }
  }

  private void processDelete(String groupName) throws Exception {
    try {
      String translatedGroupName = translateGroupName(groupName);

      // delete the group
      deleteEntry(getLDAPGroupDnFromGroupName(translatedGroupName));
      
      // now delete empty OUs
      deleteEmptyOus(translatedGroupName);
    } catch (Exception e) {
      throw new Exception("Error while deleting group " + groupName, e);
    }
  }

  private void processRename(String oldGroupName, String newGroupName) throws Exception {
    try {
      String sAMAccountName = "grouper-" + newGroupName.replaceAll(":", "-");

      // add OUs for new location
      createOusForGroup(translateGroupName(newGroupName));
      
      // rename entry
      renameEntry(getLDAPGroupDnFromGroupName(translateGroupName(oldGroupName)), 
          getLDAPGroupDnFromGroupName(translateGroupName(newGroupName)));
      
      // update sAMAccountName
      Attributes replaceAttrs = new BasicAttributes();
      Attribute replaceAttr = new BasicAttribute("sAMAccountName");
      replaceAttr.add(sAMAccountName);
      replaceAttrs.put(replaceAttr);    
      updateEntryReplaceAttributes(getLDAPGroupDnFromGroupName(translateGroupName(newGroupName)), replaceAttrs);
      
      // delete OUs for old location
      deleteEmptyOus(translateGroupName(oldGroupName));
    } catch (Exception e) {
      throw new Exception("Error while renaming group " + oldGroupName + " to " + newGroupName, e);
    }
  }

  private void processAddMember(String groupName, String dukeid) throws Exception {
    try {
      String groupDn = getLDAPGroupDnFromGroupName(translateGroupName(groupName));
      SearchResult result = findEntryByAttr("duLDAPKey", getLdapKeyFromDukeID(dukeid));
      
      if (result == null) {
        throw new ADUserNotFoundException("Not found for " + dukeid);
      }
      
      String userDn = result.getNameInNamespace();
      
      Attributes modAttrs = new BasicAttributes();
      Attribute modAttr = new BasicAttribute("member");
      modAttr.add(userDn);
      modAttrs.put(modAttr);
      
      try {
        context.modifyAttributes(groupDn, LdapContext.ADD_ATTRIBUTE, modAttrs);
        SimpleProvisioning.logger.info(connectorName + ": Added member " + dukeid + " to " + groupName);
      } catch (NamingException e) {
        // if the person is a member of the group, just ignore;  
        // Seems like NameAlreadyBoundException is being thrown instead of AttributeInUseException.
        // So I'll just check the memberships
        if (isMember(groupDn, userDn)) {
          SimpleProvisioning.logger.info(connectorName + ": User " + dukeid + " already in group " + groupName);
        } else {
          throw new RuntimeException("Failed while adding member " + dukeid + " to group " + groupName + ": " + e.getMessage(), e);
        }
      }
    } catch (OIMUserNotFoundException e) {
      // just throw
      throw e;
    } catch (ADUserNotFoundException e) {
      // just throw
      throw e;
    } catch (Exception e) {
      throw new Exception("Error while adding member " + dukeid + " to group " + groupName, e);
    }
  }

  private void processDeleteMember(String groupName, String dukeid) throws Exception {
    try {
      String groupDn = getLDAPGroupDnFromGroupName(translateGroupName(groupName));
      
      SearchResult result = null;
      
      try {
        result = findEntryByAttr("duLDAPKey", getLdapKeyFromDukeID(dukeid));
      } catch (OIMUserNotFoundException e) {
        return;
      }
      
      if (result == null) {
        return;
      }
      
      String userDn = result.getNameInNamespace();
      
      Attributes modAttrs = new BasicAttributes();
      Attribute modAttr = new BasicAttribute("member");
      modAttr.add(userDn);
      modAttrs.put(modAttr);
      
      try {
        context.modifyAttributes(groupDn, LdapContext.REMOVE_ATTRIBUTE, modAttrs);
        SimpleProvisioning.logger.info(connectorName + ": Deleted member " + dukeid + " from " + groupName);
      } catch (NamingException e) {
        // if the person is not a member of the group, just ignore;  
        // Seems like OperationNotSupportedException is being thrown instead of NoSuchAttributeException.
        // So I'll just check the memberships
        if (!isMember(groupDn, userDn)) {
          SimpleProvisioning.logger.info(connectorName + ": User " + dukeid + " not in group " + groupName);
        } else {
          throw new RuntimeException("Failed while deleting member " + dukeid + " from group " + groupName + ": " + e.getMessage(), e);
        }        
      }
    } catch (OIMUserNotFoundException e) {
      // just throw
      throw e;
    } catch (ADUserNotFoundException e) {
      // just throw
      throw e;
    } catch (Exception e) {
      throw new Exception("Error while deleting member " + dukeid + " from group " + groupName, e);
    }
  }
  
  private String getLdapKeyFromDukeID(String dukeid) throws Exception {
    
    tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) 
      super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    
    Hashtable<String, String> mhSearchCriteria = new Hashtable<String, String>();
    mhSearchCriteria.put("Users.User ID", dukeid);
    
    String[] attrs = new String[1];
    attrs[0] = AttributeData.getInstance().getOIMAttributeName("duLDAPKey");
    
    tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
    if (moResultSet.getRowCount() != 1) {
      throw new OIMUserNotFoundException("Got " + moResultSet.getRowCount() + " rows for user with dukeid " + dukeid);
    }
    
    return moResultSet.getStringValue(AttributeData.getInstance().getOIMAttributeName("duLDAPKey"));
  }
}
