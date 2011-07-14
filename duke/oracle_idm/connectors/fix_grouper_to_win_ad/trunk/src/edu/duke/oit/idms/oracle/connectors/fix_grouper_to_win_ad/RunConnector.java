package edu.duke.oit.idms.oracle.connectors.fix_grouper_to_win_ad;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import Thor.API.tcResultSet;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;


/**
 * This task will try to reissue changes that seem to be stuck in one way or another.
 * @author shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private String userName = "groups";
  private String connectorName = "FIX_GROUPER_TO_WIN_AD_RECONCILIATION";

  private Connection conn;
  private LdapContext context;

  private Pattern coursePattern = Pattern.compile("^siss:courses:(\\S+):(\\S+):(\\S+):(\\S+):(\\S+):(\\S+)$", Pattern.CASE_INSENSITIVE);

  Map<String, String> termCodeToTermNameCache = new HashMap<String, String>();
  
  String rootContainer = null;

  protected void execute() {

    logger.info(connectorName + ": Starting task.");

    ResultSet rs = null;
    ResultSet rs2 = null;
    ResultSet rs3 = null;
    PreparedStatement psSelect = null;
    PreparedStatement psSelectGroup = null;
    PreparedStatement psDelete = null;
    PreparedStatement psInsert = null;
    PreparedStatement psInsertMembers = null;
    PreparedStatement psSelectTerms = null;

    try {
      conn = getDatabaseConnection();
      context = getLDAPConnection();
      
      psDelete = conn.prepareStatement("delete from " + userName + ".grp_event_log_for_oim where consumer = 'grouper_ad' and group_name=?");
      psSelect = conn.prepareStatement("select group_name from " + userName + ".grp_event_log_for_oim where consumer='grouper_ad' and status='FAILED' and group_name like 'duke:siss:courses:%' and group_name not in (select group_name from grp_event_log_for_oim where consumer='grouper_ad' and action not in ('add_member', 'delete_member'))");
      psInsert = conn.prepareStatement("insert into " + userName + ".grp_event_log_for_oim (record_id, group_name, action, consumer) values (" + userName + ".GRP_TO_OIM_SEQ.nextval, ?, ?, ?)");
      psInsertMembers = conn.prepareStatement("insert into " + userName + ".grp_event_log_for_oim (record_id, group_name, action, consumer, field_1) select " + userName + ".GRP_TO_OIM_SEQ.nextval, group_name, 'add_member', 'grouper_ad', subject_id from " + userName + ".grouper_memberships_lw_v where group_name=? and list_type='list' and list_name='members' and subject_source='jndiperson'");
      psSelectGroup = conn.prepareStatement("select count(*) as count from " + userName + ".grouper_groups_v where name=?");
      psSelectTerms = conn.prepareStatement("SELECT term_code, term_name FROM " + userName + ".grp_course_term_code_to_name");

      // first cache term values
      rs3 = psSelectTerms.executeQuery();
      while (rs3.next()) {
        termCodeToTermNameCache.put(rs3.getString(1), rs3.getString(2));
      }
      
      // now go through events
      rs = psSelect.executeQuery();
      while (rs.next()) {
        String groupName = rs.getString(1);
        String translatedGroupName = translateGroupName(groupName);
        String dn = getLDAPGroupDnFromGroupName(translatedGroupName);
        
        logger.info("Checking existance of DN: " + dn);
        if (findEntryByDn(dn, new String[0]) != null) {
          continue;
        }
        
        logger.info("DN does not exist so resync needed: " + dn);
        
        // delete old changes
        psDelete.setString(1, groupName);
        psDelete.execute();
        
        // check grouper if group still exists...
        psSelectGroup.setString(1, groupName);
        rs2 = psSelectGroup.executeQuery();
        rs2.next();
        int count = rs2.getInt(1);
        
        if (count != 1) {
          logger.info("Group does not exist anymore so resync not needed actually.");
          conn.commit();
          continue;
        }
        
        // add group
        psInsert.setString(1, groupName);
        psInsert.setString(2, "create");
        psInsert.setString(3, "grouper_ad");
        psInsert.execute();
        
        // add members
        psInsertMembers.setString(1, groupName);
        psInsertMembers.execute();
        
        conn.commit();
      }
    } catch (Exception e) {
      logger.error(connectorName + ": Error while running connector.", e);

      if (conn != null) {
        try {
          conn.rollback();
        } catch (Exception e2) {
          // ignore
        }
      }

    } finally {
      if (rs2 != null) {
        try {
          rs2.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (rs3 != null) {
        try {
          rs3.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psInsert != null) {
        try {
          psInsert.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psInsertMembers != null) {
        try {
          psInsertMembers.close();
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
      
      if (psSelectTerms != null) {
        try {
          psSelectTerms.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (psSelectGroup != null) {
        try {
          psSelectGroup.close();
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
    conn.setAutoCommit(false);
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
}
