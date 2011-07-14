package edu.duke.oit.idms.oracle.connectors.recon_grouper_to_service_directories;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;


import Thor.API.tcResultSet;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;


/**
 * This is a task that queries an event log in the Grouper database and uses
 * that to update the group memberships in the Service Directory/LDAP.
 * @author liz, stolen from shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  //static Logger logger = Logger.getLogger(RunConnector.class);
  private String userName = "groups";
  private String connectorName = "GROUPER_TO_SERVICE_DIRECTORIES_RECONCILIATION";

  private tcGroupOperationsIntf moGroupUtility;
  private tcUserOperationsIntf moUserUtility;
  private Connection grouperConn;
  private LDAPConnectionWrapper ldapConnectionWrapper = null;

  private ArrayList<String> groupFailures = new ArrayList<String>();
  private ArrayList<String> userFailures = new ArrayList<String>();

  Map<String, String> dukeidToUserKeyCache = new HashMap<String, String>();
  Map<String, String> groupNameToGroupKeyCache = new HashMap<String, String>();

  private boolean DEBUG = false;

  protected void execute() {
    logger.info(connectorName + ": Starting task.");

//    //Get the tcGroupOperationsIntf object
//    try {
//      moGroupUtility = (tcGroupOperationsIntf)super.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
//    } catch (tcAPIException e) {
//      logger.error(connectorName + ": Unable to get an instance of tcGroupOperationsIntf");
//      return;
//    }
//
//    //Get the tcUserOperationsIntf object
//    try {
//      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
//    } catch (tcAPIException e) {
//      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf");
//      return;
//    }


    // Process changes in Grouper sequentially
    ResultSet rs = null;
    PreparedStatement psSelect = null;
    PreparedStatement psDelete = null;
    PreparedStatement psUpdateToFailure = null;

    try {
      ldapConnectionWrapper = new LDAPConnectionWrapper(DEBUG);

      grouperConn = getGrouperConnection();
      psDelete = grouperConn.prepareStatement("DELETE FROM "+userName+".grp_event_log_for_oim WHERE consumer = 'grouper_ldap' AND record_id=?");
      psSelect = grouperConn.prepareStatement("SELECT record_id, group_name, action, field_1, status FROM "+userName+".grp_event_log_for_oim WHERE consumer = 'grouper_ldap' ORDER BY record_id");
      psUpdateToFailure = grouperConn.prepareStatement("UPDATE "+userName+".grp_event_log_for_oim SET status='FAILED' where consumer = 'grouper_ldap' AND record_id=?");

      rs = psSelect.executeQuery();
      while (rs.next()) {
        long recordId = rs.getLong(1);
        String groupName = rs.getString(2);
        String action = rs.getString(3);
        String field1 = rs.getString(4);
        String status = rs.getString(5);

        if (status.equals("FAILED")) {
          updateFailures(action, groupName, field1);
          continue;
        }
        
        groupName = urnizeGroupName(groupName);

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
          if (action.equals("create")) {
            processCreate(groupName);
          } else if (action.equals("delete")) {
            processDelete(groupName);
          } else if (action.equals("rename")) {
            processRename(groupName, field1);
          } else if (action.equals("add_member")) {
            processAddMember(groupName, field1);
          } else if (action.equals("delete_member")) {
            processDeleteMember(groupName, field1);
          } else {
            throw new Exception("Unknown action for record id " + recordId);
          }

          // If we get here, then the OIM update succeeded.
          isSuccess = true;
        } catch (Exception e2) {
          logger.error(connectorName + ": Error while processing record " + recordId + ".", e2);
        }

        if (isSuccess) {
          // remove the event from the grouper db
          psDelete.setLong(1, recordId);
          psDelete.executeUpdate();
        } else {
          // mark the event as failed
          psUpdateToFailure.setLong(1, recordId);
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

      if (grouperConn != null) {
        try {
          grouperConn.close();
        } catch (SQLException e) {
          // this is okay
        }
      }

      if (moGroupUtility != null) {
        moGroupUtility.close();
      }

      if (moUserUtility != null) {
        moUserUtility.close();
      }
    }

    logger.info(connectorName + ": Ending task.");
  }

  private String urnizeGroupName(String groupName) {
    // The group name needs to be massaged a bit
    // Strip the "duke" at the beginning and replace it with "urn:mace:duke.edu:groups"
    groupName = groupName.replaceFirst("duke", "urn:mace:duke.edu:groups");
    return groupName;
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
  private Connection getGrouperConnection() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>();

    if (!DEBUG) {
      tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
      super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map<String, String> resourceMap = new HashMap<String, String>();
      resourceMap.put("IT Resources.Name", "GROUPER_RECONCILIATION"); // We use the Grouper Recon's connection
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
    } else {
      parameters.put("username","SUPPRESSED");
      parameters.put("password", SUPPRESSED");
      parameters.put("connectionProperties", "oracle.net.encryption_client=required,oracle.net.encryption_types_client=(RC4_256),oracle.net.crypto_checksum_client=required,oracle.net.crypto_checksum_types_client=(MD5)");
      parameters.put("url", "jdbc:oracle:thin:@SUPPRESSED:SUPPRESSED:SUPPRESSED");
      parameters.put("driver", "oracle.jdbc.driver.OracleDriver");
    }

    Class.forName((String) parameters.get("driver"));
    userName = (String) parameters.get("username");
    Properties props = new Properties();
    props.put("user", parameters.get("username"));
    props.put("password", parameters.get("password"));
    if (parameters.get("connectionProperties") != null && !parameters.get("connectionProperties").equals("")) {
      String[] additionalPropsArray = ((String) parameters.get("connectionProperties")).split(",");
      for (int i = 0; i < additionalPropsArray.length; i++) {
        String[] keyValue = additionalPropsArray[i].split("=");
        props.setProperty(keyValue[0], keyValue[1]);
      }
    }

    Connection conn = DriverManager.getConnection((String) parameters.get("url"), props);
    return conn;   
  }


  private void processCreate(String groupName) throws Exception {
    try {
      // Currently group information isn't stored in LDAP.  This is just a placeholder
      logger.info(connectorName + ": I would have created group " + groupName);
    } catch (Exception e) {
      throw new Exception("Error while creating group " + groupName, e);
    }
  }

  private void processDelete(String groupName) throws Exception {
    try {
      // Currently group information isn't stored in LDAP.  This is just a placeholder
      logger.info(connectorName + ": I would have deleted group " + groupName);
    } catch (Exception e) {
      throw new Exception("Error while deleting group " + groupName, e);
    }
  }

  private void processRename(String oldGroupName, String newGroupName) throws Exception {
    // We are assuming they never rename a "Course" group.  If they did rename such a group
    // we would need to alter the educoursemember and educourseoffering fields.
    
    newGroupName = urnizeGroupName(newGroupName);

    Vector<String> failedList;
    failedList = ldapConnectionWrapper.renameGroup(oldGroupName,newGroupName);
    if (!failedList.isEmpty()) {
      logger.error("failed to change memberships for:"+failedList.toString());
      throw new Exception("Unable to rename the group: "+oldGroupName);
    } else {
      logger.info(connectorName + ": Actually renamed group " + oldGroupName + " to "+ newGroupName);
    }
  }

  private void processAddMember(String groupName, String dukeid) throws Exception {
//    here's an example in production
//    ismemberof: urn:mace:duke.edu:groups:siss:courses:TEST:102:01:1000:9999:students
//    educoursemember: Learner@urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
//    educourseoffering: urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
    boolean success = ldapConnectionWrapper.addMemberToGroup(dukeid,groupName);
    logger.info("addMemberToGroup returned"+success);
    if (!success) 
      throw new Exception("Unable to add "+dukeid+" to the group: "+groupName);
    logger.info(connectorName + ": added " + dukeid + " to "+ groupName);

  }

  private void processDeleteMember(String groupName, String dukeid) throws Exception {
    boolean success = ldapConnectionWrapper.removeMemberFromGroup(dukeid,groupName);
    logger.debug("removeMemberFromGroup returned:"+success);
    if (!success) 
      throw new Exception("Unable to remove "+dukeid+" from the group: "+groupName);
    logger.info(connectorName + ": deleted " + dukeid + " to "+ groupName);
  }
}
