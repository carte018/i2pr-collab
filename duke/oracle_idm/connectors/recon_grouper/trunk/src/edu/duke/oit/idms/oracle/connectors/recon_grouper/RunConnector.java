package edu.duke.oit.idms.oracle.connectors.recon_grouper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcDuplicateGroupException;
import Thor.API.Exceptions.tcGroupNotFoundException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.dataaccess.tcDataProvider;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.prov_auth_directories.AuthDirectoriesProvisioning;
import edu.duke.oit.idms.oracle.exceptions.OIMUserNotFoundException;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * This is a task that queries an event log in the Grouper database and uses
 * that to update the group memberships in OIM.
 * @author shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private String userName = "groups";
  private String connectorName = "GROUPER_RECONCILIATION";

  private tcGroupOperationsIntf moGroupUtility;
  private tcUserOperationsIntf moUserUtility;
  private Connection conn;
  private tcDataProvider dataProvider = null;

  private ArrayList<String> groupFailures = new ArrayList<String>();
  private ArrayList<String> userFailures = new ArrayList<String>();

  Map<String, Long> dukeidToUserKeyCache = new HashMap<String, Long>();
  Map<String, Long> groupNameToGroupKeyCache = new HashMap<String, Long>();

  protected void execute() {

    logger.info(connectorName + ": Starting task.");
    
    dataProvider = super.getDataBase();

    // Get the tcGroupOperationsIntf object
    try {
      moGroupUtility = (tcGroupOperationsIntf)super.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcGroupOperationsIntf");
      return;
    }

    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf");
      return;
    }


    // Process changes in Grouper sequentially
    ResultSet rs = null;
    PreparedStatement psSelect = null;
    PreparedStatement psDelete = null;
    PreparedStatement psUpdateToFailure = null;

    try {
      conn = getConnection();
      psDelete = conn.prepareStatement("DELETE FROM "+userName+".grp_event_log_for_oim WHERE consumer = 'recon_grouper' AND record_id=?");
      psSelect = conn.prepareStatement("SELECT record_id, group_name, action, field_1, status FROM "+userName+".grp_event_log_for_oim WHERE consumer = 'recon_grouper' ORDER BY record_id");
      psUpdateToFailure = conn.prepareStatement("UPDATE "+userName+".grp_event_log_for_oim SET status='FAILED' where consumer = 'recon_grouper' AND record_id=?");

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
        } catch (OIMUserNotFoundException e) {
          logger.error(connectorName + ": User not found in OIM while processing record " + recordId + ".", e);
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

      if (conn != null) {
        try {
          conn.close();
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
  private Connection getConnection() throws Exception {
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
      String value = moResultSet
      .getStringValue("IT Resources Type Parameter Value.Value");
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

  private void processCreate(String groupName) throws Exception {
    try {
      Map<String, String>  map = new HashMap<String, String> ();
      map.put("Groups.Group Name", groupName);

      long groupKey = moGroupUtility.createGroup(map);
      logger.info(connectorName + ": Created group " + groupName);
      groupNameToGroupKeyCache.put(groupName, new Long(groupKey));
    } catch (tcDuplicateGroupException e) {
      // apparently the group already exists.  this is okay.
    } catch (Exception e) {
      throw new Exception("Error while creating group " + groupName, e);
    }
    
    // now lets create the group in the authdirs
    AuthDirectoriesProvisioning authDirProv = new AuthDirectoriesProvisioning();
    authDirProv.createGroup(dataProvider, groupName);
  }

  private void processDelete(String groupName) throws Exception {
    try {
      long groupKey = -1;
      boolean groupKeyFound = true;

      if (groupNameToGroupKeyCache.containsKey(groupName)) {
        groupKey = ((Long)groupNameToGroupKeyCache.get(groupName)).longValue();
        groupNameToGroupKeyCache.remove(groupName);
      } else {
        Map<String, String>  map = new HashMap<String, String> ();
        map.put("Groups.Group Name", groupName);
        tcResultSet moResultSet = moGroupUtility.findGroups(map);

        if (moResultSet.getRowCount() > 1) {
          throw new Exception("Got " + moResultSet.getRowCount() + " rows for group " + groupName);
        } else if (moResultSet.getRowCount() == 1) {
          groupKey = moResultSet.getLongValue("Groups.Key");
        } else {
          groupKeyFound = false;
        }
      }
      
      if (groupKeyFound == true) {
        moGroupUtility.deleteGroup(groupKey);
        logger.info(connectorName + ": Deleted group " + groupName);
      }
    } catch (tcGroupNotFoundException e) {
      // apparently the group doesn't exist.  this is okay.
    } catch (Exception e) {
      throw new Exception("Error while deleting group " + groupName, e);
    }
    
    // now lets delete the group in the authdirs
    AuthDirectoriesProvisioning authDirProv = new AuthDirectoriesProvisioning();
    authDirProv.deleteGroup(dataProvider, groupName);
  }

  private void processRename(String oldGroupName, String newGroupName) throws Exception {
    try {
      Map<String, String>  map = new HashMap<String, String> ();
      map.put("Groups.Group Name", oldGroupName);
      tcResultSet moResultSet = moGroupUtility.findGroups(map);

      if (moResultSet.getRowCount() > 1) {
        throw new Exception("Got " + moResultSet.getRowCount() + " rows for group " + oldGroupName);
      } else if (moResultSet.getRowCount() == 1) {
        long groupKey = moResultSet.getLongValue("Groups.Key");
  
        map.put("Groups.Group Name", newGroupName);
        moGroupUtility.updateGroup(moResultSet, map);
  
        groupNameToGroupKeyCache.remove(oldGroupName);
        groupNameToGroupKeyCache.put(newGroupName, new Long(groupKey));
  
        logger.info(connectorName + ": Renamed group " + oldGroupName + " to " + newGroupName);
      } else {
        // the group may already have been renamed if we get here.
        map = new HashMap<String, String> ();
        map.put("Groups.Group Name", newGroupName);
        moResultSet = moGroupUtility.findGroups(map);
        
        if (moResultSet.getRowCount() == 0) {
          throw new Exception("During rename, neither old group name nor new group name was found in OIM.");
        } else if (moResultSet.getRowCount() > 1) {
          throw new Exception("unexpected");
        }
      }
    } catch (Exception e) {
      throw new Exception("Error while renaming group " + oldGroupName + " to " + newGroupName, e);
    }
    
    // now lets rename the group in the authdirs
    AuthDirectoriesProvisioning authDirProv = new AuthDirectoriesProvisioning();
    authDirProv.renameGroup(dataProvider, oldGroupName, newGroupName);
  }

  private void processAddMember(String groupName, String dukeid) throws Exception {
    long groupKey;
    long userKey;

    if (groupNameToGroupKeyCache.containsKey(groupName)) {
      groupKey = ((Long)groupNameToGroupKeyCache.get(groupName)).longValue();
    } else {
      Map<String, String>  map = new HashMap<String, String> ();
      map.put("Groups.Group Name", groupName);
      tcResultSet moResultSet = moGroupUtility.findGroups(map);

      if (moResultSet.getRowCount() != 1) {
        throw new Exception("Got " + moResultSet.getRowCount() + " rows for group " + groupName);
      }

      groupKey = moResultSet.getLongValue("Groups.Key");
      groupNameToGroupKeyCache.put(groupName, new Long(groupKey));
    }

    if (dukeidToUserKeyCache.containsKey(dukeid)) {
      userKey = ((Long)dukeidToUserKeyCache.get(dukeid)).longValue();
    } else {
      userKey = OIMAPIWrapper.getUserKeyFromDukeID(moUserUtility, dukeid);
      dukeidToUserKeyCache.put(dukeid, new Long(userKey));
    }

    try {
      moGroupUtility.addMemberUser(groupKey, userKey);
      logger.info(connectorName + ": Added member " + dukeid + " to " + groupName);
    } catch (tcAPIException e) {
      // If we get an exception and it's because the user was already a member of the group,
      // we're going to log a warn instead of throwing an error.

      try {
        if (moGroupUtility.isUserGroupMember(groupKey, userKey).booleanValue() == true) {
          logger.warn(connectorName + ": Received an exception while adding member " + dukeid + " to " + groupName +
          ". However, after the error, it was verified that the user is a member of the group, so this connector will proceed normally.");
        } else {
          throw e;
        }
      } catch (Exception e2) {
        // But if we get an error while doing this check, we'll throw the original error.
        throw e;
      }
    }
    
    // now lets add the membership in the authdirs
    AuthDirectoriesProvisioning authDirProv = new AuthDirectoriesProvisioning();
    authDirProv.addMember(dataProvider, groupName, dukeid);
  }

  private void processDeleteMember(String groupName, String dukeid) throws Exception {
    long groupKey;
    long userKey;

    if (groupNameToGroupKeyCache.containsKey(groupName)) {
      groupKey = ((Long)groupNameToGroupKeyCache.get(groupName)).longValue();
    } else {
      Map<String, String>  map = new HashMap<String, String> ();
      map.put("Groups.Group Name", groupName);
      tcResultSet moResultSet = moGroupUtility.findGroups(map);

      if (moResultSet.getRowCount() != 1) {
        throw new Exception("Got " + moResultSet.getRowCount() + " rows for group " + groupName);
      }

      groupKey = moResultSet.getLongValue("Groups.Key");
      groupNameToGroupKeyCache.put(groupName, new Long(groupKey));
    }

    if (dukeidToUserKeyCache.containsKey(dukeid)) {
      userKey = ((Long)dukeidToUserKeyCache.get(dukeid)).longValue();
    } else {
      userKey = OIMAPIWrapper.getUserKeyFromDukeID(moUserUtility, dukeid);
      dukeidToUserKeyCache.put(dukeid, new Long(userKey));
    }

    try {
      moGroupUtility.removeMemberUser(groupKey, userKey);
      logger.info(connectorName + ": Deleted member " + dukeid + " from " + groupName);
    } catch (tcAPIException e) {
      // If we get an exception and it's because the user was not a member of the group,
      // we're going to log a warn instead of throwing an error.

      try {
        if (moGroupUtility.isUserGroupMember(groupKey, userKey).booleanValue() == false) {
          logger.warn(connectorName + ": Received an exception while deleting member " + dukeid + " from " + groupName +
          ". However, after the error, it was verified that the user is not a member of the group, so this connector will proceed normally.");
        } else {
          throw e;
        }
      } catch (Exception e2) {
        // But if we get an error while doing this check, we'll throw the original error.
        throw e;
      }
    }
    
    // now lets delete the membership from the authdirs
    AuthDirectoriesProvisioning authDirProv = new AuthDirectoriesProvisioning();
    authDirProv.deleteMember(dataProvider, groupName, dukeid);
  }
}
