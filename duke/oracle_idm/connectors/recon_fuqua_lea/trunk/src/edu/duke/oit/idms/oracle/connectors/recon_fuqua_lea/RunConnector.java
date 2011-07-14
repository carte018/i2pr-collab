package edu.duke.oit.idms.oracle.connectors.recon_fuqua_lea;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.util.AttributeData;

/**
 * @author shilen
 */
public class RunConnector extends SchedulerBaseTask {
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private tcUserOperationsIntf moUserUtility;
  private AttributeData attributeData;
  
  /**
   * name of connector
   */
  public final static String connectorName = "FUQUALEA_RECONCILIATION";

  protected void execute() {
    logger.info(connectorName + ": Starting task.");
    attributeData = AttributeData.getInstance();
        
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;
    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcITResourceInstanceOperationsIntf", e);
      return;
    }

    // get the LEAs from the database.
    HashMap<String, HashMap<String, String>> dbUsers = null;
    
    try {
      dbUsers = getLEAsFromDatabase();
      logger.info(connectorName + ": DB search complete");
    } catch (Exception e) {
      logger.error(connectorName + ": Unable to get LEAs from database: " + e.toString(), e);
      return;
    }

    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf", e);
      return;
    }

    // Get a list of users from OIM with LEAs
    HashMap<String, HashMap<String, String>> oimUsers = null;
    
    try {
      oimUsers = getAllOIMUsers();    
      logger.info(connectorName + ": OIM search complete");
    } catch (Exception e) {
      logger.error(connectorName + ": Unable to get LEAs from OIM: " + e.toString(), e);
      return;
    }
   
    // Iterate over the OIM users, comparing to the db list
    Iterator<String> oimIter = oimUsers.keySet().iterator();
    while (oimIter.hasNext()) {
      String dukeid = oimIter.next();
      String oldAlias = oimUsers.get(dukeid).get("duFuquaLEA");
      String oldTarget = oimUsers.get(dukeid).get("duFuquaLEATarget");
      
      String newAlias = "";
      String newTarget = "";
      
      HashMap<String, String> newMap = dbUsers.get(dukeid);
      if (newMap != null) {
        newAlias = newMap.get("duFuquaLEA");
        newTarget = newMap.get("duFuquaLEATarget");
      }
      
      Map<String, String> changesMap = new HashMap<String, String>();
      if (!oldAlias.equals(newAlias)) {
        changesMap.put("USR_UDF_FUQUALEA", newAlias);
      }
      
      if (!oldTarget.equals(newTarget)) {
        changesMap.put("USR_UDF_FUQUALEATARGET", newTarget);
      }
      
      if (changesMap.size() > 0) {
        try {
          // We need to create a result set containing just this user to do this update
          tcResultSet moSingleResult = findOIMUser(dukeid);
          moUserUtility.updateUser(moSingleResult, changesMap);
          logger.info(connectorName + ": updated user: " + dukeid);
        } catch (Exception e) {
          logger.error(connectorName + ": Error occured trying to update an OIM user(" + dukeid + "). Error:" + e.toString(), e);
        } 
      }
    }
    
    // Clean up
    if (moUserUtility != null) {
      moUserUtility.close();
    }
    
    if (moITResourceUtility != null) {
      moITResourceUtility.close();
    }
    
    logger.info(connectorName + ": Task completed");
  }

 
  private HashMap<String, HashMap<String, String>> getLEAsFromDatabase() throws Exception {
    
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    
    HashMap<String, HashMap<String, String>> dbUsers = new HashMap<String, HashMap<String, String>>();
    
    try {
      conn = getConnection();
      ps = conn.prepareStatement("select dukeid, alias, target from fuqua_alias where type in ('ALUM', 'F-MBA', 'F-WBA', 'F-CBA', 'F-GBA', 'F-NON', 'F-FMS', 'F-MMS', 'F-MCI') and dukeid is not null and alias is not null and target is not null and active='-1'");
      rs = ps.executeQuery();
      while (rs.next()) {
        String dukeid = rs.getString(1).trim();
        String alias = rs.getString(2).trim();
        String target = rs.getString(3).trim();
        
        if (!dukeid.equals("") && !alias.equals("") && !target.equals("")) {
          HashMap<String, String> map = new HashMap<String, String>();
          map.put("duFuquaLEA", alias);
          map.put("duFuquaLEATarget", target);
          dbUsers.put(dukeid, map);
        }
      }
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (ps != null) {
        try {
          ps.close();
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
    }

    return dbUsers;
  }


  /**
   * Return a hash containing attributes in OIM
   */
  private HashMap<String, HashMap<String, String>> getAllOIMUsers() throws Exception {

    Hashtable<String, String> mhSearchCriteria = new Hashtable<String,String>();
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"), "*");
    mhSearchCriteria.put("Users.Status", "Active");
    tcResultSet moResultSet = null;
    String[] getFields = new String[3];
    getFields[0] = attributeData.getOIMAttributeName("duDukeId");
    getFields[1] = "USR_UDF_FUQUALEA";
    getFields[2] = "USR_UDF_FUQUALEATARGET";

    Integer nOimUsers;
    
    try {
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getFields);
      nOimUsers = moResultSet.getRowCount();
    } catch (tcAPIException e) {
      throw new RuntimeException(e);
    }

    HashMap<String, HashMap<String,String>> retval =  new HashMap<String, HashMap<String,String>>();

    for (int i=0; i < nOimUsers; i++) {
      moResultSet.goToRow(i);
      String id = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeID"));
      String alias = moResultSet.getStringValue("USR_UDF_FUQUALEA");
      String target = moResultSet.getStringValue("USR_UDF_FUQUALEATARGET");
      if (alias == null) {
        alias = "";
      }
      
      if (target == null) {
        target = "";
      }
      
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("duFuquaLEA", alias);
      map.put("duFuquaLEATarget", target);
      retval.put(id, map);
    }
    
    return retval;
  }
  
  private tcResultSet findOIMUser(String duDukeId) throws Exception {
    String[] attrs = new String[2];
    attrs[0] = "Users.Key";
    attrs[1] = "Users.User ID";
 
    Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
    if (attributeData == null) {
      attributeData = AttributeData.getInstance();
    }
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeId"), duDukeId);
    mhSearchCriteria.put("Users.Status", "Active");

    tcResultSet moResultSet;
    try {
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
      if (moResultSet.getRowCount() > 1) {
        throw new Exception("Got " + moResultSet.getRowCount() + " rows for duDukeId " + duDukeId);
      }
    } catch (tcAPIException e) {
      throw new Exception(e);
    }
    
    return moResultSet;
  }
  
  /**
   * Get the connection parameters for the IT Resource FUQUALEA_RECONCILIATION.
   * Then returns a connection based on those parameters.
   * @return database connection
   * @throws Exception
   */
  private Connection getConnection() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>();

    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf)
        super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

    Map<String, String> resourceMap = new HashMap<String, String>();
    resourceMap.put("IT Resources.Name", "FUQUALEA_RECONCILIATION");
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
    
    logger.info(connectorName + ": creating database connection");

    Class.forName((String)parameters.get("db.driver"));

    Properties props = new Properties();
    props.put("user", parameters.get("db.username"));
    props.put("password", parameters.get("db.password"));
    if (parameters.get("db.connectionProperties") != null && !parameters.get("db.connectionProperties").equals("")) {
      String[] additionalPropsArray = ((String)parameters.get("db.connectionProperties")).split(",");
      for (int i = 0; i < additionalPropsArray.length; i++) {
        String[] keyValue = additionalPropsArray[i].split("=");
        props.setProperty(keyValue[0], keyValue[1]);
        logger.info(connectorName + ": setting property " + keyValue[0] + "=" + keyValue[1]);
      }
    }

    Connection conn = DriverManager.getConnection((String)parameters.get("db.url"), props);
    return conn;  
  }

}
