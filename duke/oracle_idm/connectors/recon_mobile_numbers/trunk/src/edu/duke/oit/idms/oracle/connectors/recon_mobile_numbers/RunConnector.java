package edu.duke.oit.idms.oracle.connectors.recon_mobile_numbers;

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
  private String[] reconAttrs = {"duSMSMobile","duPSMobile"};
  
  /**
   * name of connector
   */
  public final static String connectorName = "MOBILE_RECONCILIATION";

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

    // get the mobile numbers from the database.
    HashMap<String, Map<String, String>> dbUsers = null;
    
    try {
      dbUsers = getMobileNumbersFromDatabase();
      logger.info(connectorName + ": DB search complete");
    } catch (Exception e) {
      logger.error(connectorName + ": Unable to get mobile numbers from database: " + e.toString(), e);
      return;
    }

    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf", e);
      return;
    }

    // Get a list of all users from OIM
    HashMap<String,Map<String,String>> oimUsers = null;
    
    try {
      oimUsers = getAllOIMUsers();    
      logger.info(connectorName + ": OIM search complete");
    } catch (Exception e) {
      logger.error(connectorName + ": Unable to get mobile numbers from OIM: " + e.toString(), e);
      return;
    }
   
    // Iterate over the OIM users, comparing to the db list
    Iterator<String> oimIter = oimUsers.keySet().iterator();
    while (oimIter.hasNext()) {
      String key = oimIter.next();
      Map<String,String> dbVals = dbUsers.get(key);
      Map<String,String> oimVals = oimUsers.get(key);
      
      if (dbVals == null) {
        dbVals = new HashMap<String, String>();
        for (int i = 0; i < reconAttrs.length; i++) {
          dbVals.put(reconAttrs[i], "");
        }
      }

      HashMap<String,String> changesMap = new HashMap<String,String>();
      
      for (int i = 0; i < reconAttrs.length; i++) {
        String newVal = dbVals.get(reconAttrs[i]);
        if (!newVal.equals(oimVals.get(reconAttrs[i]))){
          changesMap.put(attributeData.getOIMAttributeName(reconAttrs[i]),newVal);
        } 
      }
      
      if (!changesMap.isEmpty()) {
        try {
          // We need to create a result set containing just this user to do this update
          tcResultSet moSingleResult = findOIMUser(key);
          moUserUtility.updateUser(moSingleResult, changesMap);
          logger.info(connectorName + ": updated user: " + key);
       } catch (Exception e) {
          logger.error(connectorName + ": Error occured trying to update an OIM user(" + key + "). Error:" + e.toString(), e);
       }
      }
    }
    
    // Clean up and go home
    if (moUserUtility != null) {
      moUserUtility.close();
    }
    
    if (moITResourceUtility != null) {
      moITResourceUtility.close();
    }
    
    logger.info(connectorName + ": Task completed");
  }

 
  private HashMap<String, Map<String, String>> getMobileNumbersFromDatabase() throws Exception {
    
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    
    HashMap<String, Map<String, String>> dbUsers = new HashMap<String, Map<String, String>>();
    
    try {
      conn = getConnection();
      ps = conn.prepareStatement("select dukeid, mobile_source, mobile_number from mobile_number_view");
      rs = ps.executeQuery();
      while (rs.next()) {
        String dukeid = rs.getString(1);
        String source = rs.getString(2);
        String number = rs.getString(3);
        
        if (dbUsers.get(dukeid) == null) {
          HashMap<String, String> temp = new HashMap<String, String>();
          temp.put("duSMSMobile", "");
          temp.put("duPSMobile", "");
          dbUsers.put(dukeid, temp);
        }
        
        if (source.equals("sms")) {
          dbUsers.get(dukeid).put("duSMSMobile", number);
        } else if (source.equals("siss")) {
          dbUsers.get(dukeid).put("duPSMobile", number);
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
   * Return a hash containing duDukeId, duSMSMobile, and duPSMobile of all users in OIM.
   */
  private HashMap<String,Map<String,String>> getAllOIMUsers() throws Exception {

    Hashtable<String, String> mhSearchCriteria = new Hashtable<String,String>();
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"), "*");
    mhSearchCriteria.put("Users.Status", "Active");
    tcResultSet moResultSet = null;
    String[] getFields = new String[reconAttrs.length+1];
    for (int i = 0; i < reconAttrs.length; i++) {
      getFields[i] = attributeData.getOIMAttributeName(reconAttrs[i]);
    }
    getFields[reconAttrs.length] = attributeData.getOIMAttributeName("duDukeId");
    Integer nOimUsers;
    
    try {
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getFields);
      nOimUsers = moResultSet.getRowCount();
    } catch (tcAPIException e) {
      throw new RuntimeException(e);
    }

    HashMap<String,Map<String,String>> retval =  new HashMap<String,Map<String,String>>();

    for (int i=0; i < nOimUsers; i++) {
      moResultSet.goToRow(i);
      String id = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeID"));
      Map<String, String> oimVals = new HashMap<String, String>();
      for (int j = 0; j < reconAttrs.length; j++) {
        String val = moResultSet.getStringValue(attributeData.getOIMAttributeName(reconAttrs[j]));
        oimVals.put(reconAttrs[j], val.isEmpty() ? "":val);
      }
      retval.put(id, oimVals);
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
   * Get the connection parameters for the IT Resource PR_RECONCILIATION_GTC.
   * Then returns a connection based on those parameters.
   * @return database connection
   * @throws Exception
   */
  private Connection getConnection() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>();

    tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf)
        super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

    Map<String, String> resourceMap = new HashMap<String, String>();
    resourceMap.put("IT Resources.Name", "PR_RECONCILIATION_GTC");
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

    Class.forName((String)parameters.get("DukeDBReconTransport_driver"));

    Properties props = new Properties();
    props.put("user", parameters.get("DukeDBReconTransport_username"));
    props.put("password", parameters.get("DukeDBReconTransport_password"));
    if (parameters.get("DukeDBReconTransport_connectionProperties") != null && !parameters.get("DukeDBReconTransport_connectionProperties").equals("")) {
      String[] additionalPropsArray = ((String)parameters.get("DukeDBReconTransport_connectionProperties")).split(",");
      for (int i = 0; i < additionalPropsArray.length; i++) {
        String[] keyValue = additionalPropsArray[i].split("=");
        props.setProperty(keyValue[0], keyValue[1]);
        logger.info(connectorName + ": setting property " + keyValue[0] + "=" + keyValue[1]);
      }
    }

    Connection conn = DriverManager.getConnection((String)parameters.get("DukeDBReconTransport_url"), props);
    return conn;  
  }

}
