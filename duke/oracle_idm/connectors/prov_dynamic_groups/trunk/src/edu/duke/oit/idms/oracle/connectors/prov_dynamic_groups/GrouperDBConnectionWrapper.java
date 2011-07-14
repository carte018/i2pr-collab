package edu.duke.oit.idms.oracle.connectors.prov_dynamic_groups;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;

/**
 * @author shilen
 */
public class GrouperDBConnectionWrapper {

  private static GrouperDBConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  // this is a connection to the grouper database  
  private Connection databaseConnection = null;
  
  private GrouperDBConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.databaseConnection = createDatabaseConnection();
    
    SimpleProvisioning.logger.info(DynamicGroupsProvisioning.connectorName + ": Created new instance of GrouperDBConnectionWrapper.");
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static GrouperDBConnectionWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new GrouperDBConnectionWrapper(dataProvider);
    } else {
      instance.dataProvider = dataProvider;
    }

    return instance;
  }

  /**
   * Create a new connection to the DB based on properties defined in the IT Resource.
   * @return database connection
   */
  private Connection createDatabaseConnection() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
          .getUtility(dataProvider,
              "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map parameters = new HashMap();
      Map resourceMap = new HashMap();
      resourceMap.put("IT Resources.Name", "DYNGRP_PROVISIONING");
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

      Class.forName((String)parameters.get("db.driver"));

      Properties props = new Properties();
      props.put("user", parameters.get("db.username"));
      props.put("password", parameters.get("db.password"));
      if (parameters.get("db.connectionProperties") != null && !parameters.get("db.connectionProperties").equals("")) {
        String[] additionalPropsArray = ((String)parameters.get("db.connectionProperties")).split(",");
        for (int i = 0; i < additionalPropsArray.length; i++) {
          String[] keyValue = additionalPropsArray[i].split("=");
          props.setProperty(keyValue[0], keyValue[1]);
        }
      }

      Connection conn = DriverManager.getConnection((String)parameters.get("db.url"), props);
      return conn;  

    } catch (Exception e) {
      throw new RuntimeException("Failed while creating DB connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }
  

  private PreparedStatement prepareQuery() {
    try {
      return this.databaseConnection.prepareStatement("select * from custom_dynamic_groups_v where dukeid = ?");
    } catch (SQLException e) {
      throw new RuntimeException("Failed while preparing statement: " + e.getMessage(), e);
    }
  }

  /**
   * Reconnect to the database
   */
  private void reconnect() {
    try {
      databaseConnection.close();
    } catch (SQLException e) {
      // this is okay
    }

    this.databaseConnection = createDatabaseConnection();
    SimpleProvisioning.logger.info(DynamicGroupsProvisioning.connectorName + ": Reconnected to the database.");
  }


  /**
   * Close database connection when this class is about to be garbage collected.
   */
  protected void finalize() throws Throwable {
    if (databaseConnection != null) {
      try {
        databaseConnection.close();
      } catch (SQLException e) {
        // this is okay
      }
    }
    
    super.finalize();
  }
  
  /**
   * @param dukeid
   * @return set of dynamic groups the user belongs to
   */
  public Set getDynamicGroupsForUser(String dukeid) {
    Set groups = new HashSet();
    
    PreparedStatement databasePS = null;
    ResultSet rs;
    
    try {
      try {
        databasePS = prepareQuery();
        databasePS.setString(1, dukeid);
        rs = databasePS.executeQuery();
      } catch (SQLException e) {
        // we'll try again after reconnecting...
        reconnect();
        
        databasePS = prepareQuery();
        databasePS.setString(1, dukeid);
        rs = databasePS.executeQuery();
      }
      
      while (rs.next()) {
        String groupName = rs.getString("GROUP_NAME");
        if (groupName != null) {
          groups.add(groupName);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed while querying the database: " + e.getMessage(), e);
    } finally {
      if (databasePS != null) {
        try {
          databasePS.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    }
    
    return groups;
  }
}
