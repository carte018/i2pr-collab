package edu.duke.oit.idms.oracle.connectors.prov_eprint;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;

/**
 * @author shilen
 */
public class EPrintDBConnectionWrapper {

  private static EPrintDBConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  // objects for connection pool
  private ObjectPool connectionPool = null;
  private ConnectionFactory connectionFactory = null;
  private PoolableConnectionFactory poolableConnectionFactory = null;
  private PoolingDriver driver = null;
  private String poolKey = "proveprint";
  private String poolUrl = "jdbc:apache:commons:dbcp:" + poolKey;
  
  
  private EPrintDBConnectionWrapper(tcDataProvider dataProvider) {
    // nothing to do here
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static EPrintDBConnectionWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new EPrintDBConnectionWrapper(dataProvider);
      instance.createDatabasePool();
      SimpleProvisioning.logger.info(EPrintProvisioning.connectorName + ": Created new instance of EPrintDBConnectionWrapper.");
    }
    
    instance.dataProvider = dataProvider;
    return instance;
  }

  /**
   * Create a database pool based on properties defined in the IT Resource.
   */
  private void createDatabasePool() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
          .getUtility(dataProvider,
              "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map parameters = new HashMap();
      Map resourceMap = new HashMap();
      resourceMap.put("IT Resources.Name", "EPRINT_PROVISIONING");
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
      Class.forName("org.apache.commons.dbcp.PoolingDriver");

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
      
      connectionPool = new GenericObjectPool(null);
      connectionFactory = new DriverManagerConnectionFactory((String)parameters.get("db.url"), props);
      poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, connectionPool, null, "SELECT 1", false, true);
      driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
      driver.registerPool(poolKey, connectionPool);
      SimpleProvisioning.logger.info(EPrintProvisioning.connectorName + ": Created database pool.");
    } catch (Exception e) {
      throw new RuntimeException("Failed while creating DB pool: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }

  /**
   * Close database connection when this class is about to be garbage collected.
   */
  protected void finalize() throws Throwable {
    if (driver != null) {
      try {
        driver.closePool(poolKey);
        driver = null;
      } catch (SQLException e) {
        // this is okay
      }
    }
    
    super.finalize();
  }
  
  /**
   * @param duLDAPKey
   * @return whether or not this user is provisioned
   */
  public boolean isProvisioned(String duLDAPKey) {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs;
    
    try {
      conn = this.getConnection();
      ps = conn.prepareStatement("select count(*) as count from dirxml_people where guid = ?");
      ps.setString(1, duLDAPKey);
      rs = ps.executeQuery();
      
      if (rs.next()) {
        int count = rs.getInt("count");
        if (count > 0) {
          return true;
        }
      }
      
      return false;
    } catch (SQLException e) {
      throw new RuntimeException("Failed while querying the database: " + e.getMessage(), e);
    } finally {
    
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    }
  }
  
  
  /**
   * @param duLDAPKey
   */
  public void deleteUser(String duLDAPKey) {

    Connection conn = null;
    PreparedStatement ps = null;
    
    try {
      conn = this.getConnection();
      ps = conn.prepareStatement("delete from dirxml_people where guid = ?");
      ps.setString(1, duLDAPKey);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed while deleting user from database: " + e.getMessage(), e);
    } finally {
    
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    }
  }
  
  /**
   * @param duLDAPKey
   * @param column
   * @param value
   */
  public void updateUser(String duLDAPKey, String column, String value) {

    Connection conn = null;
    PreparedStatement ps = null;
    
    try {
      conn = this.getConnection();
      ps = conn.prepareStatement("update dirxml_people set " + column + " = ? where guid = ?");
      if (value == null) {
        ps.setNull(1, Types.VARCHAR);
      } else {
        ps.setString(1, value);
      }
      ps.setString(2, duLDAPKey);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed while updating user in database: " + e.getMessage(), e);
    } finally {
    
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    }
  }
  
  /**
   * @param attributes
   */
  public void addUser(Map attributes) {

    Connection conn = null;
    PreparedStatement ps = null;
    
    String sql = "insert into dirxml_people (";
    Set columns = attributes.keySet();
    Iterator iter = columns.iterator();
    int count = 0;
    while (iter.hasNext()) {
      String column = (String)iter.next();
      sql += column;
      if (iter.hasNext()) {
        sql += ", ";
      }
      count++;
    }
    
    sql += ") values (?";
    for (int i = 0; i < count - 1; i++) {
      sql += ", ?";
    }
    
    sql += ")";
    
    try {
      conn = this.getConnection();
      ps = conn.prepareStatement(sql);
      
      iter = columns.iterator();
      count = 0;
      while (iter.hasNext()) {
        count++;
        String value = (String)attributes.get(iter.next());
        if (value == null) {
          ps.setNull(count, Types.VARCHAR);
        } else {
          ps.setString(count, value);
        }
      }

      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed while inserting user in database: " + e.getMessage(), e);
    } finally {
    
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // ignore this
        }
      }
    }
  }
  
  private Connection getConnection() throws SQLException {
    try {
      Connection conn = DriverManager.getConnection(poolUrl);
      return conn;
    } catch (Exception e) {
      // if we get an exception, we'll try to close the pool and re-open it.
      if (driver != null) {
        try {
          driver.closePool(poolKey);
        } catch (Exception e2) {
          // ignore this
        }
      }
      
      connectionPool = null;
      connectionFactory = null;
      poolableConnectionFactory = null;
      driver = null;
      
      this.createDatabasePool();
      Connection conn = DriverManager.getConnection(poolUrl);
      return conn;
    }
  }
}
