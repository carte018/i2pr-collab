package edu.duke.oit.idms.oracle.connectors.recon_service_directories;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.util.ConnectorConfig;
import edu.duke.oit.idms.registry.DBAttribute;
import edu.duke.oit.idms.registry.util.IDMSRegistryException;
import edu.duke.oit.idms.urn.ExternalURNBroker;
import edu.duke.oit.idms.urn.URNLookup;


/**
 * @author shilen
 *
 */
public class PersonRegistryHelper {
  
  private ConnectorConfig cfg = ConnectorConfig.getInstance();
  protected org.apache.log4j.Logger LOG = Logger.getLogger(PersonRegistryHelper.class);
  private ExternalURNBroker broker = null;
  private Connection conn = null;
  private String poolKey = "reconservicedirectories";
  private PreparedStatement nextPRID = null;
  private PreparedStatement newPersonEntry = null;
  private CallableStatement setValue = null;
  private CallableStatement removeValue = null;
  private CallableStatement removeAllValues = null;
  private CallableStatement deletePersonEntry = null;
  private PreparedStatement pridFromLDAPKey = null;
  
  private LinkedList<Integer> storedProcedureTimes = new LinkedList<Integer>();
  private int minSQLTime = Integer.parseInt(cfg.getProperty("pr.minsqltime"));
  private int minSQLCalls = Integer.parseInt(cfg.getProperty("pr.minsqlcalls"));

  
  /**
   * @throws ClassNotFoundException 
   * @throws SQLException 
   */
  public PersonRegistryHelper() throws ClassNotFoundException, SQLException {
    Class.forName(cfg.getProperty("pr.driver"));
    Class.forName("org.apache.commons.dbcp.PoolingDriver");
    init();
  }
  
  private void init() throws SQLException {
    LOG.info("Starting up Person Registry Helper connections.");

    // set up connection pooling with ExternalURNBroker
    Properties props = new Properties();
    props.setProperty("user", cfg.getProperty("pr.username"));
    props.setProperty("password", cfg.getProperty("pr.password"));
    String additionalProps = cfg.getProperty("pr.props");
    if (additionalProps != null && !additionalProps.equals("")) {
      String[] additionalPropsArray = additionalProps.split(",");
      for (int i = 0; i < additionalPropsArray.length; i++) {
        String[] keyValue = additionalPropsArray[i].split("=");
        props.setProperty(keyValue[0], keyValue[1]);
      }
    }

    ObjectPool connectionPool = new GenericObjectPool(null);
    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
        cfg.getProperty("pr.url"), 
        props);
    GenericKeyedObjectPoolFactory statementPool = new GenericKeyedObjectPoolFactory(
        null);
    new PoolableConnectionFactory(connectionFactory, connectionPool, statementPool,
        "SELECT SYSDATE FROM DUAL", false, true).setDefaultAutoCommit(false);

    PoolingDriver poolingDriver = (PoolingDriver) DriverManager
        .getDriver("jdbc:apache:commons:dbcp:");
    poolingDriver.registerPool(poolKey, connectionPool);

    String realUrl = "jdbc:apache:commons:dbcp:" + poolKey;

    broker = new ExternalURNBroker(cfg.getProperty("pr.driver"), realUrl, cfg
        .getProperty("pr.username"), cfg.getProperty("pr.password"));

    // Set up a connection to make modifications to the person registry.
    // We're intentionally not using ExternalURNBroker for modifications since
    // calls to the SQL procedures seem to be more efficient.
    conn = DriverManager.getConnection(realUrl);
    conn.setAutoCommit(false);
    nextPRID = conn.prepareStatement("SELECT ims.prid_seq.nextval FROM DUAL");
    newPersonEntry = conn.prepareStatement("INSERT INTO ims.pr_person (prid) values(?)");
    deletePersonEntry = conn.prepareCall("call ims.pr_maintain.deletePerson(?)");
    setValue = conn.prepareCall("call ims.pr_update.setURNValue(?, ?, ?)");
    removeValue = conn.prepareCall("call ims.pr_update.removeURNValue(?, ?, ?)");
    removeAllValues = conn.prepareCall("call ims.pr_update.removeAllURNValues(?, ?)");
    pridFromLDAPKey = conn.prepareStatement("select prid from pr_identifier where source_cd='idms' and type_cd='directory' and lower(identifier)=? and seq='0'");
  }
  
  /**
   * @throws SQLException
   */
  public void shutdown() throws SQLException {
    LOG.info("Shutting down Person Registry Helper connections.");
    if (nextPRID != null) {
      nextPRID.close();
      nextPRID = null;
    }
    
    if (newPersonEntry != null) {
      newPersonEntry.close();
      newPersonEntry = null;
    }
    
    if (deletePersonEntry != null) {
      deletePersonEntry.close();
      deletePersonEntry = null;
    }
    
    if (setValue != null) {
      setValue.close();
      setValue = null;
    }
    
    if (removeValue != null) {
      removeValue.close();
      removeValue = null;
    }
    
    if (removeAllValues != null) {
      removeAllValues.close();
      removeAllValues = null;
    }
    
    if (pridFromLDAPKey != null) {
      pridFromLDAPKey.close();
      pridFromLDAPKey = null;
    }
    
    if (broker != null) {
      PoolingDriver driver = (PoolingDriver)DriverManager.getDriver("jdbc:apache:commons:dbcp:");
      driver.closePool(poolKey);
      broker = null;
      conn = null;
    }
  }
  
  
  /**
   * @param urn
   * @return DBAttribute
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IDMSRegistryException
   */
  @SuppressWarnings("unchecked")
  public DBAttribute getDBAttribute(String urn)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      IDMSRegistryException {
    String attributeNameCorrectCase = URNLookup.getAttrName(urn);
    return ((Class<DBAttribute>) Class.forName("edu.duke.oit.idms.registry.attribute."
        + attributeNameCorrectCase)).newInstance();
  }

  /**
   * @param attrsForPR
   * @throws SQLException 
   */
  public void createUser(Map<String, PersonRegistryAttribute> attrsForPR) throws SQLException {
    try {
      checkIfReconnectNeeded();
      ResultSet result = nextPRID.executeQuery();
      result.next();
      long prid = result.getInt(1);
      result.close();
      
      newPersonEntry.setLong(1, prid);
      newPersonEntry.executeUpdate();
      
      Iterator<String> iter = attrsForPR.keySet().iterator();
      while (iter.hasNext()) {
        String urn = iter.next();
        Iterator<String> values = attrsForPR.get(urn).getValuesToAdd().iterator();
        while (values.hasNext()) {
          long before = System.currentTimeMillis();
          setValue.setString(1, urn);
          setValue.setLong(2, prid);
          setValue.setString(3, values.next());
          setValue.executeUpdate();
          long after = System.currentTimeMillis();
          int diff = (int) (after - before);
          storedProcedureTimes.add(diff);
        }
      }
      
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    }
  }


  /**
   * @param prid 
   * @param attrsForPR
   * @throws SQLException 
   */
  public void updateUser(long prid, Map<String, PersonRegistryAttribute> attrsForPR)
      throws SQLException {
    try {
      checkIfReconnectNeeded();
      Iterator<String> iter = attrsForPR.keySet().iterator();
      while (iter.hasNext()) {
        String urn = iter.next();
        PersonRegistryAttribute prAttr = attrsForPR.get(urn);
        if (prAttr.isRemoveAllValues()) {
          long before = System.currentTimeMillis();
          removeAllValues.setString(1, urn);
          removeAllValues.setLong(2, prid);
          removeAllValues.executeUpdate();
          long after = System.currentTimeMillis();
          int diff = (int) (after - before);
          storedProcedureTimes.add(diff);
        }
        
        Iterator<String> addValues = prAttr.getValuesToAdd().iterator();
        while (addValues.hasNext()) {
          long before = System.currentTimeMillis();
          setValue.setString(1, urn);
          setValue.setLong(2, prid);
          setValue.setString(3, addValues.next());
          setValue.executeUpdate();
          long after = System.currentTimeMillis();
          int diff = (int) (after - before);
          storedProcedureTimes.add(diff);
        }
        
        Iterator<String> removeValues = prAttr.getValuesToRemove().iterator();
        while (removeValues.hasNext()) {
          long before = System.currentTimeMillis();
          removeValue.setString(1, urn);
          removeValue.setLong(2, prid);
          removeValue.setString(3, removeValues.next());
          removeValue.executeUpdate();
          long after = System.currentTimeMillis();
          int diff = (int) (after - before);
          storedProcedureTimes.add(diff);
        }
      }
      
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    }
  }
  
  /**
   * @param ldapKey
   * @throws SQLException
   */
  public void deleteUser(String ldapKey) throws SQLException {
    try {
      long prid = getPRIDFromLDAPKey(ldapKey);
      deletePersonEntry.setLong(1, prid);
      deletePersonEntry.executeUpdate();
      
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    }
  }
  
  /**
   * @param ldapKey
   * @return prid
   * @throws SQLException
   */
  public long getPRIDFromLDAPKey(String ldapKey) throws SQLException {
    pridFromLDAPKey.setString(1, ldapKey.toLowerCase());
    ResultSet result = pridFromLDAPKey.executeQuery();
    
    if (!result.next()) {
      result.close();
      return -1;
    }
    
    long prid = result.getInt(1);
    result.close();

    return prid;
  }

  /**
   * Get all values in the Person Registry for a multi-valued attribute.
   * @param ldapKey
   * @param urn
   * @return list
   * @throws SQLException 
   * @throws IDMSRegistryException 
   */
  @SuppressWarnings("unchecked")
  public List<String> getAttributeValues(String ldapKey, String urn) 
      throws SQLException, IDMSRegistryException {
    
    long prid = getPRIDFromLDAPKey(ldapKey);
    Object valuesList = broker.getURNValue(urn, prid);
    if (valuesList == null) {
      return new ArrayList<String>();
    } else {
      return (List<String>) valuesList;
    }
  }
  
  
  private void checkIfReconnectNeeded() throws SQLException {
    if (storedProcedureTimes.size() < minSQLCalls) {
      return;
    }
    
    while (storedProcedureTimes.size() > minSQLCalls) {
      storedProcedureTimes.removeFirst();
    }
    
    int total = 0;
    Iterator<Integer> iter = storedProcedureTimes.iterator();
    while (iter.hasNext()) {
      total += iter.next();
    }
    
    int average = total / minSQLCalls;
    if (average > minSQLTime) {
      storedProcedureTimes = new LinkedList<Integer>();
      shutdown();
      init();
    }
  }
}
