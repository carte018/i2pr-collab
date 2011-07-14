package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.thortech.util.logging.Logger;
import com.thortech.xl.gc.exception.ProviderException;
import com.thortech.xl.gc.spi.ReconTransportProvider;
import com.thortech.xl.gc.vo.designtime.Provider;
import com.thortech.xl.gc.vo.designtime.TargetSchema;
import com.thortech.xl.gc.vo.runtime.TargetRecord;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.gtc.db.recon.logic.Logic;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * @author shilen
 *
 */
public class ReconTransportFromDB implements ReconTransportProvider {

  private static Logger logger = Logger.getLogger(LoggerModules.GC_PROVIDER_RECONTRANSPORT);
  
  private Provider providerData;
  private String driver;
  private String url;
  private String username;
  private String password;
  private String schema;
  private String connectionProperties;
  
  // display name of connector
  private String connectorName;
  
  // name of parent table
  private String parentTableName;
  
  // name of parent table with schema
  private String parentTableNameWithSchema;
  
  // list of child tables and columns in the format <child table>.<column with value>
  private List childTableNamesAndColumns;
  
  // primary key column on parent table
  private String parentPrimaryKey;
  
  // foreign key column on any child tables
  private String childForeignKey;
  
  // table with timestamp
  private String timestampTable;
  
  // timestamp attribute on parent table used for incremental reconciliations
  private String timestampAttribute;
  
  // table with timestamp with schema
  private String timestampTableWithSchema;
  
  // logic class 
  private Logic logicClass = null;
  
  // connection object
  private Connection conn = null;
  
  // HashMap of keys -> timestamp that are being reconciled.
  private HashMap keysToTimestamp = null;
  
  public void initialize(Provider providerData) throws ProviderException {
    
    this.providerData = providerData;
    Hashtable runTimeParams = providerData.getRuntimeParams();
    Hashtable designParams = providerData.getDesignParams();
    
    this.connectorName = (String)runTimeParams.get("connectorName");
    if (connectorName == null) {
      connectorName = "Unknown connector";
    }
    
    logger.info(connectorName + ": Running initialize()");
    
    this.driver = (String)runTimeParams.get("driver");
    this.url = (String)runTimeParams.get("url");
    this.username = (String)runTimeParams.get("username");
    this.password = (String)runTimeParams.get("password");
    this.connectionProperties = (String)runTimeParams.get("connectionProperties");
    
    this.schema = (String)designParams.get("databaseSchema");
    if (this.schema != null && this.schema.equals("")) {
      this.schema = null;
    }
    this.parentTableName = (String)designParams.get("parentTableName");
    this.parentPrimaryKey = (String)designParams.get("parentPrimaryKey");
    this.childForeignKey = (String)designParams.get("childForeignKey");
    this.timestampAttribute = (String)designParams.get("timestampAttribute");
    this.timestampTable = (String)designParams.get("timestampTable");
    if (designParams.get("childTableNamesAndColumns") != null && !designParams.get("childTableNamesAndColumns").equals("")) {
      this.childTableNamesAndColumns = Arrays.asList(((String)designParams.get("childTableNamesAndColumns")).split(","));
    } else {
      childTableNamesAndColumns = new ArrayList();
    }
    
    if (schema != null) {
      this.parentTableNameWithSchema = schema + "." + parentTableName;
      this.timestampTableWithSchema = schema + "." + timestampTable;
    } else {
      this.parentTableNameWithSchema = parentTableName;
      this.timestampTableWithSchema = timestampTable;
    }
    
    try {
      Class.forName(this.driver);
    } catch (ClassNotFoundException e) {
      throw new ProviderException(connectorName + ": Class not found for " + this.driver + ".", e);
    }
    
    try {
      String theClass = (String)designParams.get("logicClass");
      if (theClass != null && !theClass.equals("")) {
        logicClass = (Logic)Class.forName(theClass).newInstance();
      }
    } catch (Exception e ) {
      throw new ProviderException(connectorName + ": Unable to get instance of Logic class: " + designParams.get("logicClass") + ".", e);
    }
  }
  
  public TargetRecord[] getFirstPage(int pageSize, String startString) throws ProviderException {
    if (pageSize > 0) {
      logger.error(connectorName + ": Batch size is not supported.");
      throw new ProviderException(connectorName + ": Batch size is not supported");
    } 
    
    logger.info(connectorName + ": Starting data retrieval");
    
    try {
      TargetRecord[] targetRecords = getAllData();
      return targetRecords;
    } catch (Exception e) {
      // we're setting this hashmap to null so that if the end() method is called
      // we won't be clearing the timestamps.
      keysToTimestamp = null;
      if (e instanceof ProviderException) {
        throw (ProviderException)e;
      } else {
        throw new ProviderException(connectorName + ": Unexpected exception.", e);
      }
    }
  }
  
  private ArrayList getParentFields(DatabaseMetaData metadata) throws SQLException {
    ArrayList parentFields = new ArrayList();
    ResultSet results = null;
    try {
      results = metadata.getColumns(null, schema, parentTableName, null);

      while (results.next()) {
        String currColumn = results.getString("COLUMN_NAME");

        parentFields.add(currColumn);
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      if (results != null) {
        results.close();
      }
    }
    return parentFields;
  }

  public TargetSchema getMetadata() throws ProviderException {
    TargetIdentityMetadata targetMetadata = new TargetIdentityMetadata();
    ArrayList parentFields = new ArrayList();
    ResultSet results = null;
    
    connect();
        
    try {

      DatabaseMetaData metadata = conn.getMetaData();
      parentFields = getParentFields(metadata);
                
      if (parentFields.size() == 0) {
        throw new ProviderException(connectorName + ": Parent table not found!  Note that schema and table names are case sensitive.");
      }
      
      // child name should be in the format <child table>.<column with value>
      Iterator iter = childTableNamesAndColumns.iterator();
      while (iter.hasNext()) {
        ArrayList childFields = new ArrayList();
        String[] childTableAndColumn = ((String)iter.next()).split("\\.");
        if (childTableAndColumn.length != 2) {
          throw new ProviderException(connectorName + ": Child tables should be in the format <child table>.<column with value>");
        }
        String childTable = childTableAndColumn[0];
        String childColumn = childTableAndColumn[1];
        
        results = metadata.getColumns(null, schema, childTable, childColumn);

        if (!results.next()) {
          throw new ProviderException(connectorName + ": Child table " + childTable
              + " with column " + childColumn + " not found!  Note that schema and table names are case sensitive.");
        }
             
        parentFields.add(childTable + "." + childColumn);
      }

      targetMetadata.setParentFields(parentFields);  
    } catch (SQLException e) {
      throw new ProviderException(connectorName + ": SQLException while getting schema.", e);
    } finally {
      if (results != null) {
        try {
          results.close();
        } catch (SQLException e) {
          // ignore
        }
      }
      disconnect();
    }
    
    return targetMetadata;
  }

  public TargetRecord[] getNextPage(int pageSize) throws ProviderException {
    
    // we're not doing batched updates
    return null;
  }

  public String end() throws ProviderException {
        
    // We're not using the timestamp during reconciliations because it's unreliable.
    // Because databases use transactions and commits can happen long after the timestamp
    // value in the parent table gets updated, if we use this timestamp to figure out
    // which records should be reconciled, it's possible to miss some updates.
    
    if (keysToTimestamp != null) {
      // Instead, here we will just remove the timestamp field if the timestamp has not changed.
      String updateSQL = "update " + timestampTableWithSchema + " set " + timestampAttribute
          + " = null where " + parentPrimaryKey + " = ? and " + timestampAttribute + " = ?";
      PreparedStatement ps = null;
      
      connect();
      
      try {
        ps = conn.prepareStatement(updateSQL);
        Iterator iter = keysToTimestamp.keySet().iterator();
        while (iter.hasNext()) {
          Long key = (Long)iter.next();
          Timestamp timestamp = (Timestamp)keysToTimestamp.get(key);
          
          ps.setLong(1, key.longValue());
          ps.setTimestamp(2, timestamp);
          
          // Auto-commit is set to true otherwise it's possible to get into a deadlock 
          // with the process that adds the timestamps.
          ps.execute();
        }
      } catch (SQLException e) {
        throw new ProviderException (connectorName + ": Failed to remove timestamp in end() method.");
      } finally {
        if (ps != null) {
          try {
            ps.close();
          } catch (SQLException e) {
            // this is okay
          }
        }
        
        disconnect();
      }
    }
    
    long timestamp = System.currentTimeMillis();
    logger.info(connectorName + ": Ending reconciliation");
    return "" + timestamp;
  }
  
  private TargetRecord[] getAllData() throws ProviderException {

    TargetRecord[] targetRecords;
    ArrayList targetRecordsList = new ArrayList();
    connect();
    
    try {
      ArrayList parentFields = getParentFields(conn.getMetaData());
      getKeysForUpdates();
      
      Iterator iter = keysToTimestamp.keySet().iterator();
      
      while (iter.hasNext()) {
        long key = ((Long)iter.next()).longValue();
        TargetRecord curr = getData(key, parentFields);
        if (curr != null) {
          targetRecordsList.add(curr);
        }
      }
      
      targetRecords = new TargetRecord[targetRecordsList.size()];
      for (int i = 0; i < targetRecordsList.size(); i++) {
        TargetRecord curr = (TargetRecord) targetRecordsList.get(i);
        targetRecords[i] = curr;
      }
      
      // if a logic class is defined, use it to perform any further actions
      // unrelated to a specific targetRecord.
      if (logicClass != null) {
        try {
          logicClass.doSomething(conn, providerData);
        } catch (Exception e) {
          // if we get exceptions from this, we'll just log it.
          logger.error("Error received when running connector " + connectorName + " " + 
              "while executing the logic method doSomething(conn, providerData). " + 
              "Though this error may need attention, it will not effect the reconciliation " +
              "process.", e);
        }
      }
      
    } catch (SQLException e) {
      throw new ProviderException(connectorName + 
          ": SQLException while getting data from target.", e);
    } finally {
      disconnect();
    }
    
    return targetRecords;
  }
  
  private TargetRecord getData(long key, ArrayList parentFields) throws SQLException {
    TargetIdentityRecord targetRecord = new TargetIdentityRecord();
    Hashtable fieldsHash = new Hashtable();
    
    PreparedStatement ps = null;
    ResultSet results = null;
    
    try {
      // first get data for parent table
      String selectSQL = "select * from " + parentTableNameWithSchema
          + " where " + parentPrimaryKey + " = ?";
      ps = conn.prepareStatement(selectSQL);
      ps.setLong(1, key);
      results = ps.executeQuery();
      if (results.next()) {
        Iterator iter = parentFields.iterator();
        while (iter.hasNext()) {
          String field = (String)iter.next();
          Object value = results.getObject(field);
          
          if (value != null && !value.equals("") && value instanceof String) {
            fieldsHash.put(field, value);
          }
        }
      } else {
        // odd ... maybe the record was deleted...
        logger.error("Unable to find key " + key + " in " + parentTableNameWithSchema + ".  This record needs to be fixed and reconciled.");
        return null;
      }
      
      results.close();
      results = null;
      
      ps.close();
      ps = null;
      
      // now let's go through the child tables
      Iterator iter = childTableNamesAndColumns.iterator();
      while (iter.hasNext()) {
        List childValues = new ArrayList();
        String[] childTableAndColumn = ((String)iter.next()).split("\\.");

        String childTable = childTableAndColumn[0];
        String childTableWithSchema = childTableAndColumn[0];
        String childColumn = childTableAndColumn[1];
        
        if (this.schema != null) {
          childTableWithSchema = this.schema + "." + childTableWithSchema;
        }
        
        selectSQL = "select " + childColumn + " from " + childTableWithSchema
            + " where " + childForeignKey + " = ? order by " + childColumn;
        ps = conn.prepareStatement(selectSQL);
        ps.setLong(1, key);
        results = ps.executeQuery();
        while (results.next()) {
          childValues.add(results.getObject(1));
        }
        
        String childValuesAsString = OIMAPIWrapper.join(childValues);
        if (!childValuesAsString.equals("")) {
          fieldsHash.put(childTable + "." + childColumn, childValuesAsString);
        }
        
        results.close();
        results = null;
        
        ps.close();
        ps = null;
      }
        
      
    } catch (SQLException e) {
      throw e;
    } finally {
      if (results != null) {
        results.close();
      }
      
      if (ps != null) {
        ps.close();
      }
    }
    
    targetRecord.setParentData(fieldsHash);
   
    // if a logic class is defined, use it to transform the data.
    if (logicClass != null) {
      targetRecord = logicClass.doSomething(targetRecord, conn);
    }
    
    return targetRecord;
  }
  
  private void getKeysForUpdates() throws SQLException {
   
    keysToTimestamp = new HashMap();

    PreparedStatement ps = null;
    ResultSet results = null;
    
    try {
      // get all the keys with updated data.  
      String selectSQL = "select " + parentPrimaryKey + ", " + timestampAttribute
          + " from " + timestampTableWithSchema + " where " + timestampAttribute
          + " is not null";
      ps = conn.prepareStatement(selectSQL);
      results = ps.executeQuery();
      while (results.next()) {
        Long key = new Long(results.getLong(1));
        Timestamp timestamp = results.getTimestamp(2);
        keysToTimestamp.put(key, timestamp);
      }

      logger.info(connectorName + ": Retrieved keys to reconcile: " + keysToTimestamp.keySet().toString());
    } catch (SQLException e) {
      throw e;
    } finally {
      if (results != null) {
        try {
          results.close();
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
    }
  }

  private void connect() throws ProviderException {
    Properties props = new Properties();
    props.setProperty("user", this.username);
    props.setProperty("password", this.password);
    if (connectionProperties != null && !connectionProperties.equals("")) {
      String[] connectionPropertiesArray = connectionProperties.split(",");
      for (int i = 0; i < connectionPropertiesArray.length; i++) {
        String[] keyValue = connectionPropertiesArray[i].split("=");
        props.setProperty(keyValue[0], keyValue[1]);
      }
    }

    try {
      this.conn = DriverManager.getConnection(this.url, props);
    } catch (SQLException e) {
      throw new ProviderException(connectorName + ": Unable to connect to the database.", e);
    }
  }

  private void disconnect() {
    try {
      this.conn.close();
    } catch (SQLException e) {
      // this is okay
    }
  }  
}
