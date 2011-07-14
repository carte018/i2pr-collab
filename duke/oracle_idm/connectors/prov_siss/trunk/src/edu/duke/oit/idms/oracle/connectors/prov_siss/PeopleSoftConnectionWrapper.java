package edu.duke.oit.idms.oracle.connectors.prov_siss;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import psft.pt8.joa.API;
import psft.pt8.joa.IPSMessage;
import psft.pt8.joa.IPSMessageCollection;
import psft.pt8.joa.ISession;
import psft.pt8.joa.JOAException;

import PeopleSoft.Generated.CompIntfc.IDuIdmsStageCi;
import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;

/**
 * @author shilen
 */
public class PeopleSoftConnectionWrapper {

  private static PeopleSoftConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  private ISession session = null;
  
  private String componentInterfaceName = null;
  
  private Class<?> IDuIdmsStageCi = null;
  
  private PeopleSoftConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.session = createConnection();
    
    try {
      IDuIdmsStageCi = Class.forName("PeopleSoft.Generated.CompIntfc.IDuIdmsStageCi");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    
    SimpleProvisioning.logger.info(SISSProvisioning.connectorName + ": Created new instance of PeopleSoftConnectionWrapper.");
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static PeopleSoftConnectionWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new PeopleSoftConnectionWrapper(dataProvider);
    } else {
      instance.dataProvider = dataProvider;
    }

    return instance;
  }

  /**
   * Create a new connection to PeopleSoft based on properties defined in the IT Resource.
   * @return peoplesoft connection
   */
  private ISession createConnection() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
          .getUtility(dataProvider,
              "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map<String, String> parameters = new HashMap<String, String>();
      Map<String, String> resourceMap = new HashMap<String, String>();
      resourceMap.put("IT Resources.Name", "SISS_PROVISIONING");
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
      
      this.componentInterfaceName = parameters.get("componentInterfaceName");
      
      String serverName = parameters.get("serverName");
      String serverPort = parameters.get("serverPort");
      String username = parameters.get("username");
      String password = parameters.get("password");
      
      session = API.createSession();
      session.connect(1, serverName + ":" + serverPort, username, password, null);
      errorCheck();

      return session;
    } catch (Exception e) {
      throw new RuntimeException("Failed while creating connection to PeopleSoft: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }
  }
  
  /**
   * find a record from peoplesoft
   * @return IDuIdmsStageCi
   */
  public IDuIdmsStageCi getNewRecordObject() {
    IDuIdmsStageCi record;
    try {
      record = (IDuIdmsStageCi)session.getCompIntfc(this.componentInterfaceName);
      errorCheck();
      if (record == null) {
        throw new RuntimeException("Failed to get component interface from PeopleSoft");
      }
    } catch (Exception e) {
      // if the query fails, we'll retry only once after reconnecting...
      reconnect();
      
      try {
        record = (IDuIdmsStageCi)session.getCompIntfc(this.componentInterfaceName);
      } catch (JOAException e1) {
        throw new RuntimeException(e1);
      }
      
      errorCheck();
      if (record == null) {
        throw new RuntimeException("Failed to get component interface from PeopleSoft");
      }
    }
    
    return record;
  }
  
  /**
   * Calls get() on the record with the given dukeid.
   * @param record
   * @param dukeid
   * @param dukeidTargetMapping 
   * @param createIfNotFound 
   * @return true if record was found in PeopleSoft
   */
  public boolean getRecordData(IDuIdmsStageCi record, String dukeid, String dukeidTargetMapping, boolean createIfNotFound) {
    try {
      record.setInteractiveMode(false);
      
      String methodName = "set" + dukeidTargetMapping;
      
      Class<?> parameterTypes[] = {String.class};
      Object parameters[] = {dukeid};
      
      Method method = IDuIdmsStageCi.getMethod(methodName, parameterTypes);
      method.invoke(record, parameters);
      
      boolean found = record.get();
      
      // the get() may or may not leave an error in the message queue if the record is not found...
      // we'll delete all errors...
      session.getPSMessages().deleteAll();
      
      if (!found && createIfNotFound) {
        SimpleProvisioning.logger.info(SISSProvisioning.connectorName + ": Record not found for " + dukeid + ".  Running create().");
        boolean result = record.create();
        errorCheck();
        
        if (!result) {
          // i'm not sure if the save would ever return false without errors....
          throw new RuntimeException("record.create() returned false.");
        }      
      }
      
      return found;
      
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * delete a record
   * @param record
   */
  public void deleteRecord(IDuIdmsStageCi record) {
    try {
      boolean result = record.delete();
      errorCheck();
      
      if (!result) {
        // i'm not sure if the delete would ever return false without errors....
        throw new RuntimeException("record.delete() returned false.");
      }
      
      result = record.save();
      errorCheck();
      
      if (!result) {
        // i'm not sure if the save would ever return false without errors....
        throw new RuntimeException("record.save() returned false when trying to delete record.");
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (JOAException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * save a record to peoplesoft
   * @param record
   * @param dukeid 
   * @param attributes
   */
  public void saveRecord(IDuIdmsStageCi record, String dukeid, Map<String, String> attributes) {
    if (attributes.size() == 0) {
      return;
    }
    
    try {
      Iterator<String> iter = attributes.keySet().iterator();
      while (iter.hasNext()) {
        String attribute = iter.next();
        String value = attributes.get(attribute);
        String methodName = "set" + attribute;
        
        if (value == null) {
          value = "";
        }
        
        Class<?> parameterTypes[] = {String.class};
        Object parameters[] = {value};
        
        Method method = IDuIdmsStageCi.getMethod(methodName, parameterTypes);
        
        SimpleProvisioning.logger.info(SISSProvisioning.connectorName + ": Invoking " + methodName + "(\"" + value + "\") for " + dukeid);
        method.invoke(record, parameters);
      }
      
      // make sure errors haven't queued up before saving...
      errorCheck();
      
      boolean result = record.save();
      errorCheck();
      
      if (!result) {
        // i'm not sure if the save would ever return false without errors....
        throw new RuntimeException("record.save() returned false.");
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reconnect to PeopleSoft
   */
  private void reconnect() {
    try {
      session.disconnect();
    } catch (Exception e) {
      // this is okay
    }

    this.session = createConnection();
    SimpleProvisioning.logger.info(SISSProvisioning.connectorName + ": Reconnected to PeopleSoft.");
  }

  /**
   * Close PeopleSoft connection when this class is about to be garbage collected.
   */
  protected void finalize() throws Throwable {
    if (session != null) {
      try {
        session.disconnect();
      } catch (Exception e) {
        // this is okay
      }
    }
    super.finalize();
  }
  
  /**
   * throw exception if there's an error.
   */
  private void errorCheck() {
    if (session.getErrorPending()) {
      String errors = "";
      
      IPSMessageCollection collection = session.getPSMessages();
      for (int i = 0; i < collection.getCount(); i++) {
        IPSMessage message = collection.item(i);
        if (message != null) {
          errors += message.getMessageSetNumber() + " - " + message.getText() + "|";
        }
      }

      collection.deleteAll();
      SimpleProvisioning.logger.warn(SISSProvisioning.connectorName + ": PeopleSoft errors: " + errors);
      throw new RuntimeException("PeopleSoft errors: " + errors);
    }
    
    // in case we didn't have any errors but we have warnings, let's log those.
    if (session.getWarningPending()) {
      String warnings = "";
      
      IPSMessageCollection collection = session.getPSMessages();
      for (int i = 0; i < collection.getCount(); i++) {
        IPSMessage message = collection.item(i);
        if (message != null) {
          warnings += message.getMessageSetNumber() + " - " + message.getText() + "|";
        }
      }

      collection.deleteAll();
      SimpleProvisioning.logger.warn(SISSProvisioning.connectorName + ": PeopleSoft warnings: " + warnings);
    }
  }
}
