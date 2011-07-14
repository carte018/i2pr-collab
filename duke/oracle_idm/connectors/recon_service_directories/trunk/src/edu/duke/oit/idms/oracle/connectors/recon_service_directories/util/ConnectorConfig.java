package edu.duke.oit.idms.oracle.connectors.recon_service_directories.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.Logic;


/**
 * @author shilen
 *
 */
public class ConnectorConfig {

  private static ConnectorConfig cfg = null;
  private Properties props = new Properties();
  private HashMap<String, Boolean> attributesToSyncToOIM = new HashMap<String, Boolean>();
  private HashMap<String, Boolean> attributesToSyncToPR = new HashMap<String, Boolean>();
  private HashMap<String, ArrayList<Class<Logic>>> logicAttributes = new HashMap<String, ArrayList<Class<Logic>>>();
  private final String OIM_ATTRIBUTE_PREFIX = "oim.sync.attribute.";
  private final String PR_ATTRIBUTE_PREFIX = "pr.sync.attribute.";
  private final String LOGIC_ATTRIBUTE_PREFIX = "logic.attribute.";
  
  /**
   * Get instance of class.
   * @return ConnectorConfig
   */
  public static ConnectorConfig getInstance() {
    if (cfg == null) {
      cfg = new ConnectorConfig();
    }
    
    return cfg;
  }
  
  /**
   * Read all the properties.
   */
  @SuppressWarnings("unchecked")
  private ConnectorConfig() {
    try {
      props.load(new FileInputStream(System.getenv("OIM_CONNECTOR_HOME")
          + "/conf/properties.conf"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Enumeration<String> en = (Enumeration<String>)props.propertyNames();
    while (en.hasMoreElements()) {
      String property = en.nextElement();
      String value = props.getProperty(property);

      if (property.startsWith(OIM_ATTRIBUTE_PREFIX)) {
        String attribute = property.substring(OIM_ATTRIBUTE_PREFIX.length());
        if (value.equals("true")) {
          attributesToSyncToOIM.put(attribute.toLowerCase(), true);
        }
      } else if (property.startsWith(PR_ATTRIBUTE_PREFIX)) {
        String attribute = property.substring(PR_ATTRIBUTE_PREFIX.length());
        if (value.equals("true")) {
          attributesToSyncToPR.put(attribute.toLowerCase(), true);
        }
      } else if (property.startsWith(LOGIC_ATTRIBUTE_PREFIX)) {
        String attribute = property.substring(LOGIC_ATTRIBUTE_PREFIX.length());
        try {
          String[] allValues = value.split(",");
          for (int i = 0; i < allValues.length; i++) {
            String currValue = allValues[i];
            if (logicAttributes.get(attribute.toLowerCase()) == null) {
              logicAttributes.put(attribute.toLowerCase(), new ArrayList<Class<Logic>>());
            }
            logicAttributes.get(attribute.toLowerCase()).add((Class<Logic>) Class.forName(currValue));
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
  
  /**
   * Check to see if this attribute is going to be sync'ed to OIM.
   * @param attribute
   * @return boolean
   */
  public boolean isOIMSyncAttribute(String attribute) {
    if (attributesToSyncToOIM.get(attribute.toLowerCase()) == null) {
      return false;
    }    
    
    return true;
  }
  
  /**
   * Check to see if this attribute is going to be sync'ed to the Person Registry.
   * @param attribute
   * @return boolean
   */
  public boolean isPRSyncAttribute(String attribute) {
    if (attributesToSyncToPR.get(attribute.toLowerCase()) == null) {
      return false;
    }    
    
    return true;
  }
  
  /**
   * Check to see if this attribute is used in any logic.
   * @param attribute
   * @return boolean
   */
  public boolean isLogicAttribute(String attribute) {
    if (logicAttributes.get(attribute.toLowerCase()) == null) {
      return false;
    }    
    
    return true;
  }
  
  /**
   * Get logic class for an attribute
   * @param attribute
   * @return Logic
   */
  public ArrayList<Logic> getLogicClass(String attribute) {
    try {
      ArrayList<Logic> allLogic = new ArrayList<Logic>();
      Iterator<Class<Logic>> iter = logicAttributes.get(attribute.toLowerCase()).iterator();
      while (iter.hasNext()) {
        allLogic.add(iter.next().newInstance());
      }
      
      return allLogic;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Get a property.
   * @param key
   * @return String
   */
  public String getProperty(String key) {
    return props.getProperty(key);
  }
  
}