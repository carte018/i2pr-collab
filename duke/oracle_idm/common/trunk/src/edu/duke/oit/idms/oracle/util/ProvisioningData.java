package edu.duke.oit.idms.oracle.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.thortech.util.logging.Logger;

/**
 * @author shilen
 *
 */
public abstract class ProvisioningData {
  
  private static Logger logger = Logger.getLogger(ProvisioningData.class.getName());

  private Set syncAttributes = new LinkedHashSet();
  private Set logicAttributes = new LinkedHashSet();
  private Map targetMapping = new HashMap();
  private Map allProperties = new HashMap();
  private boolean isConnectorDisabled = false;
  private boolean isConnectorDisabledWithoutErrors = false;
  private final String SYNC_PREFIX = "sync.attribute.";
  private final String LOGIC_PREFIX = "logic.attribute.";
  private final String TARGET_PREFIX = "target.attribute.";
    
  /**
   * Read all the properties.
   * @param connectorName 
   */
  public ProvisioningData(String connectorName) {
    Properties props = new Properties();
    try {
      // we'll default to /opt/idms/oracle_idm/common which is where the app server config files should be...
      props.load(new FileInputStream(System.getProperty("OIM_COMMON_HOME", "/opt/idms/oracle_idm/common")
          + "/conf/provisioning_connectors/" + connectorName + "/properties.conf"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Enumeration en = props.propertyNames();
    while (en.hasMoreElements()) {
      String property = (String)en.nextElement();
      String value = props.getProperty(property);
      
      allProperties.put(property.toLowerCase(), value);

      if (property.startsWith(SYNC_PREFIX)) {
        String attribute = property.substring(SYNC_PREFIX.length());
        if (value.equals("true")) {
          syncAttributes.add(attribute.toLowerCase());
        }
      } else if (property.startsWith(LOGIC_PREFIX)) {
        String attribute = property.substring(LOGIC_PREFIX.length());
        if (value.equals("true")) {
          logicAttributes.add(attribute.toLowerCase());
        }
      } else if (property.startsWith(TARGET_PREFIX)) {
        String attribute = property.substring(TARGET_PREFIX.length());
        targetMapping.put(attribute.toLowerCase(), value);
      } else if (property.equals("connector.disabled")) {
        if (value.equals("true")) {
          isConnectorDisabled = true;
        }
      } else if (property.equals("connector.disabledWithoutErrors")) {
        if (value.equals("true")) {
          isConnectorDisabledWithoutErrors = true;
        }
      }
    }
    
    logger.info(connectorName + ": Finished loading configuration.");
  }
  
  /**
   * Check if the given attribute is a logic attribute
   * @param attribute
   * @return boolean
   */
  public boolean isLogicAttribute(String attribute) {
    return logicAttributes.contains(attribute.toLowerCase());
  }
  
  /**
   * Check if the given attribute is a sync attribute
   * @param attribute
   * @return boolean
   */
  public boolean isSyncAttribute(String attribute) {
    return syncAttributes.contains(attribute.toLowerCase());
  }
  
  /**
   * Get all sync attributes
   * @return set
   */
  public Set getSyncAttributes() {
    return syncAttributes;
  }
  
  /**
   * Get all logic attributes
   * @return set
   */
  public Set getLogicAttributes() {
    return logicAttributes;
  }
  
  /**
   * Get all attributes that this connector is concerned with.
   * @return string array
   */
  public String[] getAllAttributes() {
    Set allAttributes = new LinkedHashSet();
    allAttributes.addAll(syncAttributes);
    allAttributes.addAll(logicAttributes);
    
    return (String[])allAttributes.toArray(new String[allAttributes.size()]);
  }
  
  /**
   * @param attribute
   * @return the name of the attribute in the target system.
   */
  public String getTargetMapping(String attribute) {
    return (String)targetMapping.get(attribute.toLowerCase());
  }
  
  /**
   * Whether the connector should be disabled in the adapter code.
   * @return boolean
   */
  public boolean isConnectorDisabled() {
    return this.isConnectorDisabled;
  }
  
  /**
   * Whether the connector should be disabled without errors in the adapter code.
   * @return boolean
   */
  public boolean isConnectorDisabledWithoutErrors() {
    return this.isConnectorDisabledWithoutErrors;
  }
  
  /**
   * Get property
   * @param property 
   * @return string
   */
  public String getProperty(String property) {
    return (String)allProperties.get(property.toLowerCase());
  }
  
  /**
   * @return all properties
   */
  public Map getAllProperties() {
    return this.allProperties;
  }
}
