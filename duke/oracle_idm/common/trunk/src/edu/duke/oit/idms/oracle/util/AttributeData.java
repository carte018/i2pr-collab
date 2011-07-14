package edu.duke.oit.idms.oracle.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author shilen
 *
 */
public class AttributeData {
  
  private static AttributeData cfg = null;
  private HashMap multiValuedAttributes = new HashMap();
  private HashMap oimAttributes = new HashMap();
  private HashMap ldapAttributes = new HashMap();
  private final String ATTRIBUTE_PREFIX = "oim.attribute.";
  private final String MULTIVALUED_PREFIX = "oim.multivalued.";
  
  /**
   * Get instance of class.
   * @return Config
   */
  public static AttributeData getInstance() {
    if (cfg == null) {
      cfg = new AttributeData();
    }
    
    return cfg;
  }
  
  /**
   * Read all the properties.
   */
  private AttributeData() {
    Properties props = new Properties();
    try {
      // we'll default to /opt/idms/oracle_idm/common which is where the app server config files should be...
      props.load(new FileInputStream(System.getProperty("OIM_COMMON_HOME", "/opt/idms/oracle_idm/common")
          + "/conf/attributes.conf"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Enumeration en = props.propertyNames();
    while (en.hasMoreElements()) {
      String property = (String)en.nextElement();
      String value = props.getProperty(property);

      if (property.startsWith(ATTRIBUTE_PREFIX)) {
        String attribute = property.substring(ATTRIBUTE_PREFIX.length());
        oimAttributes.put(attribute.toLowerCase(), value);
        ldapAttributes.put(value.toLowerCase(), attribute);
      } else if (property.startsWith(MULTIVALUED_PREFIX)) {
        String attribute = property.substring(MULTIVALUED_PREFIX.length());
        if (value.equals("true")) {
          multiValuedAttributes.put(attribute.toLowerCase(), new Boolean(true));
        }
      }
    }
  }
  
  /**
   * Check if the given attribute is multi-valued or not.
   * @param attribute
   * @return boolean
   */
  public boolean isMultiValued(String attribute) {
    if (multiValuedAttributes.get(attribute.toLowerCase()) == null) {
      return false;
    }    
    
    return true;
  }
  
  
  /**
   * Get OIM attribute name from LDAP attribute name.
   * @param attribute
   * @return String
   */
  public String getOIMAttributeName(String attribute) {
    String oimAttr = (String)oimAttributes.get(attribute.toLowerCase());
    
    if (oimAttr == null) {
      throw new RuntimeException(attribute + " is not a valid attribute.");
    }
    
    return oimAttr;
  }
  
  /**
   * Get LDAP attribute name from OIM attribute name.
   * @param attribute
   * @return String
   */
  public String getLDAPAttributeName(String attribute) {
    String ldapAttr = (String)ldapAttributes.get(attribute.toLowerCase());
    
    if (ldapAttr == null) {
      throw new RuntimeException(attribute + " is not a valid attribute.");
    }
    
    return ldapAttr;
  }
}
