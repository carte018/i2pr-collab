package edu.duke.oit.idms.oracle.connectors.prov_eprint;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * @author shilen
 *
 */
public class EPrintProvisioning extends SimpleProvisioning {

  public final static String connectorName = "EPRINT_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  public String deprovisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);

    // here we're just deleting the user in the database table if the user exists

    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    databaseWrapper.deleteUser(duLDAPKey);
    
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    logger.info(connectorName + ": Provision user: " + duLDAPKey);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);

    // here we should get all attributes from OIM and sync them with the database.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map attributes = new HashMap();
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
      
      String[] allSyncAttributes = provisioningData.getAllAttributes();
      String[] allSyncAttributesOIMNames = new String[allSyncAttributes.length];
      for (int i = 0; i < allSyncAttributes.length; i++) {
        allSyncAttributesOIMNames[i] = attributeData.getOIMAttributeName(allSyncAttributes[i]);
      }
      
      
      Hashtable mhSearchCriteria = new Hashtable();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, allSyncAttributesOIMNames);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    Iterator iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String)iter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String attributeOIM = attributeData.getOIMAttributeName(attribute);
      String value;
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      try {
        value = moResultSet.getStringValue(attributeOIM);
      } catch (Exception e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      }
      
      // netid cannot be null.  the access policy shouldn't allow it but we're just making sure...
      if (attribute.equals("uid") && (value == null || value.equals(""))) {
        throw new RuntimeException("Unable to provision because there's no NetID.");
      }
            
      if (value != null && !value.equals("")) {
        value = reformatIfMultiValuedAttribute(attribute, value);
        attributes.put(targetAttribute, value);
      } else {
        // this is needed in case we need to update all the fields rather than do an insert.
        attributes.put(targetAttribute, null);
      }
    }

    if (!databaseWrapper.isProvisioned(duLDAPKey)) {
      // if the user isn't in the target system, add the user.
      databaseWrapper.addUser(attributes);
    } else {
      // if the user is in the target system, update the user.
      Iterator iter2 = attributes.keySet().iterator();
      while (iter2.hasNext()) {
        String column = (String)iter2.next();
        String value = (String)attributes.get(column);
        
        databaseWrapper.updateUser(duLDAPKey, column, value);
      }
    }

    logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String unused1,
      String attribute, String unused2, String newValue) {
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);
    
    // here we're just updating one attribute.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    if (newValue != null && newValue.equals("")) {
      newValue = null;
    }
    
    // netid cannot be null.  the access policy shouldn't allow it but we're just making sure...
    if (attribute.equals("uid") && newValue == null) {
      return SUCCESS;
    }
    
    if (provisioningData.isSyncAttribute(attribute)) {
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      newValue = reformatIfMultiValuedAttribute(attribute, newValue);
      
      databaseWrapper.updateUser(duLDAPKey, targetAttribute, newValue);
    } else {
      throw new RuntimeException(attribute + " is not a sync attribute");
    }
    
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }

  /**
   * multi-valued attributes are pipe delimited in the ePrint database.
   * @param attribute
   * @param value
   * @return the new value
   */
  private String reformatIfMultiValuedAttribute(String attribute, String value) {
    if (value != null && attributeData.isMultiValued(attribute)) {
      Iterator values = OIMAPIWrapper.split(value).iterator();
      value = "";
      while (values.hasNext()) {
        value += values.next();
        if (values.hasNext()) {
          value += "|";
        }
      }
    }
    
    return value;
  }
}
