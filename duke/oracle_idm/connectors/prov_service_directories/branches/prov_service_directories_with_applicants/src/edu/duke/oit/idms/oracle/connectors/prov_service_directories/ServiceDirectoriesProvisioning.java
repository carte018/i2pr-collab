package edu.duke.oit.idms.oracle.connectors.prov_service_directories;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

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
public class ServiceDirectoriesProvisioning extends SimpleProvisioning {

  public final static String connectorName = "SVCDIR_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  private String[] mailAlternateAddressAttrs = {"USR_UDF_ACPUBEMAIL", "USR_UDF_EMAILALIAS", 
      "USR_UDF_USEREMAIL", "USR_UDF_DEMPOEMAIL", "USR_UDF_DEPARTMENTALEMAIL"};
  
  public String deprovisionUser(tcDataProvider dataProvider, String duLDAPKey, String entryType) {
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey);
    
    // we don't need to do anything here
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String duLDAPKey, String entryType) {
    logger.info(connectorName + ": Provision user: " + duLDAPKey);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Attributes modAttrs = new BasicAttributes();
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
    
    modAttrs.put(mailAlternateAddressLogic(duLDAPKey, entryType, moResultSet, ldapConnectionWrapper));

    Iterator iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String)iter.next();
      String attributeOIM = attributeData.getOIMAttributeName(attribute);
      String value;
      try {
        value = moResultSet.getStringValue(attributeOIM);
      } catch (Exception e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      }
      
      Attribute modAttr = new BasicAttribute(attribute);
      
      if (value != null && !value.equals("")) {
        if (attributeData.isMultiValued(attribute)) {
          Iterator newValues = OIMAPIWrapper.split(value).iterator();
          while (newValues.hasNext()) {
            modAttr.add((String)newValues.next());
          }
        } else {
          modAttr.add(value);
        }
      }
      
      modAttrs.put(modAttr);
    }
    
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);

    logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String entryType,
      String attribute, String oldValue, String newValue) {
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", oldValue=" + oldValue + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we're just updating one attribute.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Attributes modAttrs = new BasicAttributes();

    if (provisioningData.isSyncAttribute(attribute)) {
      Attribute modAttr = new BasicAttribute(attribute);
      
      if (newValue != null && !newValue.equals("")) {
        if (attributeData.isMultiValued(attribute)) {
          Iterator newValues = OIMAPIWrapper.split(newValue).iterator();
          while (newValues.hasNext()) {
            modAttr.add((String)newValues.next());
          }
        } else {
          modAttr.add(newValue);
        }
      }
      
      modAttrs.put(modAttr);
    }
    
    if (provisioningData.isLogicAttribute(attribute)) {
      if (Arrays.asList(mailAlternateAddressAttrs).contains(attributeData.getOIMAttributeName(attribute))) {
            
        tcResultSet moResultSet = null;
        
        try {
          tcUserOperationsIntf moUserUtility = 
            (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
          
          Hashtable mhSearchCriteria = new Hashtable();
          mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
          moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, mailAlternateAddressAttrs);
          
          if (moResultSet.getRowCount() != 1) {
            throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed while querying OIM to calculate mailAlternateAddress: " + e.getMessage(), e);
        }
        
        modAttrs.put(mailAlternateAddressLogic(duLDAPKey, entryType, moResultSet, ldapConnectionWrapper));

      } else {
        throw new RuntimeException("Attribute " + attribute + " is a logic attribute but there's no logic!");
      }
    }
    
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);
    
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", oldValue=" + oldValue + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }

  private Attribute mailAlternateAddressLogic(String duLDAPKey, String entryType, tcResultSet moResultSet, 
      LDAPConnectionWrapper ldapConnectionWrapper) {
    Attribute modAttr = new BasicAttribute("mailAlternateAddress");

    try {
      for (int i = 0; i < mailAlternateAddressAttrs.length; i++) {
        String attribute = attributeData.getLDAPAttributeName(mailAlternateAddressAttrs[i]);
        String value = moResultSet.getStringValue(mailAlternateAddressAttrs[i]);
        
        if (value != null && !value.equals("")) {
          if (attributeData.isMultiValued(attribute)) {
            Iterator valuesIter = OIMAPIWrapper.split(value).iterator();
            while (valuesIter.hasNext()) {
              modAttr.add(((String)valuesIter.next()).toLowerCase());
            }
          } else {
            modAttr.add(value.toLowerCase());
          }
        }
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed while trying to calculate mailAlternateAddress: " + e.getMessage(), e);
    }
    
    // if there are values, we need to verify the object class is there.
    if (modAttr.size() > 0) {
      ldapConnectionWrapper.checkAndAddObjectClass(duLDAPKey, entryType, "mailRecipient");
    }

    return modAttr;
  }

}
