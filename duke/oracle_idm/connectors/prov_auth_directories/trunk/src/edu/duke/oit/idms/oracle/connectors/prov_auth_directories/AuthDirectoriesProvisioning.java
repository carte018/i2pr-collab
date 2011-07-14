package edu.duke.oit.idms.oracle.connectors.prov_auth_directories;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

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
public class AuthDirectoriesProvisioning extends SimpleProvisioning {

  /**
   * connector name
   */
  public final static String connectorName = "AUTHDIR_PROVISIONING";

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
    
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    tcUserOperationsIntf moUserUtility = null;
    long userKey;
    
    try {
      moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> mhSearchCriteria = new Hashtable<String, String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, new String[0]);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
      
      userKey = moResultSet.getLongValue("Users.Key");
      
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    // get user's groups
    Set<String> groups = this.getGroupsForUser(moUserUtility, userKey);
    
    // delete based on ldap key rather than netid to make sure consolidations work.
    ldapConnectionWrapper.deleteEntryByLDAPKey(duLDAPKey, groups);
    
    // check if the user's groups have changed... if so, due to a possible timing issue, we'll fail the request.
    Set<String> groups2 = this.getGroupsForUser(moUserUtility, userKey);
    Set<String> groups3 = new HashSet<String>(groups);
    groups3.removeAll(groups2);
    if (groups.size() != groups2.size() || groups3.size() > 0) {
      throw new RuntimeException("Group memberships changing while deprovisioning.  " +
      		"Make sure the user does not have memberships in the LDAP and then force revoke the resource.");
    }
    
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey + ".  Returning success.");
    
    return SUCCESS;
  }
  
  
  /**
   * This basically allows one to manually revoke the resource, which will delete the
   * entry from LDAP along with all group memberships for the user.  Unlike the 
   * deprovision method, it does not consider multiple processes updating LDAP
   * at the same time since this is done manually.
   * @param dataProvider
   * @param duLDAPKey
   * @return status code
   */
  public String forceRevoke(tcDataProvider dataProvider, String duLDAPKey) {
    logger.info(connectorName + ": Force revoke user: " + duLDAPKey);
    
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.deleteEntryByLDAPKey(duLDAPKey, null);
    
    logger.info(connectorName + ": Force revoke user: " + duLDAPKey + ".  Returning success.");

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
    tcUserOperationsIntf moUserUtility = null;
    long userKey;
    
    try {
      moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
      
      Iterator<?> allSyncAttributesIter = provisioningData.getSyncAttributes().iterator();
      Set<String> allSyncAttributesOIMNames = new HashSet<String>();
      while (allSyncAttributesIter.hasNext()) {
        String attribute = (String) allSyncAttributesIter.next();
        if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
          allSyncAttributesOIMNames.addAll(OIMAPIWrapper.getOIMAffiliationFieldNames());
        } else {
          allSyncAttributesOIMNames.add(attributeData.getOIMAttributeName(attribute));
        }
      }
      
      Hashtable<String, String> mhSearchCriteria = new Hashtable<String, String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, allSyncAttributesOIMNames.toArray(new String[0]));
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
      
      userKey = moResultSet.getLongValue("Users.Key");
      
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    String uid = null;
    
    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String)iter.next();
      String attributeOIM = null;
      String value;
      
      Attribute modAttr = new BasicAttribute(attribute);

      try {
        if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
          Iterator<String> affiliationFieldsIter = OIMAPIWrapper.getOIMAffiliationFieldNames().iterator();
          while (affiliationFieldsIter.hasNext()) {
            attributeOIM = affiliationFieldsIter.next();
            String affiliationFieldValue = moResultSet.getStringValue(attributeOIM);
            if (affiliationFieldValue != null && affiliationFieldValue.equals("1")) {
              modAttr.add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(attributeOIM));
            }
          }
        } else {
          attributeOIM = attributeData.getOIMAttributeName(attribute);
          value = moResultSet.getStringValue(attributeOIM);
          
          if (attribute.equalsIgnoreCase("uid") && value != null && !value.equals("")) {
            uid = new String(value);
          }
          
          if (value != null && !value.equals("")) {
            if (attributeData.isMultiValued(attribute)) {
              Iterator<?> newValues = OIMAPIWrapper.split(value).iterator();
              while (newValues.hasNext()) {
                modAttr.add((String)newValues.next());
              }
            } else {
              modAttr.add(value);
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      }
      
      if (modAttr.size() > 0) {
        modAttrs.put(modAttr);
      }
    }
    
    if (uid == null) {
      throw new RuntimeException(connectorName + ": OIM user does not have a NetID");
    }
    
    // get user's groups
    Set<String> groups = this.getGroupsForUser(moUserUtility, userKey);
    
    // delete this uid from the directory if it exists.  it might exist during consolidations...
    ldapConnectionWrapper.deleteEntryByNetID(uid, null);
    
    // create entry and groups
    ldapConnectionWrapper.createEntry(uid, entryType, modAttrs, groups);
    
    // check if the user's groups have changed... if so, due to a possible timing issue, we'll fail the request.
    Set<String> groups2 = this.getGroupsForUser(moUserUtility, userKey);
    Set<String> groups3 = new HashSet<String>(groups);
    groups3.removeAll(groups2);
    if (groups.size() != groups2.size() || groups3.size() > 0) {
      throw new RuntimeException("Group memberships changing while provisioning.");
    }

    logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String entryType,
      String attribute, String unused1, String newValue) {
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue);

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
    
    // if netid is changing, deprovision and reprovisiong if needed.
    if (attribute.equalsIgnoreCase("uid")) {
      logger.info(connectorName + ": NetID is changing for " + duLDAPKey + ".  Running deprovision.");
      this.deprovisionUser(dataProvider, duLDAPKey, entryType);
      
      if (newValue != null && !newValue.equals("")) {
        this.provisionUser(dataProvider, duLDAPKey, entryType);
      }
      
      return SUCCESS;
    }
    
    Attributes modAttrs = new BasicAttributes();
    Attribute modAttr = new BasicAttribute(attribute);

    if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
      // the attribute is eduPersonAffiliation so we're going to query OIM for all affiliation values
      tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, OIMAPIWrapper.getOIMAffiliationFieldNames().toArray(new String[0]));
      Iterator<String> affiliationFieldsIter = OIMAPIWrapper.getOIMAffiliationFieldNames().iterator();
      while (affiliationFieldsIter.hasNext()) {
        String attributeOIM = affiliationFieldsIter.next();
        String affiliationFieldValue;
        try {
          affiliationFieldValue = moResultSet.getStringValue(attributeOIM);
        } catch (Exception e) {
          throw new RuntimeException("Failed to get affiliation values from OIM." , e);
        }
        if (affiliationFieldValue != null && affiliationFieldValue.equals("1")) {
          modAttr.add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(attributeOIM));
        }
      }
    } else if (newValue != null && !newValue.equals("")) {
      if (attributeData.isMultiValued(attribute)) {
        Iterator<?> values = OIMAPIWrapper.split(newValue).iterator();
        while (values.hasNext()) {
          modAttr.add(values.next());
        }
      } else {
        modAttr.add(newValue);
      }
    }
    
    modAttrs.put(modAttr);
    
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);
    
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   * @param attrs
   * @return tcResultSet
   */
  private tcResultSet getOIMAttributesForUser(tcDataProvider dataProvider, String duLDAPKey, String[] attrs) {
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> mhSearchCriteria = new Hashtable<String ,String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return moResultSet;
  }
  
  /**
   * @param moUserUtility
   * @param userKey
   * @return set
   */
  private Set<String> getGroupsForUser(tcUserOperationsIntf moUserUtility, Long userKey) {
    Set<String> groups = new HashSet<String>();
    
    try {
      tcResultSet results = moUserUtility.getGroups(userKey);
      for (int i = 0; i < results.getRowCount(); i++) {
        results.goToRow(i);
        String groupName = results.getStringValue("Groups.Group Name");
        if (groupName.startsWith("duke:")) {
          groups.add(groupName);
        }
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM for group data: " + e.getMessage(), e);
    }
    
    return groups;
  }
  
  /**
   * @param dataProvider 
   * @param groupName
   */
  public void createGroup(tcDataProvider dataProvider, String groupName) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.createGroup(groupName);
  }
  
  /**
   * @param dataProvider 
   * @param groupName
   */
  public void deleteGroup(tcDataProvider dataProvider, String groupName) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.deleteGroup(groupName);
  }
  
  /**
   * @param dataProvider 
   * @param oldGroupName
   * @param newGroupName
   */
  public void renameGroup(tcDataProvider dataProvider, String oldGroupName, String newGroupName) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.renameGroup(oldGroupName, newGroupName);
  }
  
  /**
   * @param dataProvider 
   * @param groupName
   * @param dukeid
   */
  public void addMember(tcDataProvider dataProvider, String groupName, String dukeid) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.addMember(groupName, dukeid);
  }
  
  /**
   * @param dataProvider 
   * @param groupName
   * @param dukeid
   */
  public void deleteMember(tcDataProvider dataProvider, String groupName, String dukeid) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.deleteMember(groupName, dukeid);
  }
}
