package edu.duke.oit.idms.oracle.connectors.prov_mail_routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;

import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.logic.*;


/**
 * @author shilen
 *
 */
public class MailRoutingProvisioning extends SimpleProvisioning {

  /**
   * connector name
   */
  public final static String connectorName = "MAIL_ROUTING_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  private String queryAttrs[] = { 
      attributeData.getOIMAttributeName("uid"),
      "USR_UDF_IS_STAFF", "USR_UDF_IS_STUDENT", "USR_UDF_IS_EMERITUS", "USR_UDF_IS_FACULTY", "USR_UDF_IS_AFFILIATE",
      attributeData.getOIMAttributeName("duFunctionalGroup"),
      attributeData.getOIMAttributeName("duNetIDStatus"),
      attributeData.getOIMAttributeName("duPSAcadCareerC1"),
      attributeData.getOIMAttributeName("duPSAcadCareerC2"),
      attributeData.getOIMAttributeName("duPSAcadCareerC3"),
      attributeData.getOIMAttributeName("duPSAcadCareerC4"),
      attributeData.getOIMAttributeName("duPSAcadProgC1"),
      attributeData.getOIMAttributeName("duPSAcadProgC2"),
      attributeData.getOIMAttributeName("duPSAcadProgC3"),
      attributeData.getOIMAttributeName("duPSAcadProgC4"),
      attributeData.getOIMAttributeName("duLDAPKey"),
      attributeData.getOIMAttributeName("givenName"),
      attributeData.getOIMAttributeName("cn"),
      attributeData.getOIMAttributeName("sn"),
      attributeData.getOIMAttributeName("duDukeID"),
      "USR_UDF_ENTRYTYPE", "Users.Key" };
  
  public String deprovisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey);
    
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    ldapConnectionWrapper.deleteEntriesByLDAPKey(duLDAPKey);
    
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey + ".  Returning success.");
    
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
        
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
        
    NamingEnumeration<SearchResult> results = ldapConnectionWrapper.findEntriesByLDAPKey(duLDAPKey, "dc=duke,dc=edu");
    if (results.hasMoreElements()) {
      throw new RuntimeException("LDAP entry already exists.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);

    Attributes modAttrs = new BasicAttributes();

    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String)iter.next();
      String value = null;
      for (String attr : attrs.keySet()) {
        if (attribute.equalsIgnoreCase(attr)) {
          value = attrs.get(attr);
        }
      }

      Attribute modAttr = new BasicAttribute(attribute);

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

      if (modAttr.size() > 0) {
        modAttrs.put(modAttr);
      }
    }
    
    // create entry
    ldapConnectionWrapper.createEntry(duLDAPKey, entryType, modAttrs);

    logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String entryType,
      String attribute, String unused2, String newValue) {
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
        
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    if (attribute.equalsIgnoreCase("uid")) {
      if (newValue == null || newValue.isEmpty()) {
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Deleting NetID so deprovisioning.");
        deprovisionUser(dataProvider, duLDAPKey, entryType);
        return "DELETED";
      }
    }
    
    if (provisioningData.isSyncAttribute(attribute)) {
      Attributes modAttrs = new BasicAttributes();
      Attribute modAttr = new BasicAttribute(attribute);
      
      if (newValue != null && !newValue.equals("")) {
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
    } else {
      // may need to change routing based on departmental rules
      Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
      SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey, ldapConnectionWrapper.getBaseDnByEntryType(entryType));
      
      if (result == null) {
        throw new RuntimeException("Missing LDAP entry during update.");
      }
      
      runDepartmentalRules(provisioningData, ldapConnectionWrapper, duLDAPKey, entryType, attrs, result);
    }
        
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   * @return map
   */
  private Map<String, String> getOIMAttributesForUser(tcDataProvider dataProvider, String duLDAPKey) {
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> mhSearchCriteria = new Hashtable<String ,String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, queryAttrs);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
      
      Map<String, String> result = new HashMap<String, String>();
      for (int i = 0; i < queryAttrs.length; i++) {
        String value = moResultSet.getStringValue(queryAttrs[i]);
        if (value == null) {
          value = "";
        }
        
        if (OIMAPIWrapper.isOIMAffiliationField(queryAttrs[i])) {
          result.put(queryAttrs[i], value);
        } else if (queryAttrs[i].equals("USR_UDF_ENTRYTYPE") || queryAttrs[i].equals("Users.Key")) {
          result.put(queryAttrs[i], value);
        } else {
          result.put(attributeData.getLDAPAttributeName(queryAttrs[i]), value);
        }
      }
      
      return result;
      
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
  }
  

  
  /**
   * @param dataProvider
   * @param duLDAPKey
   */
  public void provisionSunMailRouting(tcDataProvider dataProvider, String duLDAPKey) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String entryType = attrs.get("USR_UDF_ENTRYTYPE");
    String uid = attrs.get("uid");
    
    if (uid == null || uid.isEmpty()) {
      throw new RuntimeException("missing netid.");
    }

    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey, ldapConnectionWrapper.getBaseDnByEntryType(entryType));
    if (result == null) {
      throw new RuntimeException("Missing maildir LDAP entry during Sun Mail provisioning.");
    }
    
    Attribute allMailDrops = result.getAttributes().get("mailDrop");
    Attribute allMailAcceptingGeneralIds = result.getAttributes().get("mailAcceptingGeneralId");

    Attributes modAttrs = new BasicAttributes();
    
    if (allMailDrops == null) {
      allMailDrops = new BasicAttribute("mailDrop");
      result.getAttributes().put(allMailDrops);
    }
    
    if (allMailAcceptingGeneralIds == null) {
      allMailAcceptingGeneralIds = new BasicAttribute("mailAcceptingGeneralId");
      result.getAttributes().put(allMailAcceptingGeneralIds);
    }

    if (allMailDrops.size() == 0) {
      allMailDrops.add(uid + "@duke.edu");
      modAttrs.put(allMailDrops);
    }
    
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@acpub.duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@mail.duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    
    modAttrs.put(allMailAcceptingGeneralIds);
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);

    // now run through departmental rules to see if anything needs to change
    runDepartmentalRules(provisioningData, ldapConnectionWrapper, duLDAPKey, entryType, attrs, result);
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   */
  public void provisionExchangeRouting(tcDataProvider dataProvider, String duLDAPKey) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String entryType = attrs.get("USR_UDF_ENTRYTYPE");
    String uid = attrs.get("uid");
    
    if (uid == null || uid.isEmpty()) {
      throw new RuntimeException("missing netid.");
    }

    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey, ldapConnectionWrapper.getBaseDnByEntryType(entryType));
    if (result == null) {
      throw new RuntimeException("Missing maildir LDAP entry during Exchange provisioning.");
    }
    
    Attribute allMailDrops = result.getAttributes().get("mailDrop");
    Attribute allMailAcceptingGeneralIds = result.getAttributes().get("mailAcceptingGeneralId");

    Attributes modAttrs = new BasicAttributes();

    if (allMailDrops == null) {
      allMailDrops = new BasicAttribute("mailDrop");
      result.getAttributes().put(allMailDrops);
    }
    
    if (allMailAcceptingGeneralIds == null) {
      allMailAcceptingGeneralIds = new BasicAttribute("mailAcceptingGeneralId");
      result.getAttributes().put(allMailAcceptingGeneralIds);
    }
    
    if (allMailDrops.size() == 0) {
      allMailDrops.add(uid + "@win.duke.edu");
      modAttrs.put(allMailDrops);
    }
    
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@win.duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@acpub.duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    addMailAcceptingGeneralIdUnlessMailDrop(uid + "@mail.duke.edu", allMailAcceptingGeneralIds, allMailDrops);
    
    modAttrs.put(allMailAcceptingGeneralIds);
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);

    // now run through departmental rules to see if anything needs to change
    runDepartmentalRules(provisioningData, ldapConnectionWrapper, duLDAPKey, entryType, attrs, result);
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   */
  public void deprovisionSunMailRouting(tcDataProvider dataProvider, String duLDAPKey) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String entryType = attrs.get("USR_UDF_ENTRYTYPE");
    String uid = attrs.get("uid");
    
    if (uid == null || uid.isEmpty()) {
      // netid deleted apparently so the maildir entry should be deleted or about to be deleted...
      return;
    }

    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey, ldapConnectionWrapper.getBaseDnByEntryType(entryType));
    if (result == null) {
      return;
    }
    
    String exchangeResourceName = provisioningData.getProperty("oim.resources.exchange");
    boolean hasExchange = getProvisionedResources(dataProvider, Long.parseLong(attrs.get("Users.Key"))).contains(exchangeResourceName);
    
    Attribute allMailDrops = result.getAttributes().get("mailDrop");
    Attribute allMailAcceptingGeneralIds = result.getAttributes().get("mailAcceptingGeneralId");
    Attribute mailDropOverride = new BasicAttribute("mailDropOverride");
    
    Attributes modAttrs = new BasicAttributes();

    if (allMailDrops != null) {
      allMailDrops.remove(uid + "@duke.edu");
    }
    
    if (allMailAcceptingGeneralIds != null) {      
      if (!hasExchange) {
        allMailAcceptingGeneralIds.remove(uid + "@duke.edu");
        allMailAcceptingGeneralIds.remove(uid + "@acpub.duke.edu");
        allMailAcceptingGeneralIds.remove(uid + "@mail.duke.edu");
      } else if (!allMailAcceptingGeneralIds.contains(uid + "@duke.edu")) {
        allMailAcceptingGeneralIds.add(uid + "@duke.edu");
      }
    }
    
    if (allMailDrops == null || allMailDrops.size() == 0) {
      modAttrs.put(mailDropOverride);
      if (hasExchange) {
        if (!allMailDrops.contains(uid + "@win.duke.edu")) {
          allMailDrops.add(uid + "@win.duke.edu");
          allMailAcceptingGeneralIds.remove(uid + "@win.duke.edu");
        }
      } else if (allMailAcceptingGeneralIds != null) {
        allMailAcceptingGeneralIds.clear();
      }
    }
    
    if (allMailAcceptingGeneralIds == null || allMailAcceptingGeneralIds.size() == 0) {
      if (hasExchange) {
        throw new RuntimeException("unexpected");
      }
      
      if (allMailDrops != null && allMailDrops.size() != 0) {
        modAttrs.put(mailDropOverride);
        allMailDrops.clear();
      }
    }
    
    if (allMailDrops != null) {
      modAttrs.put(allMailDrops);
    }
    
    if (allMailAcceptingGeneralIds != null) {
      modAttrs.put(allMailAcceptingGeneralIds);
    }
    
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   */
  public void deprovisionExchangeRouting(tcDataProvider dataProvider, String duLDAPKey) {
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String entryType = attrs.get("USR_UDF_ENTRYTYPE");
    String uid = attrs.get("uid");
    
    if (uid == null || uid.isEmpty()) {
      // netid deleted apparently so the maildir entry should be deleted or about to be deleted...
      return;
    }

    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey, ldapConnectionWrapper.getBaseDnByEntryType(entryType));
    if (result == null) {
      return;
    }
    
    String sunMailResourceName = provisioningData.getProperty("oim.resources.sunmail");
    boolean hasSunMail = getProvisionedResources(dataProvider, Long.parseLong(attrs.get("Users.Key"))).contains(sunMailResourceName);
        
    Attribute allMailDrops = result.getAttributes().get("mailDrop");
    Attribute allMailAcceptingGeneralIds = result.getAttributes().get("mailAcceptingGeneralId");
    Attribute mailDropOverride = new BasicAttribute("mailDropOverride");
    
    Attributes modAttrs = new BasicAttributes();

    if (allMailDrops != null) {
      allMailDrops.remove(uid + "@win.duke.edu");
    }
    
    if (allMailAcceptingGeneralIds != null) {
      allMailAcceptingGeneralIds.remove(uid + "@win.duke.edu");
      
      if (!hasSunMail) {
        allMailAcceptingGeneralIds.remove(uid + "@duke.edu");
        allMailAcceptingGeneralIds.remove(uid + "@acpub.duke.edu");
        allMailAcceptingGeneralIds.remove(uid + "@mail.duke.edu");
      }      
    }
    
    if (allMailDrops == null || allMailDrops.size() == 0) {
      modAttrs.put(mailDropOverride);
      if (hasSunMail) {
        if (!allMailDrops.contains(uid + "@duke.edu")) {
          allMailDrops.add(uid + "@duke.edu");
          allMailAcceptingGeneralIds.remove(uid + "@duke.edu");
        }
      } else if (allMailAcceptingGeneralIds != null) {
        allMailAcceptingGeneralIds.clear();
      }
    }
    
    if (allMailAcceptingGeneralIds == null || allMailAcceptingGeneralIds.size() == 0) {
      if (hasSunMail) {
        throw new RuntimeException("unexpected");
      }
      
      if (allMailDrops != null && allMailDrops.size() != 0) {
        modAttrs.put(mailDropOverride);
        allMailDrops.clear();
      }
    }
    
    if (allMailDrops != null) {
      modAttrs.put(allMailDrops);
    }
    
    if (allMailAcceptingGeneralIds != null) {
      modAttrs.put(allMailAcceptingGeneralIds);
    }
    
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);
  }
  
  private void addMailAcceptingGeneralIdUnlessMailDrop(String address, Attribute mailAcceptingGeneralIds, Attribute mailDrop) {
    if (!mailDrop.contains(address) && !mailAcceptingGeneralIds.contains(address)) {
      mailAcceptingGeneralIds.add(address);
    }
  }
  
  private void runDepartmentalRules(ProvisioningDataImpl provisioningData, LDAPConnectionWrapper ldapConnectionWrapper,
      String duLDAPKey, String entryType, Map<String, String> attrs, SearchResult result) {
    
    ArrayList<Logic> departmentalRules = provisioningData.getLogicClasses();
    for (Logic logic : departmentalRules) {
      boolean changed = logic.updateMailDrop(provisioningData, ldapConnectionWrapper, duLDAPKey, entryType, attrs, result);
      if (changed) {
        break;
      }
    }
  }
  
  private Set<String> getProvisionedResources(tcDataProvider dataProvider, long userKey) {
    Set<String> resources = new HashSet<String>();
   
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      tcResultSet results = moUserUtility.getObjects(userKey);
      for (int i = 0; i < results.getRowCount(); i++) {
        results.goToRow(i);
        String resource = results.getStringValue("Objects.Name");
        String status = results.getStringValue("Objects.Object Status.Status");
        if (status != null && status.equals("Provisioned")) {
          resources.add(resource);
        }
      }
 
      return resources;
    } catch (tcAPIException e) {
      throw new RuntimeException(e);
    } catch (tcColumnNotFoundException e) {
      throw new RuntimeException(e);
    } catch (tcUserNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
