package edu.duke.oit.idms.oracle.connectors.prov_exchange;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.MailRoutingProvisioning;


/**
 * @author shilen
 *
 */
public class ExchangeProvisioning extends SimpleProvisioning {

  /**
   * connector name
   */
  public final static String connectorName = "EXCHANGE_PROVISIONING";

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
      attributeData.getOIMAttributeName("mail"),
      attributeData.getOIMAttributeName("duFuquaLEA"),
      attributeData.getOIMAttributeName("duFuquaLEATarget"),
      attributeData.getOIMAttributeName("duEmailAlias") };
  
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
        
    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey);
    String uid;
    try {
      uid = (String)result.getAttributes().get("sAMAccountName").get();
    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
    
    ExchangeConnectionWrapper.getInstance(dataProvider).deprovision(uid, ldapConnectionWrapper.getActiveDirectoryHostname());

    // update routing
    new MailRoutingProvisioning().deprovisionExchangeRouting(dataProvider, duLDAPKey);
    
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

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
        
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
        
    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey);
    if (result == null) {
      logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Could not find AD object.  Failing.");
      throw new RuntimeException("Could not find AD object for " + duLDAPKey);
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String primarySMTPAddress = getPrimarySMTPAddress(attrs, true);
    String netidStatus = attrs.get("duNetIDStatus");
    
    ExchangeConnectionWrapper.getInstance(dataProvider).provision(attrs.get("uid"), primarySMTPAddress, ldapConnectionWrapper.getActiveDirectoryHostname());
    if (this.isFuqua(attrs)) {
      ExchangeConnectionWrapper.getInstance(dataProvider).addMailboxPermission(attrs.get("uid"), "fuqua.fworld", "FullAccess", ldapConnectionWrapper.getActiveDirectoryHostname());
      logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Added Fuqua permission.");
    }
    
    if (!netidStatus.equals("inactive")) {
      if (this.isFuquaStudent(attrs) || !this.isActive(attrs)) {
        String duFuquaLEATarget = attrs.get("duFuquaLEATarget");
        
        if (duFuquaLEATarget != null && !duFuquaLEATarget.equals("") && !duFuquaLEATarget.endsWith("@win.duke.edu")) {
          ExchangeConnectionWrapper.getInstance(dataProvider).setForwarding(attrs.get("uid"), duFuquaLEATarget, ldapConnectionWrapper.getActiveDirectoryHostname());
          logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Setting mail contact object to: " + duFuquaLEATarget);
        }
      }
    }

    // update routing
    new MailRoutingProvisioning().provisionExchangeRouting(dataProvider, duLDAPKey);

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

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
        
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attrs = getOIMAttributesForUser(dataProvider, duLDAPKey);
    String uid = attrs.get("uid");
    String netidStatus = attrs.get("duNetIDStatus");
    String duFuquaLEATarget = attrs.get("duFuquaLEATarget");
    
    if (attribute.equalsIgnoreCase("uid")) {
      if (newValue == null || newValue.isEmpty()) {
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Deleting NetID so deprovisioning.");
        deprovisionUser(dataProvider, duLDAPKey, null);
        return "DELETED";
      }
    }
    
    if (uid == null || uid.isEmpty()) {
      // this shouldn't happen...
      throw new RuntimeException("User doesn't have a NetID.");
    }

    if (attribute.equalsIgnoreCase("duNetIDStatus")) {
      if (newValue.equals("inactive")) {
        ExchangeConnectionWrapper.getInstance(dataProvider).removeForwarding(attrs.get("uid"), ldapConnectionWrapper.getActiveDirectoryHostname());
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Removed any existing mail contact objects.");
      } else if (!duFuquaLEATarget.equals("") && !duFuquaLEATarget.endsWith("@win.duke.edu")) {
        if (this.isFuquaStudent(attrs) || !this.isActive(attrs)) {
          ExchangeConnectionWrapper.getInstance(dataProvider).setForwarding(attrs.get("uid"), duFuquaLEATarget, ldapConnectionWrapper.getActiveDirectoryHostname());
          logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Setting mail contact object to: " + duFuquaLEATarget);
        }
      }
    } else if (attribute.equals("duFuquaLEATarget")) {
      if (newValue == null || newValue.equals("") || newValue.endsWith("@win.duke.edu") || netidStatus.equals("inactive")) {
        ExchangeConnectionWrapper.getInstance(dataProvider).removeForwarding(attrs.get("uid"), ldapConnectionWrapper.getActiveDirectoryHostname());
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Removed any existing mail contact objects.");
      } else if (this.isFuquaStudent(attrs) || !this.isActive(attrs)) {
        ExchangeConnectionWrapper.getInstance(dataProvider).setForwarding(attrs.get("uid"), duFuquaLEATarget, ldapConnectionWrapper.getActiveDirectoryHostname());
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Setting mail contact object to: " + duFuquaLEATarget);
      }
    } else if (attribute.equalsIgnoreCase("duEmailAlias") || 
        attribute.equalsIgnoreCase("duFuquaLEA") ||
        attribute.equalsIgnoreCase("uid") || 
        attribute.equalsIgnoreCase("mail")) {

      // update primary SMTP address in AD.
      updateProxyAddresses(ldapConnectionWrapper, duLDAPKey, attrs);

    } else {
      // we need to recalculate the primary smtp address and mail contact object
      updateProxyAddresses(ldapConnectionWrapper, duLDAPKey, attrs);
      
      if (!netidStatus.equals("inactive")) {
        if (this.isFuquaStudent(attrs) || !this.isActive(attrs)) {
          if (duFuquaLEATarget != null && !duFuquaLEATarget.equals("") && !duFuquaLEATarget.endsWith("@win.duke.edu")) {
            ExchangeConnectionWrapper.getInstance(dataProvider).setForwarding(attrs.get("uid"), duFuquaLEATarget, ldapConnectionWrapper.getActiveDirectoryHostname());
            logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Setting mail contact object to: " + duFuquaLEATarget);
          }
        } else {
          // commenting out since non-fuqua folks have contact objects as well and this could be a bad idea without appropriate rules          
          // ExchangeConnectionWrapper.getInstance(dataProvider).removeForwarding(attrs.get("uid"), ldapConnectionWrapper.getActiveDirectoryHostname());
          // logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Removed any existing mail contact objects.");
        }
      }
    }
    
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  private boolean isFuqua(Map<String, String> attrs) {    
    
    if (attrs.get("USR_UDF_IS_STAFF").equals("1") || attrs.get("USR_UDF_IS_EMERITUS").equals("1") ||
        attrs.get("USR_UDF_IS_FACULTY").equals("1") || attrs.get("USR_UDF_IS_AFFILIATE").equals("1")) {
      if (attrs.get("duFunctionalGroup").equals("Fuqua")) {
        return true;
      }
    }
    
    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      if (attrs.get("duPSAcadCareerC1").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC2").equalsIgnoreCase("FUQ") ||
          attrs.get("duPSAcadCareerC3").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC4").equalsIgnoreCase("FUQ")) {
        return true;
      }
      
      if (attrs.get("duPSAcadProgC1").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC2").equalsIgnoreCase("G-BUS") ||
          attrs.get("duPSAcadProgC3").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC4").equalsIgnoreCase("G-BUS")) {
        return true;
      }
    }
    
    return false;
  }
  
  private boolean isFuquaStudent(Map<String, String> attrs) {    

    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      if (attrs.get("duPSAcadCareerC1").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC2").equalsIgnoreCase("FUQ") ||
          attrs.get("duPSAcadCareerC3").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC4").equalsIgnoreCase("FUQ")) {
        return true;
      }
      
      if (attrs.get("duPSAcadProgC1").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC2").equalsIgnoreCase("G-BUS") ||
          attrs.get("duPSAcadProgC3").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC4").equalsIgnoreCase("G-BUS")) {
        return true;
      }
    }
    
    return false;
  }
  
  // TODO Not tested
  @SuppressWarnings("unused")
  private boolean isFuquaOnly(Map<String, String> attrs) {
    if (isFuqua(attrs) && !isActiveAndNotFuqua(attrs)) {
      return true;
    }
    
    return false;
  }
  
  // TODO Not tested
  private boolean isActiveAndNotFuqua(Map<String, String> attrs) {    
    
    if (attrs.get("USR_UDF_IS_STAFF").equals("1") || attrs.get("USR_UDF_IS_EMERITUS").equals("1") ||
        attrs.get("USR_UDF_IS_FACULTY").equals("1") || attrs.get("USR_UDF_IS_AFFILIATE").equals("1")) {
      if (!attrs.get("duFunctionalGroup").equals("Fuqua")) {
        return true;
      }
    }
    
    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      for (int i = 1; i <= 4; i++) {
        if (!attrs.get("duPSAcadCareerC" + i).isEmpty()) {
          if (!attrs.get("duPSAcadCareerC" + i).equalsIgnoreCase("FUQ") && !attrs.get("duPSAcadProgC" + i).equalsIgnoreCase("G-BUS")) {
            return true;
          }
        }
      }
    }
    
    return false;
  }
  
  private boolean isActive(Map<String, String> attrs) {
    if (attrs.get("USR_UDF_IS_STAFF").equals("1") || attrs.get("USR_UDF_IS_EMERITUS").equals("1") ||
        attrs.get("USR_UDF_IS_FACULTY").equals("1") || attrs.get("USR_UDF_IS_AFFILIATE").equals("1") ||
        attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      return true;
    }
    
    return false;
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
        } else {
          result.put(attributeData.getLDAPAttributeName(queryAttrs[i]), value);
        }
      }
      
      return result;
      
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
  }
  

  private String getPrimarySMTPAddress(Map<String, String> attrs, boolean isCreate) {
    
    String uid = attrs.get("uid");
    String mail = attrs.get("mail");
    String duFuquaLEA = attrs.get("duFuquaLEA");    
    String duEmailAlias = attrs.get("duEmailAlias"); 
    
    if (duFuquaLEA != null && !duFuquaLEA.isEmpty()) {
      if (this.isFuquaStudent(attrs) || !this.isActive(attrs)) {
        return duFuquaLEA;
      }
    }

    // we have to be careful using the mail attribute since users can populate this with whatever they want...
    if (mail != null && !mail.isEmpty()) {
      if (uid != null && !uid.isEmpty()) {
        if (mail.equalsIgnoreCase(uid + "@duke.edu")) {
          return uid + "@duke.edu";
        }
      }
      
      if (duEmailAlias != null && !duEmailAlias.isEmpty()) {
        if (mail.equalsIgnoreCase(duEmailAlias)) {
          return duEmailAlias;
        }
      }
      
      if (duFuquaLEA != null && !duFuquaLEA.isEmpty()) {
        if (mail.equalsIgnoreCase(duFuquaLEA)) {
          return duFuquaLEA;
        }
      }
      
      // if this is an update and the mail attribute contains something else, just return null so we skip the update
      if (!isCreate) {
        return null;
      }
    }

    if (duEmailAlias != null && !duEmailAlias.isEmpty()) {
      return duEmailAlias;
    }
    
    if (uid != null && !uid.isEmpty()) {
      return uid + "@duke.edu";
    }
    
    if (isCreate) {
      throw new RuntimeException("Could not calculate primary SMTP address.");
    }
    
    return null;
  }
  
  @SuppressWarnings("unchecked")
  private void updateProxyAddresses(LDAPConnectionWrapper ldapConnectionWrapper, String duLDAPKey, Map<String, String> attrs) {
    String primarySMTPAddress = getPrimarySMTPAddress(attrs, false);
    if (primarySMTPAddress == null) {
      return;
    }
    
    SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(duLDAPKey);
    if (result == null) {
      logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Could not find AD object.  Failing.");
      throw new RuntimeException("Could not find AD object for " + duLDAPKey);
    }
    
    Attribute proxyAddresses = result.getAttributes().get("proxyAddresses");
    List<String> newValues = new LinkedList<String>();
    newValues.add("SMTP:" + primarySMTPAddress);
    
    if (proxyAddresses != null) {
      try {
        NamingEnumeration<String> enumeration = (NamingEnumeration<String>) proxyAddresses.getAll();
        while (enumeration.hasMore()) {
          String value = (String) enumeration.next();
          if (!value.startsWith("SMTP:")) {
            if (!value.toLowerCase().equals(newValues.get(0).toLowerCase())) {
              newValues.add(value);
            }
          }
        }
      } catch (NamingException e) {
        logger.info(connectorName + ": Update user: " + duLDAPKey + ".  Received Exception.", e);
        throw new RuntimeException(e);
      }
    }
           
    Attributes modAttrs = new BasicAttributes();
    Attribute modAttr = new BasicAttribute("proxyAddresses");

    Iterator<String> iter = newValues.iterator();
    while (iter.hasNext()) {
      String value = iter.next();
      modAttr.add(value);
    }
    
    modAttrs.put(modAttr);
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, modAttrs);
  }
}
