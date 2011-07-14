package edu.duke.oit.idms.oracle.connectors.prov_voice_ad;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import org.apache.commons.lang.RandomStringUtils;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * @author shilen
 *
 */
public class VoiceADProvisioning extends SimpleProvisioning {

  /** name of connector for logging purposes */
  public final static String connectorName = "VOICEAD_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  @SuppressWarnings("unchecked")
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

    // here we should delete a bunch of attributes from the target
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);
    
    if (result != null) {
      Attributes modAttrs = new BasicAttributes();
      
      Set<String> attrsToDelete = new HashSet<String>();
      attrsToDelete.addAll(provisioningData.getSyncAttributes());
      attrsToDelete.removeAll(getSyncAttrsNotDeletedDuringDeprovisioning());
      Iterator<String> attrsToDeleteIter = attrsToDelete.iterator();
      while (attrsToDeleteIter.hasNext()) {
        String attrToDelete = attrsToDeleteIter.next();
        Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attrToDelete));
        modAttrs.put(modAttr);
      }
      
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);      
    } else {
      logger.info(connectorName + ": Deprovision user: " + duLDAPKey + ".  Target entry does not exist.");
    }
    
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
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    // netid of the user
    String netid = null;
    
    // first name of the user
    String givenName = "";
    
    // last name of the user
    String sn = "";
    
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);

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
    
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, allSyncAttributesOIMNames.toArray(new String[0]));
  
    // Collect attributes for the user that are being sync'ed to the target
    Attributes addAttrs = new BasicAttributes();

    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String) iter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String value;
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      Attribute addAttr = new BasicAttribute(targetAttribute);
      
      String attributeOIM = null;
      try {
        if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
          Iterator<String> affiliationFieldsIter = OIMAPIWrapper.getOIMAffiliationFieldNames().iterator();
          while (affiliationFieldsIter.hasNext()) {
            attributeOIM = affiliationFieldsIter.next();
            String affiliationFieldValue = moResultSet.getStringValue(attributeOIM);
            if (affiliationFieldValue != null && affiliationFieldValue.equals("1")) {
              addAttr.add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(attributeOIM));
            }
          }
        } else {
          attributeOIM = attributeData.getOIMAttributeName(attribute);
          value = moResultSet.getStringValue(attributeOIM);

          // saving a few variables because they are needed later.
          if (attribute.equalsIgnoreCase("uid")) {
            netid = new String(value);
            logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  User has NetID: " + netid);
          } else if (attribute.equalsIgnoreCase("givenName") && value != null) {
            givenName = new String(value);
          } else if (attribute.equalsIgnoreCase("sn") && value != null) {
            sn = new String(value);
          }
                
          if (value != null && !value.equals("")) {
            if (attributeData.isMultiValued(attribute)) {
              Iterator<?> values = OIMAPIWrapper.split(value).iterator();
              while (values.hasNext()) {
                addAttr.add(values.next());
              }
            } else {
              addAttr.add(value);
            }
          }
        }
        
        if (result == null) {
          // if the entry does not exist, only add attribute if it has values
          if (addAttr.size() > 0) {
            addAttrs.put(addAttr);
          }
        } else {
          addAttrs.put(addAttr);
        }
        
      } catch (tcColumnNotFoundException e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      } catch (tcAPIException e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      }
    }
    
    // netid cannot be null.  the access policy shouldn't allow it but we're just making sure...
    if (netid == null || netid.equals("")) {
      throw new RuntimeException("Unable to provision because there's no NetID.");
    }
    
    if (result == null) {
      // add attributes needed for new entry
      addAttrs = addAttributesForNewEntry(duLDAPKey, netid, addAttrs);
      ldapConnectionWrapper.createEntry(duLDAPKey, getCN(givenName, sn, netid), addAttrs);
    } else {
      addAttrs = addAttributesForNewNetID(duLDAPKey, netid, addAttrs);
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), addAttrs);
      
      // we may have to update the RDN
      String cnInAD = result.getName();
      cnInAD = cnInAD.replaceFirst("^\"", "").replaceFirst("\"$", "");
      cnInAD = Pattern.compile("^cn=", Pattern.CASE_INSENSITIVE).matcher(cnInAD).replaceFirst("");
      ldapConnectionWrapper.renameEntry(duLDAPKey, cnInAD, getCN(givenName, sn, netid));
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

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here one attribute in OIM is being updated.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);
    
    if (result == null) {
      // throw an exception if this occurs
      throw new RuntimeException("Entry with duLDAPKey=" + duLDAPKey + " not found in target.");
    }
    
    Attributes modAttrs = new BasicAttributes();
    
    Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attribute));
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
    
    // if attribute is uid and a new value is available, set uid specific attributes
    if (attribute.equalsIgnoreCase("uid") && newValue != null && !newValue.equals("")) {
      modAttrs = addAttributesForNewNetID(duLDAPKey, newValue, modAttrs);
    }
    
    ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);
    
    // if the attribute being updated is givenName, sn, or uid, we need to recalculate CN in the target
    if (attribute.equalsIgnoreCase("uid") || attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn")) {

      String[] attrsToReturn = { attributeData.getOIMAttributeName("uid"), 
          attributeData.getOIMAttributeName("givenName"), 
          attributeData.getOIMAttributeName("sn") };
      
      tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, attrsToReturn);
      
      String cn = null;
      try {
        String uid = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
        String givenName = moResultSet.getStringValue(attributeData.getOIMAttributeName("givenName"));
        String sn = moResultSet.getStringValue(attributeData.getOIMAttributeName("sn"));
        cn = getCN(givenName, sn, uid);
      } catch (Exception e) {
        throw new RuntimeException("Failed to calculate CN for duLDAPKey=" + duLDAPKey, e);
      }

      
      // Strip off leading and trailing double quotes if they exist.  And strip off leading cn=
      // Note that this is escaped...
      String cnInAD = result.getName();
      cnInAD = cnInAD.replaceFirst("^\"", "").replaceFirst("\"$", "");
      cnInAD = Pattern.compile("^cn=", Pattern.CASE_INSENSITIVE).matcher(cnInAD).replaceFirst("");
      ldapConnectionWrapper.renameEntry(duLDAPKey, cnInAD, cn);
    }

    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  
  /**
   * @return a random password
   * @throws NoSuchAlgorithmException 
   */
  private String generatePassword() throws NoSuchAlgorithmException {
    return RandomStringUtils.random(45,33,127,false,false,null,SecureRandom.getInstance("SHA1PRNG")) + 
        RandomStringUtils.random(5,97,123,false,false,null,SecureRandom.getInstance("SHA1PRNG")) +
        RandomStringUtils.random(5,65,91,false,false,null,SecureRandom.getInstance("SHA1PRNG")) +
        RandomStringUtils.random(5,48,58,false,false,null,SecureRandom.getInstance("SHA1PRNG"));
  }

  
  /**
   * These attributes are added when creating a new entry
   * @param duLDAPKey
   * @param addAttrs
   * @return Attributes
   */
  private Attributes addAttributesForNewEntry(String duLDAPKey, String netid, Attributes addAttrs) {

    // add object class
    Attribute addAttrOC = new BasicAttribute("objectClass");
    addAttrOC.add("top");
    addAttrOC.add("person");
    addAttrOC.add("user");
    addAttrOC.add("inetOrgPerson");
    addAttrOC.add("organizationalPerson");
    addAttrs.put(addAttrOC);
    
    // userAccountControl
    Attribute addAttrUAC = new BasicAttribute("userAccountControl");
    addAttrUAC.add("66048");
    addAttrs.put(addAttrUAC);
    
    // unicodePwd
    Attribute addAttrPASS = new BasicAttribute("unicodePwd");
    try {
      String password = generatePassword();
      addAttrPASS.add(new String("\"" + password + "\"").getBytes("UnicodeLittleUnmarked"));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to generate password while creating new entry with duLDAPKey=" + duLDAPKey + ": " + e.getMessage(), e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Failed to encode password while creating new entry with duLDAPKey=" + duLDAPKey + ": " + e.getMessage(), e);
    }
    addAttrs.put(addAttrPASS);
    
    return addAttributesForNewNetID(duLDAPKey, netid, addAttrs);
  }
  
  /**
   * These attributes are added when there's a new NetID
   * @param duLDAPKey
   * @param addAttrs
   * @return Attributes
   */
  private Attributes addAttributesForNewNetID(String duLDAPKey, String netid, Attributes addAttrs) {
    
    // sAMAccountName
    Attribute addAttrSAN = new BasicAttribute("sAMAccountName");
    addAttrSAN.add("duke-" + System.currentTimeMillis());
    addAttrs.put(addAttrSAN);
    
    // userPrincipalName
    Attribute addAttrUPN = new BasicAttribute("userPrincipalName");
    addAttrUPN.add(netid + "@voice.oit.duke.edu");
    addAttrs.put(addAttrUPN);

    // altSecurityIdentities
    Attribute addAttrASI = new BasicAttribute("altSecurityIdentities");
    addAttrASI.add("Kerberos:" + netid + "@ACPUB.DUKE.EDU");
    addAttrs.put(addAttrASI);
    
    return addAttrs;
  }
  
  /**
   * @return attributes that we're sync'ing that are not deleted during deprovisioning events.
   */
  private Set<String> getSyncAttrsNotDeletedDuringDeprovisioning() {
    Set<String> attrs = new HashSet<String>();
    
    // these should all be lower case
    attrs.add("dudukeid");
    attrs.add("dudukeidhistory");
    attrs.add("uid");
    
    return attrs;
  }
  
  /**
   * @param givenName
   * @param sn
   * @param uid
   * @return cn
   */
  private String getCN(String givenName, String sn, String uid) {
    if (givenName == null) {
      givenName = "";
    }
    
    if (sn == null) {
      sn = "";
    }
    
    if (uid == null) { 
      uid = "";
    }
    
    String cn = givenName + " " + sn + " (" + uid + ")";
    return cn.trim();
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
}
