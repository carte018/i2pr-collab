package edu.duke.oit.idms.oracle.connectors.prov_edir;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * @author liz, with mucho code stolen from shilen's VOICE_AD and ServiceDir providers
 *
 */
public class EDirectoryProvisioning extends SimpleProvisioning {

  private static final String GROUP_NAME_EDIR = "cn=itim,ou=services,ou=is,ou=csi,ou=oit,ou=duke,ou=groups,dc=duke,dc=edu";
  /** name of connector for logging purposes */
  public final static String connectorName = "EDIR_PROVISIONING";
  public final static String GROUPNAME = "duke:oit:csi:is:services:itim";
  private final static String PEOPLE = "People";
  private final static String ENTRYTYPE = "USR_UDF_ENTRYTYPE";  

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();

  public void addItimGroup(tcDataProvider dataProvider, long plUserKey, long plGroupKey) {
    toggleItimGroup(dataProvider, plUserKey, plGroupKey, true);
  }
  
  public void removeFromItimGroup(tcDataProvider dataProvider, long plUserKey, long plGroupKey) {
    toggleItimGroup(dataProvider, plUserKey, plGroupKey, false);
  }
  
  // Check if they have eDir as a resource - if not return doing nothing
  // Add or remove "groupMembership" attribute to eDir otherwise
  private void toggleItimGroup(tcDataProvider dataProvider, long plUserKey, long plGroupKey, Boolean addToGroup) {
 
    //First - check the group.  If it isn't ITIM, then bail
    if (!checkGroup(dataProvider, plGroupKey))
      return;

    tcUserOperationsIntf userIntf = null;
    logger.info(connectorName + ": "+(addToGroup?"add":"remove")+" ITIM group memebership: " + plUserKey);
    try {
      userIntf = (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
      

      // Get their duLDAPKey and EntryType so we can update eDir
      Map<String, String> a = new HashMap<String, String>();
      a.put("Users.Key", String.valueOf(plUserKey));
      String[] attrs = {ENTRYTYPE, attributeData.getOIMAttributeName("duLDAPKey")};
      tcResultSet myGuy = userIntf.findUsersFiltered(a, attrs);
      if (myGuy.getRowCount() == 1) {
        myGuy.goToRow(0); 
          
        // First off, check that this is a People entry.  We only deal with People.  
        String entryType = myGuy.getStringValue(ENTRYTYPE);
        if (entryType != null && entryType.equalsIgnoreCase(PEOPLE)) {
            
          if (hasEDir(plUserKey, userIntf)){

            String duLDAPKey = myGuy.getStringValue(attributeData.getOIMAttributeName("duLDAPKey"));
            
            try {
              LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
              // We assume this returns something cuz they are allegedly provisioned for eDir.
              SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);
  
              // Constructs a new instance of Attributes with one attribute.
              Attribute ourAttr = new BasicAttribute("groupMembership");     
              if (addToGroup){
                ourAttr.add(GROUP_NAME_EDIR);
              }
              Attributes modAttrs = new BasicAttributes();
              modAttrs.put(ourAttr);
              ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);
            } catch (Exception e2) {        
               logger.error(connectorName + ": Failed to update eDirectory group membership for "+duLDAPKey);
            }
          } else {
            logger.info(connectorName + ": attempted to add ITIM group to a user with no eDir resource:"+plUserKey);
          }
         
        } else {
          // This can happen when the creation of the edir resource happens after addition to the group - the issue is resolved in the provisioner
          logger.info(connectorName + ":Not adding group information for "+plUserKey+" because they aren't in people");
        }
      }else {
        // This is weird and should not happen - maybe I should throw an exception?
        logger.warn(connectorName + ": attempted to find user with key:"+plUserKey+" but could not find in OIM (or found too many?)");
      }
    } catch (Exception e) {
      logger.error(connectorName + ": exception occurred:"+ e.toString());
      return;
    }

    if (userIntf != null){
      userIntf.close();
    }
   }
  
  public String deprovisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey);
    
    // we don't need to do anything here
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    Boolean cnFound=false;
    
    logger.info(connectorName + ": Provision user: " + duLDAPKey);
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
          
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
   
    // see if the entry already exists in the target - We don't throw an error if they do.  Is that right?
    SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);

    // For all the sync attributes, look up their OIM equivalents.
    String[] allSyncAttributes = provisioningData.getAllAttributes();
    Set<String> allSyncAttributesOIMNames = new HashSet<String>();
    allSyncAttributesOIMNames.add(ENTRYTYPE); // We don't sync ENTRYTYPE but we need to check it
    for (int i = 0; i < allSyncAttributes.length; i++) {
      String attribute = (String) allSyncAttributes[i];
      if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
        allSyncAttributesOIMNames.addAll(OIMAPIWrapper.getOIMAffiliationFieldNames());
      } else {
        allSyncAttributesOIMNames.add(attributeData.getOIMAttributeName(attribute));
      }
    }
   
    // Get the current OIM values for this person and the attributes we are sync=ing
    tcUserOperationsIntf moUserUtility = null;
    tcResultSet moResultSet = null;
    try {
      moUserUtility =  (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
      Hashtable<String, String> mhSearchCriteria = new Hashtable<String ,String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, allSyncAttributesOIMNames.toArray(new String[0]));
    
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
      moResultSet.goToRow(0); 

    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    // Now, before we get down to business, check to make sure they are a person.  If not, quit without whining
    String entryType;
    try {
      entryType = moResultSet.getStringValue(ENTRYTYPE);
      if (entryType == null || !entryType.equalsIgnoreCase(PEOPLE)) {
        logger.info(connectorName + ": Returning. Tried to provision a user with entry type of:"+entryType);
        return SUCCESS;
      }
    } catch (Exception e1) {
      logger.error(connectorName + ": Returning. Exception reading entryType:"+duLDAPKey,e1);
      throw new RuntimeException("Failed while reading entryType");
    }

    Attributes addAttrs = new BasicAttributes();
    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String) iter.next();
      // What do we call this attribute over in the eDirectory?
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String value;
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      Attribute addAttr = new BasicAttribute(targetAttribute);     
      String attributeOIM = null;
      try {
        // Again, eduPersonAffiliation is odd - For each of its 6 values, see if they are set and then add a eduPerson record if they are
        if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
          Iterator<String> affiliationFieldsIter = OIMAPIWrapper.getOIMAffiliationFieldNames().iterator();
          while (affiliationFieldsIter.hasNext()) {
            attributeOIM = affiliationFieldsIter.next();
            String affiliationFieldValue = moResultSet.getStringValue(attributeOIM);
            if (affiliationFieldValue != null && affiliationFieldValue.equals("1")) {
              addAttr.add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(attributeOIM));
            }
          }
        } else if (attribute.equalsIgnoreCase("duNetIDStatus")) {
          // ITIM wants to know if the netid is expired or locked.
          attributeOIM = attributeData.getOIMAttributeName(attribute);
          value = moResultSet.getStringValue(attributeOIM);

          String uid = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
          uid = (uid.isEmpty() ? "missing":uid);
          if (value != null && value.equalsIgnoreCase("inactive")) {
            addAttr.add(uid+": expire at "+getDateTime());
          } else if (value != null && value.equalsIgnoreCase("locked")) {
            addAttr.add(uid+": locked at "+getDateTime());        
          }
        } else if (attribute.equalsIgnoreCase("hasChallengeResponse")){
          // This is stored as 1 and 0 in OIM.  Only store a 1 in EDIR
          String cr = "0";
          try {
            cr = moResultSet.getStringValue(attributeData.getOIMAttributeName("hasChallengeResponse"));
          } catch (Exception e) {
            throw new RuntimeException("Failed to get hasChallengeResponse from OIM." , e);
          }
          if (cr != null && !cr.equals("") && !cr.equals("0")){
            addAttr.add(cr);
          }
          
        } else {
          attributeOIM = attributeData.getOIMAttributeName(attribute);
          value = moResultSet.getStringValue(attributeOIM);
          
          if (value != null && !value.equals("")) {
            if (attribute.equalsIgnoreCase("cn")){
              cnFound=true;
            }
            
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
          // add object class
          Attribute addAttrOC = new BasicAttribute("objectClass");
          addAttrOC.add("duPerson");
          addAttrOC.add("eduPerson");
          addAttrOC.add("inetOrgPerson");
          addAttrOC.add("organizationalPerson");
          addAttrOC.add("Person");
          addAttrOC.add("ndsLoginProperties");
          addAttrOC.add("Top");
          addAttrOC.add("posixAccount");
          addAttrOC.add("duProvision");
          addAttrs.put(addAttrOC);
        } else {
          addAttrs.put(addAttr);
        }
        
      } catch (tcColumnNotFoundException e) {
        throw new RuntimeException("Failed while retrieving attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      } catch (tcAPIException e) {
        throw new RuntimeException("Failed while retrieving attribute value for " + attributeOIM + ": " + e.getMessage(), e);
        } catch (Exception e) {
        throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
      }

    }
    
    if (!cnFound) {
      Attribute addCN = new BasicAttribute("cn");
      addCN.add("BOGUS");
      addAttrs.put(addCN);
    }

    // Check if this user is in the ITIM group and add their isMemberOf if they are.
    
    tcGroupOperationsIntf moGroupUtility = null;
    try {
      moGroupUtility = (tcGroupOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcGroupOperationsIntf");       
      Map<String,String> map = new HashMap<String,String>();
      map.put("Groups.Group Name", GROUPNAME);
      tcResultSet myGroup = moGroupUtility.findGroups(map);
     if (moGroupUtility.isUserGroupMember(myGroup.getLongValue("Groups.Key"),moResultSet.getLongValue("Users.Key"))){
         Attribute groupAttr = new BasicAttribute("groupMembership",GROUP_NAME_EDIR);
        addAttrs.put(groupAttr);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while trying to set up ITIM group membership: " + e.getMessage(), e);
    }
  
    if (result == null) {
      // add attributes needed for new entry
      ldapConnectionWrapper.createEntry(duLDAPKey, duLDAPKey, addAttrs);
    } else {
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), addAttrs);
    }

    if (moUserUtility != null){
      moUserUtility.close();
    }
    if (moGroupUtility != null) {
      moGroupUtility.close();
    }
    logger.info(connectorName + ": Provision user: " + duLDAPKey + ".  Returning success.");
    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String unused1,
      String attribute, String unused2, String newValue) {
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue);
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here one attribute in OIM is being updated.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
   
    Attributes modAttrs = new BasicAttributes();
    
    // We need to fetch a couple fields from the user's OIM record:
    Set<String> attrs = new HashSet<String>( OIMAPIWrapper.getOIMAffiliationFieldNames());
    attrs.add(attributeData.getOIMAttributeName("uid"));
    attrs.add(attributeData.getOIMAttributeName("hasChallengeResponse"));
    attrs.add(ENTRYTYPE);
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, attrs.toArray(new String[0]));
        
    // Now, before we get down to business, check to make sure they are a person.  If not, quit without whining
    try {
      String entryType = moResultSet.getStringValue(ENTRYTYPE);
      if (entryType == null || !entryType.equalsIgnoreCase(PEOPLE)) {
        logger.info(connectorName + ": Returning. Tried to update a user with entry type of:"+entryType);
        return SUCCESS;
      }
    } catch (Exception e1) {
      logger.error(connectorName + ": Returning. Exception reading entryType:"+duLDAPKey,e1);
      throw new RuntimeException("Failed while reading entryType");
    }
   
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);
    
    if (result == null) {
      // throw an exception if this occurs
      throw new RuntimeException("Entry with duLDAPKey=" + duLDAPKey + " not found in target.");
    }

    Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attribute));
    if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
      // the attribute is eduPersonAffiliation so we're going to query OIM for all affiliation values
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
    } else if (attribute.equalsIgnoreCase("duNetIDStatus")) {
      // ITIM wants to know if the netid is expired or locked.
      String uid = "netid";
      try {
        uid = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
        uid = (uid.isEmpty() ? "netid":uid);
      } catch (Exception e) {
        throw new RuntimeException("Failed to get uid value from OIM." , e);
      }
      if (newValue != null && newValue.equalsIgnoreCase("inactive")) {
        modAttr.add(uid+": expire at "+getDateTime());
      } else if (newValue != null && newValue.equalsIgnoreCase("locked")) {
        modAttr.add(uid+": locked at "+getDateTime());        
      }
    } else if (attribute.equalsIgnoreCase("hasChallengeResponse")){
      // This is store as 1 and 0 in OIM.  Only store a 1 in EDIR
      String cr = "0";
      try {
        cr = moResultSet.getStringValue(attributeData.getOIMAttributeName("hasChallengeResponse"));
      } catch (Exception e) {
        throw new RuntimeException("Failed to get hasChallengeResponse from OIM." , e);
      }
      if (cr != null && !cr.equals("") && !cr.equals("0")){
        modAttr.add(cr);
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
    String dn = result.getNameInNamespace();
    
    // These are all the object classes that can be required.  To save time we will only check for the ones we KNOW we need here.
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "duPerson");
    ldapConnectionWrapper.checkAndAddObjectClass(dn, "eduPerson");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "inetOrgPerson");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "organizationalPerson");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "Person");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "ndsLoginProperties");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "Top");
    ldapConnectionWrapper.checkAndAddObjectClass(dn, "posixAccount");
    // ldapConnectionWrapper.checkAndAddObjectClass(dn, "duProvision");
    
    ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);
    
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
        logger.error(connectorName + ": Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
    } catch (Exception e) {
      logger.error(connectorName + ": Failed while querying OIM: " + duLDAPKey,e);
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return moResultSet;
  }
  
  private String getDateTime() {
    //Mon Jan  5 11:00:00 EST 2009 
    DateFormat dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy");
    Date date = new Date();
    return dateFormat.format(date);
  }
  
  private Boolean checkGroup(tcDataProvider dataProvider, long plGroupKey) {
    tcGroupOperationsIntf moGroupUtility = null;
    Boolean rightGroup = false;
    try {
      moGroupUtility = (tcGroupOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcGroupOperationsIntf");       
      Map<String,String> map = new HashMap<String,String>();
      map.put("Groups.Group Name", GROUPNAME);
      tcResultSet moResultSet = moGroupUtility.findGroups(map);
      if (moResultSet.getRowCount() != 1) {
        logger.error("Got " + moResultSet.getRowCount() + " rows for group " + GROUPNAME);
      } else {
        rightGroup =  (plGroupKey == moResultSet.getLongValue("Groups.Key"));
      }
    } catch (Exception e) {
      logger.error(connectorName + ": Error encountered checking Group:"+e.toString());
    }
    if (moGroupUtility != null) {
      moGroupUtility.close();
    }
    return rightGroup;
  }

  private Boolean hasEDir(long plUserKey, tcUserOperationsIntf userIntf) {
    Boolean hasEDir = false;
    try {
      //Create  result set for the objects provisioned to the user:
      tcResultSet ul = userIntf.getObjects(plUserKey);
  
      //Loop through this result set checking for the EDIR resource
      for (int j = 0; j < ul.getRowCount(); j++) {
        ul.goToRow(j);
        String objName = ul.getStringValue("Objects.Name");
        String objStatus = ul.getStringValue("Objects.Object Status.Status");
        if (objName.equalsIgnoreCase(connectorName) && objStatus.equalsIgnoreCase("Provisioned")) {
           hasEDir = true;
        }
      }
    } catch (Exception e) {
      logger.error(connectorName + ": Error Occurred fetching EDIR provisioned status:"+e.toString());
    }
    return hasEDir;
  }
  
}
