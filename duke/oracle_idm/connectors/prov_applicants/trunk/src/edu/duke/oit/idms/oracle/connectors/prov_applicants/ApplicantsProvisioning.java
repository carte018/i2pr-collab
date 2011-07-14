package edu.duke.oit.idms.oracle.connectors.prov_applicants;

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

import javax.naming.directory.SearchResult;



/**
 * @author shilen
 *
 */
public class ApplicantsProvisioning extends SimpleProvisioning {

  public final static String connectorName = "APPLICANT_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  private String[] mailAlternateAddressAttrs = {"USR_UDF_ACPUBEMAIL", "USR_UDF_EMAILALIAS", 
      "USR_UDF_USEREMAIL", "USR_UDF_DEMPOEMAIL", "USR_UDF_DEPARTMENTALEMAIL"};
  
  public String deprovisionUser(tcDataProvider dataProvider, String uid, String entryType) {
    logger.info(connectorName + ": Deprovision user: " + uid);
    
    // we don't need to do anything here
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String uid, String entryType) {
    logger.info(connectorName + ": Provision user: " + uid);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (uid == null || uid.equals("")) {
      throw new RuntimeException("No uid available.");
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
      
      
      Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
      mhSearchCriteria.put("Users.User ID", uid);
      mhSearchCriteria.put("USR_UDF_ENTRYTYPE","applicants");
      mhSearchCriteria.put("Users.Status","Active");
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, allSyncAttributesOIMNames);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for uid " + uid);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
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
        modAttrs.put(modAttr);
      }
      
      // Never put a null value for a create // modAttrs.put(modAttr);
    }
    // Add the attributes that aren't part of the OIM schema but are required for 
    // new entries
    BasicAttribute fixedAttr = new BasicAttribute("objectClass");
    fixedAttr.add("top");
    fixedAttr.add("person");
    fixedAttr.add("organizationalPerson");
    fixedAttr.add("inetOrgPerson");
    fixedAttr.add("duPerson");
    fixedAttr.add("duOperational");
    modAttrs.put(fixedAttr);
    
    fixedAttr = new BasicAttribute("duK5Req");
    fixedAttr.add("0");
    modAttrs.put(fixedAttr);
    
    fixedAttr = new BasicAttribute("duSSLReq");
    fixedAttr.add("1");
    modAttrs.put(fixedAttr);
    
    fixedAttr = new BasicAttribute("cn");
    
    try {
    fixedAttr.add(moResultSet.getStringValue("Users.First Name") + " " + moResultSet.getStringValue("Users.Last Name"));
    } catch (Exception e) {
    	throw new RuntimeException ("Failed to load first and last name computing cn for new user due to " + e.getMessage(),e);
    }
    modAttrs.put(fixedAttr);
    
    ldapConnectionWrapper.createEntry(uid, modAttrs);

    logger.info(connectorName + ": Provision user: " + uid + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String uid, String entryType,
      String attribute, String oldValue, String newValue) {
    logger.info(connectorName + ": Update user: " + uid + ", attribute=" + attribute + ", oldValue=" + oldValue + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we're just updating one attribute.
    
    if (uid == null || uid.equals("")) {
      throw new RuntimeException("No uid available.");
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
    // Special casing for changes to Username or changes to givenName or sn
    if (attribute.equals("Username") || attribute.equals("uid")) {
    	// special case for Username change
    	// we need to rename the object, which means finding it by another value
    	//
    	 tcResultSet moResultSet = null;
    	 try {
    	      tcUserOperationsIntf moUserUtility = 
    	        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
    	      
    	      String[] syncAttributes = {"duPSEmplID"};
    	      String[] syncAttributesOIMNames = {"USR_UDF_PSEMPLID"};
    	      
    	      
    	      Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
    	      mhSearchCriteria.put("Users.User ID", uid);
    	      mhSearchCriteria.put("USR_UDF_ENTRYTYPE","applicants");
    	      mhSearchCriteria.put("Users.Status","Active");
    	      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, syncAttributesOIMNames);
    	      
    	      if (moResultSet.getRowCount() != 1) {
    	        throw new RuntimeException("Did not find exactly one entry in OIM for uid " + uid);
    	      }
    	    } catch (Exception e) {
    	      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    	    }
	    SearchResult found;
	    try {
    	found = ldapConnectionWrapper.findEntryByEmplID(moResultSet.getStringValue("USR_UDF_PSEMPLID"), new String [] {"dn"});
    	// Now, with the DN, perform the rename operation and update the uid value as we go
	    } catch (Exception e) {
	    	throw new RuntimeException("findEntryByEmplID failed with " + e.getMessage(),e);
	    }
	    if (found == null) {
	    	try {
	    	logger.info(connectorName + ": Got back a null value found from findEntryByEmplID for " + uid + " using " + moResultSet.getStringValue("USR_UDF_PSEMPLID"));
	    	} catch (Exception e) {
	    		logger.info(connectorName + ": Got back an exception reporting null return from findEntryByEmplID");
	    	}
	    }
    	ldapConnectionWrapper.rename(found.getNameInNamespace(),ldapConnectionWrapper.getDn(newValue,null));
    	Attributes mu = new BasicAttributes();
    	BasicAttribute newuid = new BasicAttribute("uid");
    	newuid.add(newValue);
    	mu.put(newuid);
    	ldapConnectionWrapper.replaceAttributes(uid,entryType,mu);
    } else if (attribute.equals("givenName") || attribute.equals("sn")) {
    	//Special case in update of cn value too
    	BasicAttribute newcn = new BasicAttribute("cn");
    	tcResultSet moResultSet = null;
   	 try {
   	      tcUserOperationsIntf moUserUtility = 
   	        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
   	      
   	      String[] syncAttributes = {"givenName","sn", "duPSEmplID"};
   	      String[] syncAttributesOIMNames = {"Users.First Name", "Users.Last Name","USR_UDF_PSEMPLID"};
   	      
   	      
   	      Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
   	      mhSearchCriteria.put("Users.User ID", uid);
   	      mhSearchCriteria.put("USR_UDF_ENTRYTYPE","applicants");
   	      mhSearchCriteria.put("Users.Status","Active");
   	      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, syncAttributesOIMNames);
   	      
   	      if (moResultSet.getRowCount() != 1) {
   	        throw new RuntimeException("Did not find exactly one entry in OIM for uid " + uid);
   	      }
   	    } catch (Exception e) {
   	      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
   	    }	
	    try {
	    newcn.add(moResultSet.getStringValue("Users.First Name") + " " + moResultSet.getStringValue("Users.Last Name"));
	    } catch (Exception e) {
	    	throw new RuntimeException("Failed while computing cn during givenName or sn update with " + e.getMessage(),e);
	    }
	    modAttrs.put(newcn);
	    ldapConnectionWrapper.replaceAttributes(uid,entryType,modAttrs);
    } else {
    
    ldapConnectionWrapper.replaceAttributes(uid, entryType, modAttrs);
    
    }
    
    logger.info(connectorName + ": Update user: " + uid + ", attribute=" + attribute + ", oldValue=" + oldValue + ", newValue=" + newValue + ".  Returning success.");

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
