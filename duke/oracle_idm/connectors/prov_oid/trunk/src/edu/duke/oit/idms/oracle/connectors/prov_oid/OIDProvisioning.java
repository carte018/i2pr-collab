package edu.duke.oit.idms.oracle.connectors.prov_oid;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

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


/**
 * @author shilen, rob
 *
 */
public class OIDProvisioning extends SimpleProvisioning {

  /** name of connector for logging purposes */
  public final static String connectorName = "OID_PROVISIONING";
  private String[] getlist = {"uid"};
  private String[] otherlist = {"cn"};


  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  public String deprovisionUser(tcDataProvider dataProvider, String uid, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + uid + "is ignored by " + connectorName);

    // We ignore deletions
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String uniqueid, String unused1) {
    logger.info(connectorName + ": Provision user: " + uniqueid);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (uniqueid == null || uniqueid.equals("")) {
      throw new RuntimeException("No uniqueid attribute (dudukeid) available.");
    }
    
    // Otherwise, we have a unique ID passed in -- let's use it.
    //
    
    Attributes modAttrs = new BasicAttributes();
    tcResultSet moResultSet = null;
    
    try {
    	tcUserOperationsIntf moUserUtility =
    		(tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
    	
    	Set<String> setOfSyncAttributes = null;
    	setOfSyncAttributes = provisioningData.getSyncAttributes();
    	String[] allSyncAttributes = setOfSyncAttributes.toArray(new String[setOfSyncAttributes.size()]);
    	String[] allSyncAttributesOIMNames = new String[allSyncAttributes.length];
    	for (int i = 0; i < allSyncAttributes.length; i++) {
			logger.info(connectorName + " Getting attribute name for " + allSyncAttributes[i]);
    		allSyncAttributesOIMNames[i] = attributeData.getOIMAttributeName(allSyncAttributes[i]);
    		logger.info(connectorName + " Added " + allSyncAttributesOIMNames[i] + "to attribute list");
    	}
    	
    	Hashtable mhSearchCriteria = new Hashtable();
    	mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
    	moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,allSyncAttributesOIMNames);
    	
    	if (moResultSet.getRowCount() != 1) {
    		logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
    		throw new RuntimeException("Did not find exactly one entry in OIM for  uniqueid = " + uniqueid);
    	}
    } catch (Exception e) {
    	logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
    	throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
    }
    // Now create the entry
    
    // Check if entry already exists
    SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
    
    // Collect attributes
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
    	  attributeOIM = attributeData.getOIMAttributeName(attribute);
          value = moResultSet.getStringValue(attributeOIM);
                
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
          if (result == null) {
              // if the entry does not exist, only add attribute if it has values
              if (addAttr.size() > 0) {
                addAttrs.put(addAttr);
              }
          }else {
              addAttrs.put(addAttr);
          }
      } catch (Exception e) {
    	  throw new RuntimeException("Failed while retrieving attribute value for " + attributeOIM + ": " + e.getMessage(),e);
      }
    }
    String cnvalue = null;
    try {
    	if (result == null && moResultSet.getStringValue(attributeData.getOIMAttributeName("uid")) != null && !moResultSet.getStringValue(attributeData.getOIMAttributeName("sn")).equals("BOGUS")) {
    		// No entry exists, so we need to create one
    		//
    		// add object class
    		Attribute addAttrOC = new BasicAttribute("objectClass");
    		addAttrOC.add("top");
    		addAttrOC.add("person");
    		addAttrOC.add("organizationalPerson");
    		addAttrOC.add("inetorgperson");
    		addAttrOC.add("orcluser");
    		addAttrOC.add("orcluserV2");
    		addAttrs.put(addAttrOC);
    		
    		// acquire DN value from OIM data
    		try {
    			cnvalue = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
    			ldapConnectionWrapper.createEntry(cnvalue,addAttrs);
    		} catch (Exception e) {
    			logger.info(connectorName + ": Throwing creation failure for cn=" + cnvalue + " of " + e.getMessage());
    			throw new RuntimeException("Failed in creation of new entry for cn " + cnvalue + " due to " + e.getMessage(),e);
    		}
    	}
    } catch (Exception e) {
    	// ignore exception here
    }
    logger.info("Successfully created new user cn=" + cnvalue);
    return SUCCESS;
  }

  private void privacyProtect(tcDataProvider dataProvider, String uniqueid, String sourceAttribute) {
	  /**
	   * Here, we take an attribute name and privacy protect it based on its privacy
	   * attribute being set to N.  A separate routine handles unprotecting and
	   * resetting the attribute.
	   * 
	   * When blocking an attribute, we replace its value with the string
	   * "NOT PUBLISHED".  All the privacy-controlled attributes we run across
	   * are string-typed, so we can use that sequence for any of them.
	   */
	  
	  logger.info(connectorName + ": privacyProtect " + sourceAttribute + " for " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  try {
		  Attributes curuserAttrs = result.getAttributes();
		  Attributes modAttrs = new BasicAttributes();
		  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
		  String curuserDN = (String) curuserAttrs.get("uid").get();
		  modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
		  modAttr.add("NOT PUBLISHED");
		  modAttrs.put(modAttr);
		  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
	  } catch (Exception e) {
		  	logger.info(connectorName + " Throwing exception blocking " + sourceAttribute + " for " + uniqueid);
		  	throw new RuntimeException("Failed blocking attribute " + sourceAttribute +" for " + uniqueid + " due to " + e.getMessage(), e);
	  }
  }
  
  private void privacyRemove(tcDataProvider dataProvider, String uniqueid, String sourceAttribute) {
	  /**
	   * Here, we take an attribute name and privacy protect it based on its privacy
	   * attribute being set to N.  A separate routine handles unprotecting and
	   * resetting the attribute.
	   * 
	   * When blocking an attribute, we replace its value with the string
	   * "NOT PUBLISHED".  All the privacy-controlled attributes we run across
	   * are string-typed, so we can use that sequence for any of them.
	   */
	  
	  logger.info(connectorName + ": privacyProtect " + sourceAttribute + " for " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  try {
		  Attributes curuserAttrs = result.getAttributes();
		  Attributes modAttrs = new BasicAttributes();
		  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
		  String curuserDN = (String) curuserAttrs.get("uid").get();
		  modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
		  modAttr.add("");
		  modAttrs.put(modAttr);
		  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
	  } catch (Exception e) {
		  	logger.info(connectorName + " Throwing exception blocking " + sourceAttribute + " for " + uniqueid);
		  	throw new RuntimeException("Failed blocking attribute " + sourceAttribute +" for " + uniqueid + " due to " + e.getMessage(), e);
	  }
  }
  
  private void privacyUnProtect(tcDataProvider dataProvider,String uniqueid,String sourceAttribute) {
	  /** 
	   * Here, we have removed or flipped a privacy protecting attribute so as to allow the 
	   * attribute it protects to be propagated.  We mine the OIM database for the value to
	   * assign to the privacy-controlled attribute and set it to that value.
	   */
	  logger.info(connectorName + ": privacyUnProtect " + sourceAttribute + " for " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  Attributes modAttrs = new BasicAttributes();
	  tcResultSet moResultSet = null;

	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  String[] mogetattrs = { mogetattr };
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  try {
		  	logger.info("New value for " + sourceAttribute + " is " + moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
	  } catch (Exception e) {
		  // This we can ignore
	  }
	  // and update the value
	  try {
		  Attributes curuserAttrs = result.getAttributes();
		  String curuserDN = (String) curuserAttrs.get("uid").get();
		  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
		  modAttr.add(moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
		  modAttrs.put(modAttr);
		  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception setting " + sourceAttribute);
		  throw new RuntimeException("Failed to update " + sourceAttribute + " with " + e.getMessage(),e);
	  }
  }
  
  
  private void cloneIfNoPrivacy(tcDataProvider dataProvider,String uniqueid,String sourceAttribute,String targetAttribute) {
	  /** 
	   * Here, we have removed or flipped a privacy protecting attribute so as to allow the 
	   * attribute it protects to be propagated.  We mine the OIM database for the value to
	   * assign to the privacy-controlled attribute and set it to that value.
	   */
	  logger.info(connectorName + ": cloneIfNoPrivacy " + sourceAttribute + " for " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  Attributes modAttrs = new BasicAttributes();
	  tcResultSet moResultSet = null;

	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  String mogetattrprivacy = null;
		  String moentryprivacy = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  moentryprivacy = attributeData.getOIMAttributeName("duEntryPrivacy");
		  if (sourceAttribute.equals("duTelephone1") || sourceAttribute.equals("duDukePager") || sourceAttribute.equals("mail")) {
			  mogetattrprivacy = attributeData.getOIMAttributeName(sourceAttribute + "Privacy");
			  String[] mogetattrs = { mogetattr, mogetattrprivacy, moentryprivacy };
			  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  } else {
			  String[] mogetattrs = { mogetattr, moentryprivacy };
			  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  }
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  try {
		  	logger.info("Title in OIM for " + sourceAttribute + " is " + attributeData.getOIMAttributeName(sourceAttribute));
		  	
		  	logger.info("New value for " + sourceAttribute + " is " + moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
	  } catch (Exception e) {
		  // This we can ignore
	  }
	  // determine if there is a privacy attribute set for this attribute and if there is
	  // do nothing silently.
	  String privacyValue = null;
	  String entryprivacyValue = null;
	  try {
		  entryprivacyValue = moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy"));
		  logger.info("entryPrivacy value is " + entryprivacyValue);
		  logger.info("Privacy attribute internal name is " + attributeData.getOIMAttributeName(sourceAttribute + "Privacy"));
		  privacyValue = moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute + "Privacy"));
	  } catch (Exception e) {
		  // Ignore at this point -- column not found should be impossible, or irrelevant if it happens
	  }
		  if ((privacyValue != null && privacyValue.equals("N")) || (entryprivacyValue != null && entryprivacyValue.equals("N"))) {
		  // No update -- there's a privacy block in effect
			  logger.info("No update due to privacy block");
	  } else {
		  // and update the value
		  try {
			  Attributes curuserAttrs = result.getAttributes();
			  String curuserDN = (String) curuserAttrs.get("uid").get();
			  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(targetAttribute));
			  modAttr.add(moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
			  modAttrs.put(modAttr);
			  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
		  } catch (Exception e) {
			  logger.info(connectorName + " Throwing exception setting " + sourceAttribute);
			  throw new RuntimeException("Failed to update " + sourceAttribute + " with " + e.getMessage(),e);
		  }
	  }
  }
  
  private void syncIfNoPrivacy(tcDataProvider dataProvider,String uniqueid,String sourceAttribute) {
	  /** 
	   * Here, we have removed or flipped a privacy protecting attribute so as to allow the 
	   * attribute it protects to be propagated.  We mine the OIM database for the value to
	   * assign to the privacy-controlled attribute and set it to that value.
	   */
	  logger.info(connectorName + ": syncIfNoPrivacy " + sourceAttribute + " for " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  Attributes modAttrs = new BasicAttributes();
	  tcResultSet moResultSet = null;

	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  String mogetattrprivacy = null;
		  String moentryprivacy = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  moentryprivacy = attributeData.getOIMAttributeName("duEntryPrivacy");
		  if (sourceAttribute.equals("duTelephone1") || sourceAttribute.equals("duDukePager") || sourceAttribute.equals("mail")) {
			  if (sourceAttribute.equals("mail")) {
				  mogetattrprivacy = attributeData.getOIMAttributeName("du" + sourceAttribute + "Privacy");
			  } else {
			  mogetattrprivacy = attributeData.getOIMAttributeName(sourceAttribute + "Privacy");
			  }
			  String[] mogetattrs = { mogetattr, mogetattrprivacy, moentryprivacy };
			  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  } else {
			  String[] mogetattrs = { mogetattr, moentryprivacy };
			  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  }
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  try {
		  	logger.info("Title in OIM for " + sourceAttribute + " is " + attributeData.getOIMAttributeName(sourceAttribute));
		  	
		  	logger.info("New value for " + sourceAttribute + " is " + moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
	  } catch (Exception e) {
		  // This we can ignore
	  }
	  // determine if there is a privacy attribute set for this attribute and if there is
	  // do nothing silently.
	  String privacyValue = null;
	  String entryprivacyValue = null;
	  try {
		  entryprivacyValue = moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy"));
		  logger.info("entryPrivacy value is " + entryprivacyValue);
		  logger.info("Privacy attribute internal name is " + attributeData.getOIMAttributeName(sourceAttribute + "Privacy"));
		  privacyValue = moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute + "Privacy"));
	  } catch (Exception e) {
		  // Ignore at this point -- column not found should be impossible, or irrelevant if it happens
	  }
		  if ((privacyValue != null && privacyValue.equals("N")) || (entryprivacyValue != null && entryprivacyValue.equals("N"))) {
		  // No update -- there's a privacy block in effect
			  logger.info("No update due to privacy block");
	  } else {
		  // and update the value
		  try {
			  Attributes curuserAttrs = result.getAttributes();
			  String curuserDN = (String) curuserAttrs.get("uid").get();
			  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(sourceAttribute));
			  modAttr.add(moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
			  modAttrs.put(modAttr);
			  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
		  } catch (Exception e) {
			  logger.info(connectorName + " Throwing exception setting " + sourceAttribute);
			  throw new RuntimeException("Failed to update " + sourceAttribute + " with " + e.getMessage(),e);
		  }
	  }
  }
  
  private void cloneAttribute(tcDataProvider dataProvider,String uniqueid,String sourceAttribute,String targetAttribute) {
	  /** 
	   * Here, we need to clone the value of one attribute in OIM into the value of another 
	   * attribute in the OID.  This occurs on certain privacy transitions and in cases where
	   * one attibute (eg. "uid") is part of more than one attribute in the OID (eg., "uid"
	   * as well as "cn".  
	   */
	  logger.info(connectorName + ": cloneAttribute " + sourceAttribute + " for " + uniqueid + " to " + targetAttribute);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  Attributes modAttrs = new BasicAttributes();
	  tcResultSet moResultSet = null;

	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  String[] mogetattrs = { mogetattr };
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  try {
		  	logger.info("New value for " + targetAttribute + " is " + moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
	  } catch (Exception e) {
		  // This we can ignore
	  }
	  // and update the value
	  try {
		  Attributes curuserAttrs = result.getAttributes();
		  String curuserDN = (String) curuserAttrs.get("uid").get();
		  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(targetAttribute));
		  modAttr.add(moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
		  modAttrs.put(modAttr);
		  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception setting " + targetAttribute);
		  throw new RuntimeException("Failed to update " + targetAttribute + " with " + e.getMessage(),e);
	  }
  }
  
  private void cloneAttributeByCN(tcDataProvider dataProvider,String uniqueid,String sourceAttribute,String targetAttribute) {
	  /** 
	   * Here, we need to clone the value of one attribute in OIM into the value of another 
	   * attribute in the OID.  This occurs on certain privacy transitions and in cases where
	   * one attibute (eg. "uid") is part of more than one attribute in the OID (eg., "uid"
	   * as well as "cn".  
	   */
	  logger.info(connectorName + ": cloneAttribute " + sourceAttribute + " for " + uniqueid + " to " + targetAttribute);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  SearchResult result = null;
	  try {
		  result = ldapConnectionWrapper.findEntryByUniqueIDUsingCN(uniqueid,otherlist);
	  } catch (Exception e) {
		  logger.info("Failure within ldap search by uniqueid in cloneAttributeByCN of " + e.getMessage());
	  }
	  Attributes modAttrs = new BasicAttributes();
	  tcResultSet moResultSet = null;

	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  String[] mogetattrs = { mogetattr };
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  try {
		  	logger.info("New value for " + targetAttribute + " is " + moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
	  } catch (Exception e) {
		  // This we can ignore
	  }
	  // and update the value
	  try {
		  Attributes curuserAttrs = result.getAttributes();
		  String curuserDN = (String) curuserAttrs.get("cn").get();
		  logger.info("byCN value is " + curuserDN);
		  Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(targetAttribute));
		  modAttr.add(moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute)));
		  modAttrs.put(modAttr);
		  ldapConnectionWrapper.addOrReplaceAttributes(curuserDN,null,modAttrs);
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception setting " + targetAttribute);
		  throw new RuntimeException("Failed to update " + targetAttribute + " with " + e.getMessage(),e);
	  }
  }
  
  private void modRDN(tcDataProvider dataProvider, String uniqueid,String sourceAttribute,String namingAttribute) {
	  logger.info(connectorName + ": Modifying RDN of " + uniqueid);
	  LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
	  // Here, we update the DN of the target.  
	  // We have the uniqueid value for the entry whose DN we're changing.  We have to 
	  // acquire the uid value that we're changing to from OIM and the original DN from 
	  // the OID.
	  // Because this is an RDN change, we have to also update the naming attribute used
	  // in the RDN, and because we may be cloning a different value, we use the sourceAttribute
	  // argument to source the value.
	  SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
	  String oldDN = result.getNameInNamespace();
	  String oldRDN = result.getName();
	  tcResultSet moResultSet = null;
	  try {
		  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
		  Hashtable mhSearchCriteria = new Hashtable();
		  mhSearchCriteria.put(attributeData.getOIMAttributeName("dudukeid"),uniqueid);
		  String mogetattr = null;
		  mogetattr = attributeData.getOIMAttributeName(sourceAttribute);
		  String[] mogetattrs = { mogetattr };
		  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,mogetattrs);
		  if (moResultSet.getRowCount() != 1) {
			  logger.info(connectorName + " Throwing exception for too many/few entries found in OIM");
			  throw new RuntimeException("Did not find exactly one entry in OIM for uniqueied = " + uniqueid);
		  }
	  } catch (Exception e) {
		  logger.info(connectorName + " Throwing exception for failed OIM query" + e.getMessage());
		  throw new RuntimeException("Failed querying OIM: " + e.getMessage(),e);
	  }
	  String newuid = null;
	  Attributes modAttrs = new BasicAttributes();
	  try {
		  newuid = moResultSet.getStringValue(attributeData.getOIMAttributeName(sourceAttribute));
		  ldapConnectionWrapper.rename(oldDN,"cn="+newuid+",cn=users,dc=oit,dc=duke,dc=edu");
	  } catch (Exception e) {
		  logger.info(connectorName + ": Failed to modify DN for " + oldDN + " with uid " + newuid);
		  throw new RuntimeException("Failed to update DN from " + oldDN + " to cn="+newuid+",cn=user,dc=oit,dc=duke,dc=edu with " + e.getMessage(),e); 
	  }
	  logger.info(connectorName + ": ModRDN completed successfully from " + oldDN + " to cn=" + newuid);
  }

  
  public String updateUser(tcDataProvider dataProvider, String uniqueid, String unused1,
      String attribute, String unused2, String newValue) {
    logger.info(connectorName + ": Update user: " + uniqueid + ", attribute=" + attribute + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    // here one attribute in OIM is being updated.
    
    logger.info(connectorName + ": Update user: " + uniqueid);
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (uniqueid == null || uniqueid.equals("")) {
      throw new RuntimeException("No dudukeid attribute (duke unique id) available.");
    }
    
    // Otherwise, we have a dukeid passed in -- let's use it.
    //
    
    // Check for existence of entry in the target and throw an exception
    // if the entry is not there
    
    SearchResult result = ldapConnectionWrapper.findEntryByUniqueID(uniqueid,getlist);
    if (result == null) {
    	logger.info(connectorName + " Failed to find uniqueid " + uniqueid + " in OID");
    	throw new RuntimeException("Failed to find uniqueid " + uniqueid + " in OID");
    } else {
    	logger.info(connectorName + " Found entry for " + uniqueid);
    }
    
    Attributes modAttrs = new BasicAttributes();
    tcResultSet moResultSet = null;
    
    /** 
     * If the attribute coming in is one which requires logic,
     * perform the logic on it.  If it is instead one that we are 
     * willing to sync, synchronize its target value directly.
     */
    logger.info("Operating on attribute" + attribute);
    
    if (provisioningData.isLogicAttribute(attribute)) {
    	// Here we need to add some custom code
    	if (attribute.equals("description")) {
    		// no update if a block is in place
    		syncIfNoPrivacy(dataProvider,uniqueid,"displayName");
    	}
    	if (attribute.equals("telephonenumber")) {
    		// Logic here is to trap changes occurring with blocks in place
    		// If there's a privacy block, don't propagate the change.
    		syncIfNoPrivacy(dataProvider,uniqueid,"duTelephone1");
    	} else if (attribute.equals("pager")) {
    		// Logic here is to trap changes occurring with blocks in place
    		// if there's a privacy block, don't propagate the change.
    		syncIfNoPrivacy(dataProvider,uniqueid,"duDukePager");
    	} else if (attribute.equals("sn")) {
    		// If there's a block on the entry, this can't change
    		syncIfNoPrivacy(dataProvider,uniqueid,"sn");
    	} else if (attribute.equals("duMailPrivacy")) {
    		// When duMailPrivacy changes, we check mail and either obliterate it or 
    		// sync it as appropriate.  We use privacyRemove() rather than 
    		// privacyProtect(), since we prefer an empty value to an invalid address
    		// in the private case.
    		if (newValue == null || newValue.equals("")) {
    			privacyUnProtect(dataProvider,uniqueid,"mail");
    		} else if (newValue.equals("N")) {
    			privacyRemove(dataProvider,uniqueid,"mail");
    		}
    	} else if (attribute.equals("mail")) {
    		// Mail is only sync'd if there's not a privacy block in place
    		syncIfNoPrivacy(dataProvider,uniqueid,"mail");
    	} else if (attribute.equals("uid")) {
    		// uid is a special case for us.  The uid value is both propagated into
    		// the value of cn and used as the RDN (through the value of cn), so 
    		// a change to uid requires changes in multiple other places.
    		// uid value gets updated, cloned to cn, and rdn gets changed
    		// uid changes override entryprivacy settings
    		modRDN(dataProvider,uniqueid,"uid","cn");
    		cloneAttributeByCN(dataProvider,uniqueid,"uid","uid");
    	} else if (attribute.equals("eduPersonNickname")) {
    		// epNN is used here as an override for givenName
    		// if it's being set to a value, use that value to override the 
    		// givenName
    		// if it's being unset, clone givenName to givenName
    		if (newValue == null || newValue.equals("")) {
    			syncIfNoPrivacy(dataProvider,uniqueid,"givenName");
    		} else {
    			cloneIfNoPrivacy(dataProvider,uniqueid,"eduPersonNickName","givenName");
    		}
    	} else if (attribute.equals("givenName")) {
    		// givenName is strange in that it can be privacy hidden but it has no 
    		// explicit privacy attribute
    		syncIfNoPrivacy(dataProvider,uniqueid,"givenName");
    	} else if (attribute.equals("duTelephone1Privacy")) {
    		// When duTelephone1Privacy changes, we check duTelephone1 and 
    		// obliterate it or sync it as appropriate.
    		if (newValue == null || newValue.equals("")) {
    			privacyUnProtect(dataProvider,uniqueid,"duTelephone1");
    		} else if (newValue.equals("N")) {
    			privacyProtect(dataProvider,uniqueid,"duTelephone1");
    		}
    	} else if (attribute.equals("duDukePagerPrivacy")) {
    		// When duDukePagerPrivacy changes, we check duDukePager and 
    		// either set it or set it to an empty value in the OID to 
    		// address privacy concerns.
    		if (newValue == null || newValue.equals("")) {
    				// we are clearing the value of the attribute
    				// pager can now be projected to the other side
    				privacyUnProtect(dataProvider,uniqueid,"duDukePager");
    		} else if (newValue.equals("N")) {
    			// we are blocking pager number -- remove the value from OID
    				privacyProtect(dataProvider,uniqueid,"duDukePager");
    		}
    	} else if (attribute.equals("duEntryPrivacy")) {
    		// When duEntryPrivacy changes, we have a number of attributes to manage
    		// since the attribute essentially controls access to all protected 
    		// attributes in the entry.
    		if (newValue == null || newValue.equals("")) {
    			// We are clearing the value of the attribute -- resync all the attributes
    			// we care about by privacyUnProtect()ing them individually.
    			//
    			privacyUnProtect(dataProvider,uniqueid,"duDukePager");
    			privacyUnProtect(dataProvider,uniqueid,"duTelephone1");
    			privacyUnProtect(dataProvider,uniqueid,"ou");
    			privacyUnProtect(dataProvider,uniqueid,"displayName");
    			privacyUnProtect(dataProvider,uniqueid,"sn");
    			privacyUnProtect(dataProvider,uniqueid,"mail");
    			privacyUnProtect(dataProvider,uniqueid,"givenName");
    		} else if (newValue.equals("N")) {
    			// We are protecting these attributes, so protect them, with the exception
    			// of sn, which gets uid cloned into it
    			privacyProtect(dataProvider,uniqueid,"duDukePager");
    			privacyProtect(dataProvider,uniqueid,"duTelephone1");
    			privacyProtect(dataProvider,uniqueid,"ou");
    			privacyProtect(dataProvider,uniqueid,"displayName");
    			privacyProtect(dataProvider,uniqueid,"mail");
    			privacyProtect(dataProvider,uniqueid,"givenName");
    			cloneAttribute(dataProvider,uniqueid,"uid","sn");
    		}
    	}
    } else if (provisioningData.isSyncAttribute(attribute)) {
    	// Synchronize the value(s) to the target attribute
    	Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attribute));
    	if (newValue != null && ! newValue.equals("")) {
    		// There's a new value specified -- use it
    		if (attributeData.isMultiValued(attribute)) {
    			// This is a multivalued attribute, so handle accordingly
    			Iterator newValues = OIMAPIWrapper.split(newValue).iterator();
    			while (newValues.hasNext()) {
    				modAttr.add((String) newValues.next());
    			}
    		} else {
    			// Single value only
    			modAttr.add(newValue);
    		}
    	}
    	modAttrs.put(modAttr);
    	// And actually update the value(s)
    	Attributes curuserAttrs = result.getAttributes();
    	try {
    		String curuserDN = (String) curuserAttrs.get("uid").get();
    		try {
    			ldapConnectionWrapper.addOrReplaceAttributes(curuserDN, null, modAttrs);
    		} catch (Exception e) {
    			logger.info(connectorName + " Throwing exception during attribute update for uniqueID:" + uniqueid + " due to " + e.getMessage());
    			throw new RuntimeException("Failed to update attribute " + e.getMessage(),e);
    		}
    	} catch (Exception e) {
    		// This is ok
    	}
    }
    
    // We have no logic-associated attributes in the OID -- they're all flat
    // mappings.
    

    logger.info(connectorName + ": Update user: " + uniqueid + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning successfully.");

    return SUCCESS;
  }
}
