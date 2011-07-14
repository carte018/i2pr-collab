package edu.duke.oit.idms.oracle.connectors.prov_dhe_ad;

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
import javax.naming.NamingEnumeration;

/**
 * @author rob
 *
 */
public class DHEADProvisioning extends SimpleProvisioning {

  /** name of connector for logging purposes */
  public final static String connectorName = "DHEAD_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  @SuppressWarnings("unchecked")
  public String deprovisionUser(tcDataProvider dataProvider, String duDukeID, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + duDukeID);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);

    // here we should delete a bunch of attributes from the target
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
    
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duDukeID);
    if (result == null) {
    	logger.info(connectorName + " returned null instance searching for " + duDukeID + " in LDAP");
    	
    }
    tcResultSet mor = null;
    if (result != null) {
      Attributes modAttrs = new BasicAttributes();
      Set<String> attrsToDelete = new HashSet<String>();
      Set<String> attrsToSet = provisioningData.getSyncAttributes();
      attrsToSet.remove("edupersonaffiliation");  // edupersonaffiliaton is downright strange
      logger.info(connectorName + ": Starting to get oim attr values");
      String [] alist = (String []) attrsToSet.toArray(new String[attrsToSet.size()]);
      for (int i = 0; i < alist.length; i++) {
    	  alist[i] = attributeData.getOIMAttributeName(alist[i]);
    	  logger.info(connectorName + "List includes " + alist[i]);
      }
      mor = getOIMAttributesForUser(dataProvider,duDukeID, alist);
      logger.info(connectorName + ": Got oim attr values");
      attrsToDelete.addAll(attrsToSet);
      attrsToDelete.removeAll(getSyncAttrsNotDeletedDuringDeprovisioning());
  
      logger.info(connectorName + " delete list has " + attrsToDelete.size() + " attributes");
      Iterator<String> attrsToDeleteIter = attrsToDelete.iterator();
      while (attrsToDeleteIter.hasNext()) {
        String attrToDelete = attrsToDeleteIter.next();
        logger.info(connectorName + " going to delete " + attrToDelete);
        Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attrToDelete));
        // Conversions
        String value;
        try {
        	value = mor.getStringValue(attributeData.getOIMAttributeName(attrToDelete));
        	if (attrToDelete.equalsIgnoreCase("duDukePhysicalAddressLine1") || attrToDelete.equalsIgnoreCase("duDukePhysicalAddressLine2")) {
        		value = mor.getStringValue("USR_UDF_DUKEPHYSICALADRLINE1") + "$" + mor.getStringValue("USR_UDF_DUKEPHYSICALADRLINE2");
        	} 
        } catch (Exception e) {
        	throw new RuntimeException("Failed retrieving value during deleting for " + attrToDelete + ":" + e.getMessage(),e);
        }
        if (value != null && ! value.equals("")) {
        	modAttr.add(null);
        	modAttrs.put(modAttr);
        }
      }
      
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);   
      
      // Now, change the uid value
      // New attrs to modify
      try {
      Attributes idchgAttrs = new BasicAttributes();
      
      logger.info(connectorName + ": Starting sAMAccountName");
      Attribute ida = new BasicAttribute("sAMAccountName");
      ida.add(mor.getStringValue("USR_UDF_UID") + "-deleted");
      idchgAttrs.put(ida);
      String computed = getCN(mor.getStringValue("Users.First Name"),mor.getStringValue("Users.Last Name"),mor.getStringValue("USR_UDF_UID"));
      logger.info(connectorName + ": Starting userPrincipalName");
      ida = new BasicAttribute("userPrincipalName");
      ida.add(mor.getStringValue("USR_UDF_UID") + "-deleted@dhe.duke.edu");
      idchgAttrs.put(ida);
      logger.info(connectorName + ": Starting altSecurityIdentities");
      ida = new BasicAttribute("altSecurityIdentities");
      ida.add("Kerberos:" + mor.getStringValue("USR_UDF_UID") + "-deleted@ACPUB.DUKE.EDU");
      idchgAttrs.put(ida);
      // And disable the account
      logger.info(connectorName + ": setting userAccountControl to disable account");
      ida = new BasicAttribute("userAccountControl");
      ida.add("546");
      idchgAttrs.put(ida);
      logger.info(connectorName + ": Done resetting attribute values");
      
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), idchgAttrs);   
      ldapConnectionWrapper.renameEntry(duDukeID, computed,computed.replaceFirst("\\("+mor.getStringValue("USR_UDF_UID")+"\\)","("+mor.getStringValue("USR_UDF_UID")+"-deleted)"));
      } catch (Exception e) {
    	  throw new RuntimeException("Failed marking uid deleted " + e.getMessage(),e);
      }
    } else {
      logger.info(connectorName + ": Deprovision user: " + duDukeID + ".  Target entry does not exist.");
    }
    
    logger.info(connectorName + ": Deprovision user: " + duDukeID + ".  Returning success.");
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String duDukeID, String unused1) {
    logger.info(connectorName + ": Provision user: " + duDukeID);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // here we should get all attributes from OIM and sync them with LDAP.
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    // Attributes of the user about which we care:
    
    String eduPersonAffiliation = "";
    String eduPersonPrimaryAffiliation = "";
    String givenName = "";
    String sn = "";
    String duMiddleName1 = "";
    String uid = "";
    String netid = "";
    String title = "";
    String duTelephone1 = "";
    String duSAPOrgUnit = "";
    String duSAPCompany = "";
    String duDukePhysicalAddressLine1 = "";
    String duDukePhysicalAddressLine2 = "";
    String duDukePhysicalAddressCity = "";
    String duDukePhysicalAddressState = "";
    String duDukePhysicalAddressCountry = "";
    String duDukePhysicalAddressZip = "";
    String displayName = "";
    // String duDukeID is passed in already
    String ou = "";
    String pager = "";
    String eduPersonNickname = "";
    String duDukePhysicalAddressPrivacy = "";
    String duEntryPrivacy = "";
    String duTelephone1Privacy = "";
    String duDukePagerPrivacy = "";
    String duACLBlock = "";
    String duSAPPersonnelSubArea = "";
    
    // Search result holder
    SearchResult result = null;
    
    // see if the entry already exists in the target
    // RGC -- not needed here (yet) //SearchResult result = ldapConnectionWrapper.findEntry(duLDAPKey);

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
    // Retrieve the personnel subarea anyway
    allSyncAttributesOIMNames.add(attributeData.getOIMAttributeName("duSAPPersonnelSubArea"));
    
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, allSyncAttributesOIMNames.toArray(new String[0]));
  
    // Collect attributes for the user that are being sync'ed to the target
    Attributes addAttrs = new BasicAttributes();
    
    // Preset values for privacy and override attributes 
    Iterator<?> preiter = provisioningData.getSyncAttributes().iterator();
    while (preiter.hasNext()) {
    	String preattribute = (String) preiter.next();
    	String pretargetAttribute = provisioningData.getTargetMapping(preattribute);
    	String prevalue = "";
    	
    	if (pretargetAttribute == null || pretargetAttribute.equals("")) {
    		throw new RuntimeException(preattribute + " does not have a target mapping.");
    	}
    	try{
    	prevalue = moResultSet.getStringValue(attributeData.getOIMAttributeName(preattribute));
    	} catch (Exception e) {
    		// ignore this exception
    	}
    	if (preattribute.equalsIgnoreCase("eduPersonAffiliation")) {
    		continue;
    	}
    	if (preattribute.equalsIgnoreCase("duEntryPrivacy")) {
    		duEntryPrivacy = prevalue;
    	}  else if (preattribute.equalsIgnoreCase("duTelephone1Privacy")) {
    		duTelephone1Privacy = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePagerPrivacy")) {
    		duDukePagerPrivacy = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressPrivacy")) {
    		duDukePhysicalAddressPrivacy = prevalue;
    	} else if (preattribute.equalsIgnoreCase("eduPersonPrimaryAffiliation")) {
    		eduPersonPrimaryAffiliation = prevalue;
    	} else if (preattribute.equalsIgnoreCase("ou")) {
    		ou = truncate(prevalue);
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressLine1")) {
    		duDukePhysicalAddressLine1 = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressLine2")) {
    		duDukePhysicalAddressLine2 = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressCity")) {
    		duDukePhysicalAddressCity = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressState")) {
    		duDukePhysicalAddressState = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressCountry")) {
    		duDukePhysicalAddressCountry = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressZip")) {
    		duDukePhysicalAddressZip = prevalue;
    	} else if (preattribute.equalsIgnoreCase("pager")) {
    		pager = prevalue;
    	} else if (preattribute.equalsIgnoreCase("displayName")) {
    		displayName = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duSAPCompany")) {
    		duSAPCompany = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duSAPOrgUnit")) {
    		duSAPOrgUnit = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duTelephone1")) {
    		duTelephone1 = prevalue;
    	} else if (preattribute.equalsIgnoreCase("title")) {
    		title = truncate(prevalue);
    	} else if (preattribute.equalsIgnoreCase("uid")) {
    		uid = prevalue;
    	} else if (preattribute.equalsIgnoreCase("givenName")) {
    		givenName = prevalue;
    	} else if (preattribute.equalsIgnoreCase("sn")) {
    		sn = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duMiddleName1")) {
    		duMiddleName1 = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duACLBlock")) {
    		duACLBlock = prevalue;
    	} else if (preattribute.equalsIgnoreCase("eduPersonNickname")) {
    		eduPersonNickname = prevalue;
    	} else if (preattribute.equalsIgnoreCase("dusappersonnelsubarea")) {
    		duSAPPersonnelSubArea = prevalue;
    	}
    }

    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String) iter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String value = null;
            
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      Attribute addAttr = new BasicAttribute(targetAttribute);
      value = null;
      
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
          logger.info(connectorName + ": Original Value Set for " + attribute + " is " + value);

          // saving a few variables because they are needed later.
          if (attribute.equalsIgnoreCase("uid")) {
            netid = new String(value);
            logger.info(connectorName + ": Provision user: " + duDukeID + ".  User has NetID: " + netid);
          } else if (attribute.equalsIgnoreCase("givenName") && value != null) {
            givenName = new String(value);
          } else if (attribute.equalsIgnoreCase("sn") && value != null) {
            sn = new String(value);
          } 
          
          // First, check to see if the user exists under the peopleContainer
          result = ldapConnectionWrapper.findEntry(duDukeID);
          if (result == null) {
        	  // no entry in the peopleContainer that matches that user.  Check for a collision with uid
        	  result = ldapConnectionWrapper.findEntryFromPeopleByUid(netid);
        	  if (result == null) {
        		  // no entry in peopleContainer with that uid value -- check everywhere else
        		  result = ldapConnectionWrapper.findEntryFromRootByUid(netid);
        		  if (result != null) {
        			  // There's a conflict with an existing uid -- log and return SUCCESS
        			  logger.warn(connectorName + " Encountered existing user outside the people container with uid value of " + uid + " Skipping provisioning -- you may need to review this user");
        			  return SUCCESS;
        		  } else {
        			  // no conflict, so just run through
        		  }
           	  } else {
        		  // There is an entry in the peopleContainer that collides with the uid value
           		  // We treat it as a re-creation of a deleted user and just fall thru
        	  }
          	} else {
          		// There's already a user with that ldapkey, which means we've already done what we need
          		// to.
          		logger.info(connectorName + " User already exists in people container (and is not deleted)");
          		return SUCCESS;
          	}
          
          // Apply some override logic as necessary...
          // First, the privacy protected attributes get rewritten if they're protected by ACLBlock or Entry
          // Then privacy protections for specific attributes
          // Then overrides for email attributes and displayname attribute
            
          if (duEntryPrivacy.equalsIgnoreCase("n") || (duACLBlock.equalsIgnoreCase("SAP") && ! duSAPPersonnelSubArea.equalsIgnoreCase("0015")) || duACLBlock.equalsIgnoreCase("alumni") || duACLBlock.equalsIgnoreCase("byebye")) {
        	  givenName = "Unlisted";
        	  sn = "Unlisted";
          }
          if (attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn") || attribute.equalsIgnoreCase("eduPersonNickname")) {
        	  if (duEntryPrivacy.equalsIgnoreCase("n") || (duACLBlock.equalsIgnoreCase("SAP") && ! duSAPPersonnelSubArea.equalsIgnoreCase("0015")) || duACLBlock.equalsIgnoreCase("Alumni") || duACLBlock.equalsIgnoreCase("byebye")) {
        		  value = "Unlisted";  // unlist blocked attributes
        		  logger.info(connectorName + " Unlisting " + attribute + " (givenname, sn or nickname) for " + netid + " set to " + value);
        	  } 
          } else if (attribute.equalsIgnoreCase("duTelephone1") && (duTelephone1Privacy.equalsIgnoreCase("n") || duEntryPrivacy.equalsIgnoreCase("n") || (duACLBlock.equalsIgnoreCase("SAP") && ! duSAPPersonnelSubArea.equalsIgnoreCase("0015"))|| duACLBlock.equalsIgnoreCase("Alumni") || duACLBlock.equalsIgnoreCase("full"))) {
        	  	value = "Unlisted";  // unlist blocked phone numbers
        	  	logger.info(connectorName + ": Unlisting " + attribute + " (telephone1) for " + netid + " set to " + value);
          } else if (attribute.equalsIgnoreCase("pager") && (duDukePagerPrivacy.equalsIgnoreCase("n") || duEntryPrivacy.equalsIgnoreCase("n") || (duACLBlock.equalsIgnoreCase("SAP") && ! duSAPPersonnelSubArea.equalsIgnoreCase("0015")) || duACLBlock.equalsIgnoreCase("Alumni") || duACLBlock.equalsIgnoreCase("byebye") || duACLBlock.equalsIgnoreCase("full"))) {
        	    value = "Unlisted";  // unlist blocke pager numbers
        	    logger.info(connectorName + ": Unlisting " + attribute + " (pager) for " + netid + " set to " + value);
          } else if (attribute.equalsIgnoreCase("displayName")) {
        	  if (duEntryPrivacy.equalsIgnoreCase("n") || (duACLBlock.equalsIgnoreCase("SAP") && ! duSAPPersonnelSubArea.equalsIgnoreCase("0015")) || duACLBlock.equalsIgnoreCase("Alumni") || duACLBlock.equalsIgnoreCase("full")) {
        		  value = "Unlisted"; // unlist blocked displaynames
        		  logger.info(connectorName + ": Unlisting " + attribute + " (displayName) for " + netid + " set to " + value);
        	  } else {
        		  // Apply override to set displayName to Last, First
        		  // No longer requested by DHTS -- instead, simply propagate the displayname attribute directly
        		  //if (eduPersonNickname != null && ! eduPersonNickname.equals("")) {
        			//  value = sn + ", " + eduPersonNickname;
        		  //} else {
        		  //value = sn + ", " + givenName;
        		  //}
        		  // Now simply propagate displayname as it is.
        		  logger.info(connectorName + ": displayName propagating as " + value);
        	  }
        		  
          } 
          
          // Filter off attributes we don't actually use
          if (attribute.equalsIgnoreCase("duTelephone1Privacy") || attribute.equalsIgnoreCase("duEntryPrivacy") || attribute.equalsIgnoreCase("duACLBlock") || attribute.equalsIgnoreCase("duMailPrivacy") || attribute.equalsIgnoreCase("dADdisplayName") || attribute.equalsIgnoreCase("duADMailboxOverride") || attribute.equalsIgnoreCase("duDukePagerPrivacy") || attribute.equalsIgnoreCase("duMiddleName1")) {
        	  value = null;  // skip this attribute
        	  logger.info(connectorName + " Masking off value to null for unused attribute");
          }
          
          // Hack for handling combination of address line 1 and address line 2
          if (attribute.equalsIgnoreCase("duDukePhysicalAddressLine1") || attribute.equalsIgnoreCase("duDukePhysicalAddressLine2")) {
        	  	if (duDukePhysicalAddressPrivacy.equalsIgnoreCase("N")) {
        	  		value = null;
        	  		logger.info(connectorName + " Masking off address lines to null as private");
        	  	} else {
        	  		value = duDukePhysicalAddressLine1 + "$" + duDukePhysicalAddressLine2;
        	  	}
          }
                    
                
          if (value != null && !value.equals("")) {
        	  logger.info(connectorName + ": value of value for " + attribute + " is " + value);
            if (attributeData.isMultiValued(attribute) && ! attribute.equalsIgnoreCase("ou") && ! attribute.equalsIgnoreCase("title") && ! attribute.equalsIgnoreCase("telephonenumber")) {
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
            if (attribute.equals("givenName") || attribute.equals("sn") || attribute.equalsIgnoreCase("eduPersonNickname")) {
            	if (displayName != null && ! displayName.equals("")) {
            	addAttrs.put(new BasicAttribute("displayName",displayName));
            	}
            }
          }
        } else if (addAttr.size() > 0) {
        		addAttrs.put(addAttr);
        		addAttrs.put(new BasicAttribute("displayname",displayName));
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
      addAttrs = addAttributesForNewEntry(duDukeID, netid, addAttrs);
      try {
      } catch (Exception e) {
    	  logger.info(connectorName + " threw exception getting SN value " + e.getMessage());
      }
      logger.info(connectorName + " calling createentry");
      try{
      NamingEnumeration<String> addne = addAttrs.getIDs();
      while (addne.hasMoreElements()) {
    	  String aname = addne.next();
    	  logger.info(connectorName + " will use attribute " + aname + " set to " + addAttrs.get(aname).get());
      }
      } catch (Exception e) {
    	  logger.info(connectorName + " Exception printing attributes - moving on");
      }
      ldapConnectionWrapper.createEntry(duDukeID, getCN(givenName, sn, netid), addAttrs);
      
    } else {
      addAttrs = addAttributesForNewNetID(duDukeID, netid, addAttrs);
      ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), addAttrs);
      
      // we may have to update the RDN
      String cnInAD = result.getName();
      cnInAD = cnInAD.replaceFirst("^\"", "").replaceFirst("\"$", "");
      cnInAD = Pattern.compile("^cn=", Pattern.CASE_INSENSITIVE).matcher(cnInAD).replaceFirst("");
      ldapConnectionWrapper.renameEntry(duDukeID, cnInAD, getCN(givenName, sn, netid));
    }
    

    logger.info(connectorName + ": Provision user: " + duDukeID + ".  Returning success.");

    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String duDukeID, String unused1,
      String attribute, String unused2, String newValue) {
    logger.info(connectorName + ": Update user: " + duDukeID + ", attribute=" + attribute + ", newValue=" + newValue);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    
    // Flag to control update of uid value if we *really* mean to update the sAMAccountName
    boolean updateSAMAccountName = false;
    
    // here one attribute in OIM is being updated.
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    // see if the entry already exists in the target
    SearchResult result = ldapConnectionWrapper.findEntry(duDukeID);
    
    if (result == null) {
      logger.info(connectorName + "Entry with duLDAPKey=" + duDukeID + " not foundin target -- returning success.");
      return SUCCESS;
    }
    
    // Short circuit to SUCCESS for now, if the returned user doesn't contain a unique ID
    // value in employeeNumber.  This is to cope with the case in which a user has been 
    // provisioned as a pre-existing user but isn't yet under the control of the OIM
    // system -- in these cases, we need to skip updates until the DHE folks are ready.
    //
    // With this hack, we arrange it so that users we create in the AD (which get 
    // employeeNumber values automatically) will be maintained, while users that pre-exist
    // will be provisioned but ignored until someone or something sets a unique ID value
    // into their employeeNumber attribute field.  Until then, we'll record the fact that 
    // we had a change in the logs, but that we skipped it due to the user being a 
    // pre-existing case not yet incorporated into OIM control.
    //
    try {
    if (result.getAttributes().get("employeeNumber") == null || result.getAttributes().get("employeeNumber").get() == null || result.getAttributes().get("employeeNumber").get().equals("")) {
    	// This is a pre-existing user we have not yet agreed to start maintaining -- log and reutrn Success
    	logger.info(connectorName + ": Skipping update of " + attribute + " for " + duDukeID + "due to pre-existing user without employeeNumber value -- when user gets employeeNumber, we will resync manually");
    	return SUCCESS;  // succeed the change with that log report
    }
    } catch (Exception e) {
    	logger.info(connectorName + ": Exception trying to determine whether to update " + attribute + " for " + duDukeID + "under agreement to avoid pre-existing users in DHE AD -- returning Success, but you may need to review this case");
    	return SUCCESS;
    }
    
    Attributes modAttrs = new BasicAttributes();
    
    Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attribute));
    if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
      // the attribute is eduPersonAffiliation so we're going to query OIM for all affiliation values
      tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, OIMAPIWrapper.getOIMAffiliationFieldNames().toArray(new String[0]));
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
    } else if (attribute.equalsIgnoreCase("duDukePhysicalAddressLine1") || attribute.equalsIgnoreCase("duDukePhysicalAddressLine2")) {
    	try {
    		String[] testattrs = {attributeData.getOIMAttributeName("duDukePhysicalAddressLine1"),attributeData.getOIMAttributeName("duDukePhysicalAddressLine2")};
    		tcResultSet moResultSet = getOIMAttributesForUser(dataProvider,duDukeID,testattrs);
    		if (moResultSet == null) {
    			throw new RuntimeException(connectorName + "No OIM entry for " + duDukeID);
    		}
    		logger.info(connectorName + ": setting " +attribute+ " to " + newValue);
    		newValue = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressLine1")) + "$" + moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressLine2"));
    		} catch (Exception e) {
    			throw new RuntimeException(connectorName + "Throwing exception while computing streetaddress value" + e.getMessage(),e);
    		}
    		attribute = "streetAddress";
    		//Attribute synthetic = new BasicAttribute("streetAddress");
    		//synthetic.add(newValue);
    		modAttr.add(newValue);
    		//modAttrs.put(synthetic);
    } else if (attribute.equalsIgnoreCase("ou")) {
    	// we have special rules for dealing with these two
    	String[] testattrs = {attributeData.getOIMAttributeName("ou"),attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation")}; 
    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	if (moResultSet == null) {
    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	}
    	try {
    		if (moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation")).equalsIgnoreCase("student")) {
    			newValue = moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation"));
    		} else {
    			newValue = moResultSet.getStringValue(attributeData.getOIMAttributeName("ou"));
    		}
    	} catch (Exception e) {
    		throw new RuntimeException(connectorName + " exception thrown computing ou value " + e.getMessage(),e);
    	}
    } else if (attribute.equalsIgnoreCase("displayname")) {
    
    	// one of the displayname attributes is changing -- we need to retrieve them both and do the 
    	// comparison
    	String[] testattrs = {attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("duEntryPrivacy"),attributeData.getOIMAttributeName("duACLBlock"),attributeData.getOIMAttributeName("duSAPPersonnelSubArea"),"Users.First Name","Users.Last Name","USR_UDF_EDUPERSONNICKNAME"};
    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	if (moResultSet == null) {
    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	}
    	try {
    		if (moResultSet.getStringValue(attributeData.getOIMAttributeName("duSAPPersonnelSubArea")).equalsIgnoreCase("0015") || (!moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy")).equalsIgnoreCase("n") && !moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("alumni") && !moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("SAP") && ! moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("byebye"))) {
    			// No longer need to perform special case for DHTS
    			newValue = moResultSet.getStringValue("USR_UDF_DISPLAYNAME");
    		} else {
    			newValue = "Unlisted";
    		}
    	} catch (Exception e) {
    		throw new RuntimeException (connectorName + " threw exception while retrieving display name attributes from OIM " + e.getMessage(),e);
    	}
    	modAttr = new BasicAttribute("displayName");
    	modAttr.add(newValue);
    }	else if (attribute.equalsIgnoreCase("duEntryPrivacy") || attribute.equalsIgnoreCase("duACLBlock")) {
    		// The duEntryPrivacy attribute blocks all other attributes
    		if (newValue != null && ! newValue.equals("") && ! newValue.equalsIgnoreCase("SAP")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("givenName","Unlisted"));
    			modAttrs.put(new BasicAttribute("sn","Unlisted"));
    			modAttrs.put(new BasicAttribute("telephonenumber","Unlisted"));
    			modAttrs.put(new BasicAttribute("eduPersonNickname","Unlisted"));
    			modAttrs.put(new BasicAttribute("displayName","Unlisted"));
    			modAttrs.put(new BasicAttribute("pager","Unlisted"));
    			newValue = null;  // to suppress passthru
    			attribute = "givenName";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("sn"),attributeData.getOIMAttributeName("duTelephone1"),attributeData.getOIMAttributeName("eduPersonNickname"),attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("pager")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	    	}
    	      	try {
    	      	modAttrs.put(new BasicAttribute("givenName",moResultSet.getStringValue(attributeData.getOIMAttributeName("givenName"))));
    	      	modAttrs.put(new BasicAttribute("sn",moResultSet.getStringValue(attributeData.getOIMAttributeName("sn"))));
    	      	modAttrs.put(new BasicAttribute("eduPersonNickname",moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonNickname"))));
    	      	//Request to modify displayName for DHTS AD is no longer in effect, so we simply pass the value we have
    	      	modAttrs.put(new BasicAttribute("displayName",moResultSet.getStringValue(attributeData.getOIMAttributeName("displayName"))));
    	      	modAttrs.put(new BasicAttribute("pager",moResultSet.getStringValue(attributeData.getOIMAttributeName("pager"))));
    	      	modAttrs.put(new BasicAttribute("telephonenumber",moResultSet.getStringValue(attributeData.getOIMAttributeName("duTelephone1"))));
    	      	// set attribute to givenname in order to recompute attributes
    	      	attribute = "givenName";
    	      	newValue = null;
    	      	} catch (Exception e) {
    	      		throw new RuntimeException(connectorName + " threw exception retrieving attrs from OIM " + e.getMessage());
    	      		
    	      	}
    		}
    	} else if (attribute.equalsIgnoreCase("duTelephone1Privacy")) {
    		// The duEntryPrivacy attribute blocks mail  attributes
    		if (newValue != null && ! newValue.equals("")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("telephonenumber","Unlisted"));
    			newValue = null;  // to suppress passthru
    			attribute="duTelephone1";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("sn"),attributeData.getOIMAttributeName("duTelephone1"),attributeData.getOIMAttributeName("mail"),attributeData.getOIMAttributeName("eduPersonNickname"),attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("duADdisplayName")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	    	}
    	      	try {
    	      		if (! moResultSet.getStringValue(attributeData.getOIMAttributeName("duTelephone1")).equalsIgnoreCase("")) {
    	      	modAttrs.put(new BasicAttribute("telephonenumber",moResultSet.getStringValue(attributeData.getOIMAttributeName("duTelephone1"))));
    	      		}
    	      	// set attribute to givenname in order to recompute attributes
    	      	newValue = null;
    	      	attribute="duTelephone1";
    	      	} catch (Exception e) {
    	      		throw new RuntimeException(connectorName + " threw exception retrieving attrs from OIM " + e.getMessage());
    	      		
    	      	}
    		}
    	} else if (attribute.equalsIgnoreCase("duDukePagerPrivacy")) {
    		// The duEntryPrivacy attribute blocks mail  attributes
    		if (newValue != null && ! newValue.equals("")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("pager","Unlisted"));
    			newValue = null;  // to suppress passthru
    			attribute="pager";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("sn"),attributeData.getOIMAttributeName("duTelephone1"),attributeData.getOIMAttributeName("eduPersonNickname"),attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("pager")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	    	}
    	      	try {
    	      		if (! moResultSet.getStringValue(attributeData.getOIMAttributeName("pager")).equalsIgnoreCase("")) {
    	      	modAttrs.put(new BasicAttribute("pager",moResultSet.getStringValue(attributeData.getOIMAttributeName("pager"))));
    	      		}
    	      	// set attribute to givenname in order to recompute attributes
    	      	newValue = null;
    	      	attribute="pager";
    	      	} catch (Exception e) {
    	      		throw new RuntimeException(connectorName + " threw exception retrieving attrs from OIM " + e.getMessage());
    	      		
    	      	}
    		}
    	} else if (attribute.equalsIgnoreCase("duDukePhysicalAddressPrivacy")) {
    		// The duEntryPrivacy attribute blocks mail  attributes
    		if (newValue != null && ! newValue.equals("")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("streetAddress","Unlisted"));
    			modAttrs.put(new BasicAttribute("l",null));
    			modAttrs.put(new BasicAttribute("st",null));
    			modAttrs.put(new BasicAttribute("c",null));
    			modAttrs.put(new BasicAttribute("postalCode",null));
    			newValue = null;  // to suppress passthru
    			attribute="duDukePhysicalAddressLine1";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("duDukePhysicalAddressLine1"),attributeData.getOIMAttributeName("duDukePhysicalAddressLine2"),attributeData.getOIMAttributeName("duDukePhysicalAddressCity"),attributeData.getOIMAttributeName("duDukePhysicalAddressState"),attributeData.getOIMAttributeName("duDukePhysicalAddressCountry"),attributeData.getOIMAttributeName("duDukePhysicalAddressZip")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duDukeID);
    	    	}
    	      	try {
    	      	modAttrs.put(new BasicAttribute("streetAddress",moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressLine1")) + "$" + moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressLine2"))));
    	      	modAttrs.put(new BasicAttribute("l",moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressCity"))));
    	      	modAttrs.put(new BasicAttribute("st",moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressState"))));
    	      	modAttrs.put(new BasicAttribute("c",moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressCountry"))));
    	      	modAttrs.put(new BasicAttribute("postalCode",moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressZip"))));
    	      	// set attribute to givenname in order to recompute attributes
    	      	newValue = null;
    	      	attribute="duDukePhysicalAddressLine1";
    	      	} catch (Exception e) {
    	      		throw new RuntimeException(connectorName + " threw exception retrieving attrs from OIM " + e.getMessage());
    	      		
    	      	}
    		}
    	} else if (attribute.equalsIgnoreCase("uid")) {
    		// special casing for uid value changes
    		// if uid is removed (set to null or empty) we modify the change to instead be
    		// an appending of "-deleted" to the uid value
    		if (newValue == null || newValue.equals("")) {
    			// special case emptying netid
    			String[] testattrs = {attributeData.getOIMAttributeName("uid"),attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("duDempoID")};
    			tcResultSet moResultSet = getOIMAttributesForUser(dataProvider,duDukeID,testattrs);
    			if (moResultSet == null) {
    				throw new RuntimeException("No OIM entry for " + duDukeID);
    			}
    			try {
    				logger.info(connectorName + " About to get samaccountname value out of ldap");
    				String olduid = (String) result.getAttributes().get("sAMAccountName").get();
    				//Check for equality with dempo ID
    				if (olduid.equalsIgnoreCase(moResultSet.getStringValue(attributeData.getOIMAttributeName("duDempoID")))) {
    					logger.info(connectorName + "Old UID matches DempoID -- not updating");
    					newValue = olduid;
    					updateSAMAccountName = false;
    				} else {
    					logger.info(connectorName + "Old UID is " + olduid);
    					modAttrs.put(new BasicAttribute("uid",olduid + "-deleted"));
    					newValue = olduid + "-deleted";
    					// Suppress renaming of objects
    					updateSAMAccountName = false;
    				}
    			} catch (Exception e) {
    				logger.info(connectorName + "Exception in preparing uid change " + e.getMessage());
    				throw new RuntimeException(connectorName + "Exception in preparing uid change " + e.getMessage(),e);
    			}
    		} else {
    			// In this case, we don't do anything to the current user's data (it passes thru 
    			// normally) but we need to verify that there isn't another user with the same 
    			// uid as the target, and if there is, we need to make that user change first).
    			
    			// First, though, check to see if the current uid value happens to match the 
    			// value of duDempoID -- if it does, don't do anything and log the fact.
    			try {
    			String[] testattrs = {attributeData.getOIMAttributeName("uid"),attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("duDempoID")};
    			tcResultSet moResultSet = getOIMAttributesForUser(dataProvider,duDukeID,testattrs);
    			
    			if (moResultSet == null) {
    				throw new RuntimeException("No OIM entry for " + duDukeID);
    			}
    			if (moResultSet.getStringValue(attributeData.getOIMAttributeName("duDempoID")).equalsIgnoreCase((String) result.getAttributes().get("samAccountName").get())) {
    				// We have a user with a dempo ID for their SamAccountName.  
    				logger.info(connectorName + ": uid update for users with DempoID as sAMAccountName - ignoring");
    				newValue = (String) result.getAttributes().get("sAMAccountName").get();
    				updateSAMAccountName = false;
    			} else {
    		    SearchResult otherres = ldapConnectionWrapper.findEntryFromPeopleByUid(newValue);
    		    String [] testattrs2 = {"Users.User ID"};
    			tcResultSet moResultSet2 = getOIMAttributesForUser(dataProvider,duDukeID,testattrs2);

    		    
    		    if (otherres != null) {
    		    	String orid = null;
    		    	String moid = null;
    		    	try {
    		    	orid = (String) otherres.getAttributes().get("duDukeID").get();
    		    	moid = (String) moResultSet2.getStringValue("Users.User ID");
    		    	} catch (Exception e) {
    		    		// nothing to do here
    		    	}
    		    	if (orid != null && moid != null && ! orid.equalsIgnoreCase(moid)) {
    		    	// we have a colliding user -- make that one "-consolidated" first
    		    	try{
    		    	Attributes othermodAttrs = new BasicAttributes(true);
    		    	othermodAttrs.put(new BasicAttribute("sAMAccountName",(String)otherres.getAttributes().get("sAMAccountName").get() + "-consolidated"));
    		    	othermodAttrs.put(new BasicAttribute("userPrincipalName",(String)otherres.getAttributes().get("sAMAccountName").get() + "-consolidated@DHE.DUKE.EDU"));
    		    	othermodAttrs.put(new BasicAttribute("altSecurityIdentities","Kerberos:" + (String) otherres.getAttributes().get("sAMAccountName").get() + "-consolidated@ACPUB.DUKE.EDU"));
    		    	ldapConnectionWrapper.replaceAttributes(otherres.getNameInNamespace(),othermodAttrs);
    		    	String oldcn = (String) otherres.getAttributes().get("givenName").get() + " " + (String) otherres.getAttributes().get("sn").get() + " (" + (String) otherres.getAttributes().get("sAMAccountName").get() + ")";
    		    	String newcn = (String) otherres.getAttributes().get("givenName").get() + " " + (String) otherres.getAttributes().get("sn").get() + " (" + (String) otherres.getAttributes().get("sAMAccountName").get() + "-consolidated)";
    		    	ldapConnectionWrapper.renameEntry((String) otherres.getAttributes().get("duDukeID").get(),oldcn,newcn);
    		    	updateSAMAccountName = true;
    		    	// And let the rest happen as it will, we're done with the other user now.
    		    	} catch (Exception e) {
    		    		throw new RuntimeException(connectorName + " Unable to rename conslidated user with netid " + newValue + " : " + e.getMessage(),e);
    		    	}
    		    	} else {
    		    		logger.info(connectorName + "Not renaming since collision is not a consolidation but an imdepotent uid change for " + newValue);
    		    	}
    		    }

    		}
    			} catch (Exception e) {
    				logger.info(connectorName + "Exception " + e.getMessage() + " while processing new uid value");
    			}
    		}
  
    	}	else if (newValue != null && !newValue.equals("")) {
   
    
      if (attributeData.isMultiValued(attribute) && ! attribute.equalsIgnoreCase("title") && ! attribute.equalsIgnoreCase("ou") && ! attribute.equalsIgnoreCase("telephonenumber")) {
        Iterator<?> values = OIMAPIWrapper.split(newValue).iterator();
        while (values.hasNext()) {
          modAttr.add(values.next());
        }
      } else {
    	  if (attribute.equalsIgnoreCase("title") || attribute.equalsIgnoreCase("ou")) {
    		  modAttr.add(truncate(newValue));
    	  } else {
    		  modAttr.add(newValue);
    	  }
      }
    }
    
    modAttrs.put(modAttr);
    
    
    // if attribute is uid and a new value is available, set uid specific attributes
      if (attribute.equalsIgnoreCase("uid") && newValue != null && !newValue.equals("")) {
    	modAttrs = addAttributesForNewNetID(duDukeID, newValue, modAttrs);
    }
    // RGC DEBUG
    logger.info(connectorName + " performing replaceAttributes on " + result.getNameInNamespace());
    logger.info(connectorName + " attribute list has " + modAttrs.size() + " attributes");

    try{
    modAttrs.remove("duEntryPrivacy");
    
    NamingEnumeration ne = modAttrs.getAll();
    
    while (ne.hasMore()) {
    		Attribute tfoo = (Attribute) ne.next();
    		logger.info(connectorName + " setting " + tfoo.getID() + " to values (" + tfoo.size() + ") = " + tfoo.get()); 		
    		if (tfoo.get().equals("")) {
    			modAttrs.remove(tfoo.getID());
    			modAttrs.put(tfoo.getID(),null);
    		}
    }
    } catch (Exception e) {
    		logger.info(connectorName + " exception caught " + e.getMessage() );
    }
    // END DEBUG
    ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), modAttrs);
    
    // if the attribute being updated is givenName, sn, or uid, we need to recalculate CN in the target
    if (updateSAMAccountName && (attribute.equalsIgnoreCase("uid") || attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn") || attribute.equalsIgnoreCase("eduPersonNickname"))) {

      String[] attrsToReturn = { attributeData.getOIMAttributeName("uid"), 
          attributeData.getOIMAttributeName("givenName"), 
          attributeData.getOIMAttributeName("sn"),
          attributeData.getOIMAttributeName("duEntryPrivacy"),
          attributeData.getOIMAttributeName("duACLBlock"),
          attributeData.getOIMAttributeName("eduPersonNickname"),
          attributeData.getOIMAttributeName("displayName"),
          attributeData.getOIMAttributeName("duSAPPersonnelSubArea")};
      
      tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, attrsToReturn);
      
      // If the attribute happens to be givenName or sn, we need to recalculate displayName, too
      logger.info(connectorName + "Checking if we need to update displayname");
      if (attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn") || attribute.equalsIgnoreCase("eduPersonNickname")) {
    	  logger.info(connectorName + "We do need to update displayname");
    	  BasicAttributes bas = new BasicAttributes();
    	  BasicAttribute ba = new BasicAttribute("displayName");
    	  try {
    		  ba.add(moResultSet.getStringValue("USR_UDF_DISPLAYNAME"));
    	  bas.put(ba);
    	  ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), bas);
    	  logger.info(connectorName + ": Updated displayname attribute after changing sn or givenname to " + moResultSet.getStringValue("Users.Last Name") + " and " + moResultSet.getStringValue("Users.First Name"));
    	  } catch (Exception e) {
    		  throw new RuntimeException("Failed to update displayname on sn/givenname change with " + e.getMessage(),e);
    	  }
      } else {
    	  logger.info(connectorName + "We do not need to update displayname");
      }
      String cn = null;
      String oldcn = null;
      try {
        String uid = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
        String givenName = moResultSet.getStringValue(attributeData.getOIMAttributeName("givenName"));
        String sn = moResultSet.getStringValue(attributeData.getOIMAttributeName("sn"));
        if (moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy")).equalsIgnoreCase("n") || moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("alumni") || (moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("SAP") && ! moResultSet.getStringValue(attributeData.getOIMAttributeName("duSAPPersonnelSubArea")).equalsIgnoreCase("0015"))|| moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("byebye")) {
        	cn = getCN("Unlisted","Unlisted", uid);
        	if (result != null) {
        		try{
        	oldcn = getCN((String) result.getAttributes().get("givenName").get(),(String) result.getAttributes().get("sn").get(),(String) result.getAttributes().get("sAMAccountName").get());
        		} catch (Exception e) {
        			logger.info(connectorName + "failure getting CN");
        			throw new RuntimeException(e);
        		}
        	} else {
        		if (uid != null && ! uid.equals("")) {
        			oldcn = getCN(givenName,sn,uid);
        		} else {
        			oldcn = getCN(givenName,sn,(String)result.getAttributes().get("sAMAccountName").get()+"-deleted");
        		}
        	}
        } else {
        	if (result != null) {
        		if (attribute.equalsIgnoreCase("uid")) {
        		if (uid == null || uid.equals("")) {
        		cn = getCN((String) result.getAttributes().get("givenName").get(),(String) result.getAttributes().get("sn").get(),(String) result.getAttributes().get("sAMAccountName").get() + "-deleted");
        		} else {
        		cn = getCN((String) result.getAttributes().get("givenName").get(),(String) result.getAttributes().get("sn").get(),uid);
        		}
        		} else {
        			if (uid == null || uid.equals("")) {
        				cn = getCN(givenName,sn,(String)result.getAttributes().get("sAMAccountName").get());
        			} else {
        				cn = getCN(givenName,sn,uid);
        			}
        		}
        	} else {
        		cn = getCN(givenName, sn, uid);
        	}
        	if (result != null) {
        		try{
        		oldcn = getCN((String) result.getAttributes().get("givenName").get(),(String) result.getAttributes().get("sn").get(),(String) result.getAttributes().get("sAMAccountName").get());
        		} catch (Exception e) {
        			logger.info(connectorName + "failure getting CN no privacy " + e.getMessage());
        			throw new RuntimeException(e);
        		}
        	} else {
        		if (uid != null && ! uid.equals("")) {
        			oldcn = getCN("Unlisted","Unlisted",uid);
        		} else {
        			oldcn = getCN("Unlisted","Unlisted",(String) result.getAttributes().get("sAMAccountName").get() + "-deleted");
        		}
        	}
        }
        if (attribute.equalsIgnoreCase("uid")) { // now that DHE uses only the uid as their RDN, we must not change the DN when givenName or sn changes
        ldapConnectionWrapper.renameEntry(duDukeID, oldcn, cn);
        logger.info(connectorName + " Renamed " + givenName + " , " + sn + " , " + uid + " to " + cn);
        } else {
        	logger.info(connectorName + " " + uid + " Not renamed, since uid is not changing and DHE uses UID for CN as RDN");
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to calculate CN for duLDAPKey=" + duDukeID, e);
      }

      
      
    }

    logger.info(connectorName + ": Update user: " + duDukeID + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

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
  private Attributes addAttributesForNewEntry(String duDukeID, String netid, Attributes addAttrs) {

    // add object class
    Attribute addAttrOC = new BasicAttribute("objectClass");
    addAttrOC.add("top");
    addAttrOC.add("person");
    addAttrOC.add("user");
    // addAttrOC.add("inetOrgPerson");  // DHE needs to *not* have IOP in the objectclass list for some of their delegated admin stuff
    addAttrOC.add("organizationalPerson");
    addAttrs.put(addAttrOC);
    
    // userAccountControl
    Attribute addAttrUAC = new BasicAttribute("userAccountControl");
    addAttrUAC.add("512");
    addAttrs.put(addAttrUAC);
    
    // unicodePwd
    Attribute addAttrPASS = new BasicAttribute("unicodePwd");
    try {
      String password = generatePassword();
      addAttrPASS.add(new String("\"" + password + "\"").getBytes("UnicodeLittleUnmarked"));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to generate password while creating new entry with duLDAPKey=" + duDukeID + ": " + e.getMessage(), e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Failed to encode password while creating new entry with duLDAPKey=" + duDukeID + ": " + e.getMessage(), e);
    }
    addAttrs.put(addAttrPASS);
    
    
    
    return addAttributesForNewNetID(duDukeID, netid, addAttrs);
  }
  
  /**
   * These attributes are added when there's a new NetID
   * @param duLDAPKey
   * @param addAttrs
   * @return Attributes
   */
  private Attributes addAttributesForNewNetID(String duDukeID, String netid, Attributes addAttrs) {
    
	  
	  // We actually add the real NetID now
	  Attribute addAttrSAN = new BasicAttribute("sAMAccountName");
	  addAttrSAN.add(netid);
	  addAttrs.put(addAttrSAN);
    
    // userPrincipalName
    Attribute addAttrUPN = new BasicAttribute("userPrincipalName");
    addAttrUPN.add(netid + "@dhe.duke.edu");
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
    attrs.add("uid");
    attrs.add("duentryprivacy");
    attrs.add("dumailprivacy");
    attrs.add("dutelephone1privacy");
    attrs.add("duaclblock");
    attrs.add("dudukephysicaladdressprivacy");
    attrs.add("dumiddlename1");
    attrs.add("edupersonaffiliation");
 
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
    
    //String cn = sn + " " + givenName + " (" + uid + ")";
    // Replaced by request of DHTS with just the NetID value
    String cn = uid;
    return cn.trim();
  }
  
  /**
   * @param dataProvider
   * @param duLDAPKey
   * @param attrs
   * @return tcResultSet
   */
  private tcResultSet getOIMAttributesForUser(tcDataProvider dataProvider, String duDukeID, String[] attrs) {
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> mhSearchCriteria = new Hashtable<String ,String>();
      mhSearchCriteria.put("Users.User ID", duDukeID);
      mhSearchCriteria.put("Users.Status","Active");
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duDukeID " + duDukeID);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return moResultSet;
  }
  private String truncate(String foo) {
	  if (foo.length() > 63) {
		  return foo.trim().substring(0,63);
	  } else {
		  return foo;
	  }
  }
}

