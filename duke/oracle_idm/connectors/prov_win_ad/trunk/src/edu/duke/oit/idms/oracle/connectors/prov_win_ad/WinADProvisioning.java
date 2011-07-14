package edu.duke.oit.idms.oracle.connectors.prov_win_ad;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;
import java.io.*;

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
public class WinADProvisioning extends SimpleProvisioning {

  /** name of connector for logging purposes */
  public final static String connectorName = "WINAD_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  private byte[] pwblock;
  
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
    if (result == null) {
    	logger.info(connectorName + " returned null instance searching for " + duLDAPKey + " in LDAP");
    }
    
    if (result != null) {
      Attributes modAttrs = new BasicAttributes();
      Set<String> attrsToDelete = new HashSet<String>();
      attrsToDelete.addAll(provisioningData.getSyncAttributes());
      attrsToDelete.removeAll(getSyncAttrsNotDeletedDuringDeprovisioning());
      logger.info(connectorName + " delete list has " + attrsToDelete.size() + " attributes");
      Iterator<String> attrsToDeleteIter = attrsToDelete.iterator();
      while (attrsToDeleteIter.hasNext()) {
        String attrToDelete = attrsToDeleteIter.next();
        logger.info(connectorName + " going to delete " + attrToDelete);
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
    
    // uid of the user
    String uid = "";
    
    // duADdisplayName override attribute
    String duADdisplayName = "";
    
    // duADMailboxOverride attribute
    String duADMailboxOverride = "";
    
    // eduPersonPrimaryAffiliation attribute
    String eduPersonPrimaryAffiliation = "";
    // Pulse sez I can haz value!
    String description = "none";
    
    // ou attribute
    
    String ou = "";
    
    // duPSAcadCareerDescC1 attribute
    
    String duPSAcadCareerDescC1 = "";
    
    // duEntryPrivacy, duMailPrivacy, duTelephone1Privacy, duACLBlock attributes
    String duEntryPrivacy = "";
    String duMailPrivacy = "";
    String duTelephone1Privacy = "";
    String duACLBlock = "";
    
    // Address attributes
    String duDukePhysicalAddressCity = ""; // maps to "l" in AD
    String duDukePhysicalAddressState = ""; // maps to "st" in AD
    String duDukePhysicalAddressZip = ""; // maps to "postalCode" in AD
    
    
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
    
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, allSyncAttributesOIMNames.toArray(new String[0]));
  
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
    	} else if (preattribute.equalsIgnoreCase("duACLBlock")) {
    		duACLBlock = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duMailPrivacy")) {
    		duMailPrivacy = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duTelephone1Privacy")) {
    		duTelephone1Privacy = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duADMailboxOverride")) {
    		duADMailboxOverride = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duADdisplayName")) {
    		duADdisplayName = prevalue;
    	} else if (preattribute.equalsIgnoreCase("eduPersonPrimaryAffiliation")) {
    		eduPersonPrimaryAffiliation = prevalue;
    		if (eduPersonPrimaryAffiliation == null || (eduPersonPrimaryAffiliation != null && eduPersonPrimaryAffiliation.length() < 2)) {
    			description = "none";
    		} else {
    			description = eduPersonPrimaryAffiliation;
    		}
    	} else if (preattribute.equalsIgnoreCase("duPSAcadCareerDescC1")) {
    		duPSAcadCareerDescC1 = prevalue;
    	} else if (preattribute.equalsIgnoreCase("ou")) {
    		ou = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressCity")) {
    		duDukePhysicalAddressCity = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressState")) {
    		duDukePhysicalAddressState = prevalue;
    	} else if (preattribute.equalsIgnoreCase("duDukePhysicalAddressZip")) {
    		duDukePhysicalAddressZip = prevalue;
    	}
    	
    }

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
          
          // First, check to see if the duLDAPKey exists under the peopleContainer
          result = ldapConnectionWrapper.findEntry(duLDAPKey);
          if (result == null) {
        	  // no entry in the peopleContainer that matches that LDAPKey.  Check for a collision with uid
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
            
          if (duEntryPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("byebye")) {
        	  givenName = "Unlisted";
        	  sn = "Unlisted";
          }
          if (attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn") || attribute.equalsIgnoreCase("eduPersonNickname") || attribute.equalsIgnoreCase("duDukePhysicalAddressCity") || attribute.equalsIgnoreCase("duDukePhysicalAddressState") || attribute.equalsIgnoreCase("duDukePhysicalAddressZip")) {
        	  if (duEntryPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("byebye")) {
        		  value = "Unlisted";  // unlist blocked attributes
        		  logger.info(connectorName + " Unlisting " + attribute + " for " + netid + " set to " + value);
        	  }
          } else if (attribute.equalsIgnoreCase("duTelephone1") && (duTelephone1Privacy.equalsIgnoreCase("n") || duEntryPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("full"))) {
        	  	value = "Unlisted";  // unlist blocked phone numbers
          } else if (attribute.equalsIgnoreCase("mail") && (duMailPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("full"))) {
        	  value = null; // unlisted blocked email addresses
          } else if (attribute.equalsIgnoreCase("displayName")) {
        	  if (duEntryPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("full")) {
        		  value = "Unlisted"; // unlist blocked displaynames
        	  } else if (duADdisplayName != null && !duADdisplayName.equals("")) {
        		  value = duADdisplayName;  // override if duADDisplayName is set
        	  }
          } else if (attribute.equalsIgnoreCase("mail")) {
        	  if (duADMailboxOverride != null && !duADMailboxOverride.equals("")) {
        		  value = null;  // if it's overridden, never change it
        	  } else if (duEntryPrivacy.equalsIgnoreCase("n") || duACLBlock.equalsIgnoreCase("SAP") || duACLBlock.equalsIgnoreCase("full")) {
        		  value = null;  // unlist blocked values
        	  } 
          } else if (attribute.equalsIgnoreCase("ou")) {
        	  // if we are a student, copy over the value of career
        	  if (eduPersonPrimaryAffiliation.equalsIgnoreCase("student") && !duPSAcadCareerDescC1.equals("")) {
        		  value = duPSAcadCareerDescC1;
        	  }
        	  if (value != null && value.length() >= 63) {
        	  // regardless, set the value to be at most 64 characters long
        	  value = value.substring(0,63);
        	  } 
        	  // Require non-null value to make Pulse happy
        	  if (value == null || (value != null && value.length() < 2)) {
        		  value = "none";  // set null-ish ous to "none"
        	  } 
          } else if (attribute.equalsIgnoreCase("duPSAcadCareerDescC1")) {
        	  // if we are not a student copy over the value of career
        	  if (!eduPersonPrimaryAffiliation.equalsIgnoreCase("student") && !ou.equals("")) {
        		  value = ou;
        	  }
        	  // regardless, set the value to be at most 64 characters long
        	  if (value != null && value.length() >= 63) {
        	  value = value.substring(0,63);
        	  }
        	  // Same drill for Pulse
        	  if (value == null || (value != null && value.length() < 2)) {
        		  value = "none"; // set null-ish ous to "none"
        	  }
          } else if (attribute.equalsIgnoreCase("eduPersonPrimaryAffiliation") || attribute.equalsIgnoreCase("duDukePhysicalAddressCity") || attribute.equalsIgnoreCase("duDukePhysicalAddressState") || attribute.equalsIgnoreCase("duDukePhysicalAddressZip")) {
        	  if (value == null || (value != null && value.length() < 2)) {
        		  value = "none";
        		  if (attribute.equalsIgnoreCase("eduPersonPrimaryAffiliation")) {
        			  description = "none";
        		  } 
        	  } else if (attribute.equalsIgnoreCase("eduPersonPrimaryAffiliation")) {
        		  description = value;
        	  }
          }
          
          // Filter off attributes we don't actually use
          if (attribute.equalsIgnoreCase("duTelephone1Privacy") || attribute.equalsIgnoreCase("duEntryPrivacy") || attribute.equalsIgnoreCase("duACLBlock") || attribute.equalsIgnoreCase("duMailPrivacy") || attribute.equalsIgnoreCase("dADdisplayName") || attribute.equalsIgnoreCase("duADMailboxOverride")) {
        	  value = null;  // skip this attribute
          }
                    
                
          if (value != null && !value.equals("")) {
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
          }
        } else if (addAttr.size() > 0) {
        		addAttrs.put(addAttr);
        }
        
        // Add description regardless
        addAttr = new BasicAttribute("description");
        addAttr.add(description);
        addAttrs.put(addAttr);
        
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
      try {
      } catch (Exception e) {
    	  logger.info(connectorName + " threw exception getting SN value " + e.getMessage());
      }
      ldapConnectionWrapper.createEntry(duLDAPKey, getCN(givenName, sn, netid), addAttrs);
      try {
    	  readPWBlock();
    	  Attribute aa = new BasicAttribute("nTSecurityDescriptor",pwblock);
    	  Attributes aaa = new BasicAttributes(true);
    	  aaa.put(aa);
          result = ldapConnectionWrapper.findEntry(duLDAPKey);
    	  ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(), aaa);
      } catch (Exception e) {
    	  logger.info (connectorName + " threw exception setting ntsecuritydescriptor: " + e.getMessage());
    	  e.printStackTrace(System.out);
      }
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
      logger.info(connectorName + "Entry with duLDAPKey=" + duLDAPKey + " not foundin target -- returning success.");
      return SUCCESS;
    }
    
    Attributes modAttrs = new BasicAttributes();
    
    Attribute modAttr = new BasicAttribute(provisioningData.getTargetMapping(attribute));
    if (attribute.equalsIgnoreCase("duNetIDStatus")) {
    	// We special case duNetIDStatus and handle it locally, here.
    	// When duNetIDStatus changes, we check the new value and if it's an activation, we ensure the account
    	// is activated, otherwise we deactivate the account.
    	int curval = 0;
    	try {
    		curval = Integer.parseInt((String) result.getAttributes().get("userAccountControl").get());
    	} catch (Exception e) {
    		logger.info(connectorName + ": Error -- suppressing and ignoring exception " + e.getMessage() + " thrown while retrieving userAccountControl for user with id " + duLDAPKey);
    	}
    	if (newValue.equalsIgnoreCase("inactive") || newValue.equalsIgnoreCase("locked") || newValue.equalsIgnoreCase("timebombed")) {
    		//transition to inactive state -- we need to disable the account on the AD end.
    		
    		if ((curval & 0x02) == 0x02) {
    			// we are already inactive
    			logger.info(connectorName + ": Inactivation of NetID ignored -- AD user with id " + duLDAPKey + "already deactivated");
    			return SUCCESS;
    		} else {
    			// And in the deactivate flag
    			curval += 2;
    			modAttrs.put(new BasicAttribute("userAccountControl",Integer.toString(curval)));
    			ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(),modAttrs);
    			logger.info(connectorName + ": Inactivated AD user with id " + duLDAPKey + " due to netid inactivation");
    			return SUCCESS;
    		}
    	} else {
    		// transition to active state -- we need to enable if the account is disabled
    		if ((curval & 0x02) == 0x02) {
    			// Reactivate the AD account -- it's inactive now
    			curval -= 2;  // x-2 == x & ~(0x02)
    			modAttrs.put(new BasicAttribute("userAccountControl",Integer.toString(curval)));
    			ldapConnectionWrapper.replaceAttributes(result.getNameInNamespace(),modAttrs);
    			logger.info(connectorName + ": Reactivated AD account for user with id " + duLDAPKey + " due to netid activation");
    			return SUCCESS;
    		} else {
    			logger.info(connectorName + ": Ignoring reactivation of user with id " + duLDAPKey + " -- user is already activated");
    			return SUCCESS;
    		}
    	}
    } else if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
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
    } else if (attribute.equalsIgnoreCase("ou") || attribute.equalsIgnoreCase("duPSAcadCareerDescC1")) {
    	// we have special rules for dealing with these two
    	String[] testattrs = {attributeData.getOIMAttributeName("ou"),attributeData.getOIMAttributeName("duPSAcadCareerDescC1"),attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation")}; 
    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, testattrs);
    	if (moResultSet == null) {
    		throw new RuntimeException("No OIM entry for " + duLDAPKey);
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
    	// Make Pulse happy
    	if (newValue == null || (newValue != null && newValue.length() < 2)) {
    		newValue = "none";
    	}
    	logger.info(connectorName + " New value for ou is set to " + newValue);
    	modAttr = new BasicAttribute("ou");
    	modAttr.add(newValue);
    } else if (attribute.equalsIgnoreCase("displayname") || attribute.equalsIgnoreCase("duADdisplayName")) {
    
    	// one of the displayname attributes is changing -- we need to retrieve them both and do the 
    	// comparison
    	String[] testattrs = {attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("duADdisplayName"),attributeData.getOIMAttributeName("duEntryPrivacy"),attributeData.getOIMAttributeName("duACLBlock")};
    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, testattrs);
    	if (moResultSet == null) {
    		throw new RuntimeException("No OIM entry for " + duLDAPKey);
    	}
    	try {
    	if (moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME") != null && !moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME").equals("")) {
    		if (!moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy")).equalsIgnoreCase("n") && !moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("SAP") && ! moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("byebye")) {
    		newValue = moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME");
    		} else {
    			newValue = "Unlisted";
    		}
    	} else {
    		if (!moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy")).equalsIgnoreCase("n") &&  !moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("SAP") && ! moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("byebye")) {
    		newValue = moResultSet.getStringValue("USR_UDF_DISPLAYNAME");
    		} else {
    			newValue = "Unlisted";
    		}
    	}
    	} catch (Exception e) {
    		throw new RuntimeException (connectorName + " threw exception while retrieving display name attributes from OIM " + e.getMessage(),e);
    	}
    	modAttr = new BasicAttribute("displayName");
    	modAttr.add(newValue);
    }	else if (attribute.equalsIgnoreCase("duEntryPrivacy") || attribute.equalsIgnoreCase("duACLBlock")) {
    		// The duEntryPrivacy attribute blocks all other attributes
    		if (newValue != null && ! newValue.equals("") && ! newValue.equals("alumni")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("givenName","Unlisted"));
    			modAttrs.put(new BasicAttribute("sn","Unlisted"));
    			modAttrs.put(new BasicAttribute("telephonenumber","Unlisted"));
    			modAttrs.put(new BasicAttribute("eduPersonNickname","Unlisted"));
    			modAttrs.put(new BasicAttribute("displayName","Unlisted"));
    			modAttrs.put(new BasicAttribute("l","Unlisted"));
    			modAttrs.put(new BasicAttribute("st","Unlisted"));
    			modAttrs.put(new BasicAttribute("postalCode","Unlisted"));
    			newValue = null;  // to suppress passthru
    			attribute = "givenName";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("sn"),attributeData.getOIMAttributeName("telephonenumber"),attributeData.getOIMAttributeName("mail"),attributeData.getOIMAttributeName("eduPersonNickname"),attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("duADdisplayName"),attributeData.getOIMAttributeName("duDukePhysicalAddressCity"),attributeData.getOIMAttributeName("duDukePhysicalAddressState"),attributeData.getOIMAttributeName("duDukePhysicalAddressZip")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duLDAPKey);
    	    	}
    	      	try {
    	      	modAttrs.put(new BasicAttribute("givenName",moResultSet.getStringValue(attributeData.getOIMAttributeName("givenName"))));
    	      	modAttrs.put(new BasicAttribute("sn",moResultSet.getStringValue(attributeData.getOIMAttributeName("sn"))));
    	      	modAttrs.put(new BasicAttribute("eduPersonNickname",moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonNickname"))));
    	      	if (moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME") != null && !moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME").equals("")) {
    	      		modAttrs.put(new BasicAttribute("displayName",moResultSet.getStringValue("USR_UDF_ADDISPLAYNAME")));
    	      	} else {
    	      		modAttrs.put(new BasicAttribute("displayName",moResultSet.getStringValue(attributeData.getOIMAttributeName("displayName"))));
    	      	}
    	      	String City = "none";
    	      	String State = "none";
    	      	String Zip = "none";
    	      	try {
    	      	City = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressCity"));
    	      	} catch (Exception e) {
    	      		
    	      	}
    	      	if (City == null || (City != null && City.length() < 2)) {
    	      		City = "none";
    	      	}
    	      	try {
    	      	State = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressState"));
    	      	} catch (Exception e) {
    	      		
    	      	}
    	      	if (State == null || (State != null && State.length() < 2)) {
    	      		State = "none";
    	      	} 
    	      	try {
    	      	Zip = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukePhysicalAddressZip"));
    	      	} catch (Exception e) {
    	      		
    	      	}
    	      	if (Zip == null || (Zip != null && Zip.length() < 2)) {
    	      		Zip = "none";
    	      	}
    	      	modAttrs.put(new BasicAttribute("l",City));
    	      	modAttrs.put(new BasicAttribute("st",State));
    	      	modAttrs.put(new BasicAttribute("postalCode",Zip));
    	    
    	      	// set attribute to givenname in order to recompute attributes
    	      	attribute = "givenName";
    	      	newValue = null;
    	      	} catch (Exception e) {
    	      		throw new RuntimeException(connectorName + " threw exception retrieving attrs from OIM " + e.getMessage());
    	      		
    	      	}
    		}
    	} else if (attribute.equalsIgnoreCase("duMailPrivacy")) {
    		// The duEntryPrivacy attribute blocks mail  attributes
    		if (newValue != null && ! newValue.equals("")) {
    			// unset the passthru but reset the other attributes and leave newValue as null
    			modAttrs.put(new BasicAttribute("mail"));
    			newValue = null;  // to suppress passthru
    			attribute="duTelephone1";
    		} else {
    			// reset the other attributes, which means retrieving them first
    			String[] testattrs = {attributeData.getOIMAttributeName("givenName"),attributeData.getOIMAttributeName("sn"),attributeData.getOIMAttributeName("telephonenumber"),attributeData.getOIMAttributeName("mail"),attributeData.getOIMAttributeName("eduPersonNickname"),attributeData.getOIMAttributeName("displayName"),attributeData.getOIMAttributeName("duADdisplayName")};
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duLDAPKey);
    	    	}
    	      	try {
    	      	modAttrs.put(new BasicAttribute("mail",moResultSet.getStringValue(attributeData.getOIMAttributeName("mail"))));
    	      	// set attribute to givenname in order to recompute attributes
    	      	newValue = null;
    	      	attribute="duTelephone1";
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
    	    	tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, testattrs);
    	      	if (moResultSet == null) {
    	    		throw new RuntimeException("No OIM entry for " + duLDAPKey);
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
    	} else if (attribute.equalsIgnoreCase("uid")) {
    		// special casing for uid value changes
    		// if uid is removed (set to null or empty) we modify the change to instead be
    		// an appending of "-deleted" to the uid value
    		if (newValue == null || newValue.equals("")) {
    			// special case emptying netid
    			String[] testattrs = {attributeData.getOIMAttributeName("uid"),attributeData.getOIMAttributeName("givenName")};
    			tcResultSet moResultSet = getOIMAttributesForUser(dataProvider,duLDAPKey,testattrs);
    			if (moResultSet == null) {
    				throw new RuntimeException("No OIM entry for " + duLDAPKey);
    			}
    			try {
    				logger.info(connectorName + " About to get samaccountname value out of ldap");
    				String olduid = (String) result.getAttributes().get("sAMAccountName").get();
    				logger.info(connectorName + "Old UID is " + olduid);
    				modAttrs.put(new BasicAttribute("uid",olduid + "-deleted"));
    				newValue = olduid + "-deleted";
    			} catch (Exception e) {
    				logger.info(connectorName + "Exception in preparing uid change " + e.getMessage());
    				throw new RuntimeException(connectorName + "Exception in preparing uid change " + e.getMessage(),e);
    			}
    		} else {
    			// In this case, we don't do anything to the current user's data (it passes thru 
    			// normally) but we need to verify that there isn't another user with the same 
    			// uid as the target, and if there is, we need to make that user change first).
    		    SearchResult otherres = ldapConnectionWrapper.findEntryFromPeopleByUid(newValue);
    		    String[] validateattrs = {"USR_UDF_LDAPKEY"};
    		    tcResultSet moResultSet2 = getOIMAttributesForUser(dataProvider,duLDAPKey,validateattrs);
    		    String orid = null;
    		    try {
    		    	orid = (String) moResultSet2.getStringValue("USR_UDF_LDAPKEY");
    		    } catch (Exception e) {
    		    	throw new RuntimeException(connectorName + " Unable ot get UserID for " + newValue + " from OIM while changing uid",e);
    		    }
    		    String lid = null;
    		    try {
    		    	if (otherres != null && otherres.getAttributes() != null && otherres.getAttributes().get("duLDAPKey") != null) {
    		    	lid = (String) otherres.getAttributes().get("duLDAPKey").get();
    		    	}
    		    } catch (Exception e) {
    		    	throw new RuntimeException(connectorName + " Unable to get dudukeid attribute for " + newValue + " while changing uid",e);		    	
    		    }
    		    if (otherres != null && ! orid.equalsIgnoreCase(lid)) {
    		    	// we have a colliding user -- make that one "-consolidated" first
    		    	try{
    		    	Attributes othermodAttrs = new BasicAttributes(true);
    		    	othermodAttrs.put(new BasicAttribute("sAMAccountName",(String)otherres.getAttributes().get("sAMAccountName").get() + "-consolidated"));
    		    	othermodAttrs.put(new BasicAttribute("userPrincipalName",(String)otherres.getAttributes().get("sAMAccountName").get() + "-consolidated@WIN.DUKE.EDU"));
    		    	othermodAttrs.put(new BasicAttribute("altSecurityIdentities","Kerberos:" + (String) otherres.getAttributes().get("sAMAccountName").get() + "-consolidated@ACPUB.DUKE.EDU"));
    		    	ldapConnectionWrapper.replaceAttributes(otherres.getNameInNamespace(),othermodAttrs);
    		    	String oldcn = (String) otherres.getAttributes().get("givenName").get() + " " + (String) otherres.getAttributes().get("sn").get() + " (" + (String) otherres.getAttributes().get("sAMAccountName").get() + ")";
    		    	String newcn = (String) otherres.getAttributes().get("givenName").get() + " " + (String) otherres.getAttributes().get("sn").get() + " (" + (String) otherres.getAttributes().get("sAMAccountName").get() + "-consolidated)";
    		    	ldapConnectionWrapper.renameEntry((String) otherres.getAttributes().get("duLDAPKey").get(),oldcn,newcn);
    		    	// And let the rest happen as it will, we're done with the other user now.
    		    	} catch (Exception e) {
    		    		throw new RuntimeException(connectorName + " Unable to rename conslidated user with netid " + newValue + " : " + e.getMessage(),e);
    		    	}
    		    } else {
    		    	logger.info(connectorName + "Imdepotent uid change - ignoring " + newValue);
    		    }

    		}
  
    	}	else if (attribute.contains("duDukePhysicalAddress") || (newValue != null && !newValue.equals(""))) {
   
    
      if (attributeData.isMultiValued(attribute) && ! attribute.equalsIgnoreCase("title") && ! attribute.equalsIgnoreCase("ou") && ! attribute.equalsIgnoreCase("telephonenumber")) {
        Iterator<?> values = OIMAPIWrapper.split(newValue).iterator();
        while (values.hasNext()) {
          modAttr.add(values.next());
        }
        logger.info(connectorName + "Mutlivalue added " + attribute + " to list with possible multiple values");
      } else {
    	  if (attribute.equalsIgnoreCase("duDukePhysicalAddressCity") || attribute.equalsIgnoreCase("duDukePhysicalAddressState") || attribute.equalsIgnoreCase("duDukePhysicalAddressZip")) {
    		  logger.info(connectorName + " Working on duDukePhysical");
    		  if (newValue == null || (newValue != null && newValue.length() < 2)) {
    			  newValue = "none";
    		  }
    	  }
        modAttr.add(newValue);
        logger.info(connectorName + " Added " + attribute + " to list with value " + newValue);
      }
    }
    if (! attribute.equalsIgnoreCase("FunctionalGroup") && ! attribute.equalsIgnoreCase("hasFunctionalGroup")) {
    	modAttrs.put(modAttr);  // if modAttr is null this is a change to a non-proliferating attribute
    }
 
    
    // if attribute is uid and a new value is available, set uid specific attributes
    if (attribute.equalsIgnoreCase("uid") && newValue != null && !newValue.equals("")) {
      modAttrs = addAttributesForNewNetID(duLDAPKey, newValue, modAttrs);
    }
    
    // if attribute is eduPersonPrimaryAffilation, reflect it into description
    if (attribute.equalsIgnoreCase("eduPersonPrimaryAffiliation")) {
    	BasicAttribute modAttr2 = new BasicAttribute("description");
    	if (newValue == null || (newValue != null && newValue.length() < 2)) {
    		modAttr2.add("none");
    	} else {
    		modAttr2.add(newValue);
    	}
    	modAttrs.put(modAttr2);
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
    // or if the attribute being changed is either USR_UDF_HAS_FUNCTIONALGROUP or USR_UDF_FUNCTIONALGROUP
    // we need to trigger a rename event, which we do by recalculating CN in the target.
    if (attribute.equalsIgnoreCase("uid") || attribute.equalsIgnoreCase("givenName") || attribute.equalsIgnoreCase("sn") || attribute.equalsIgnoreCase("hasFunctionalGroup") || attribute.equalsIgnoreCase("FunctionalGroup")) {

      String[] attrsToReturn = { attributeData.getOIMAttributeName("uid"), 
          attributeData.getOIMAttributeName("givenName"), 
          attributeData.getOIMAttributeName("sn"),
          attributeData.getOIMAttributeName("duEntryPrivacy"),
          attributeData.getOIMAttributeName("duACLBlock") };
      
      tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duLDAPKey, attrsToReturn);
      
      String cn = null;
      String oldcn = null;
      try {
        String uid = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
        String givenName = moResultSet.getStringValue(attributeData.getOIMAttributeName("givenName"));
        String sn = moResultSet.getStringValue(attributeData.getOIMAttributeName("sn"));
        if (moResultSet.getStringValue(attributeData.getOIMAttributeName("duEntryPrivacy")).equalsIgnoreCase("n") || moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("SAP") || moResultSet.getStringValue(attributeData.getOIMAttributeName("duACLBlock")).equalsIgnoreCase("byebye")) {
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
        if (attribute.equalsIgnoreCase("FunctionalGroup") || attribute.equalsIgnoreCase("hasFunctionalGroup")) {
        	ldapConnectionWrapper.renameEntryForOU(duLDAPKey, oldcn, cn);
        } else {
        	ldapConnectionWrapper.renameEntry(duLDAPKey, oldcn, cn);
        }
        logger.info(connectorName + " Renamed " + givenName + " , " + sn + " , " + uid + " to " + cn);
      } catch (Exception e) {
        throw new RuntimeException("Failed to calculate CN for duLDAPKey=" + duLDAPKey, e);
      }

      
      
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
    addAttrUAC.add("544");
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
    
	  
	  // We actually add the real NetID now
	  Attribute addAttrSAN = new BasicAttribute("sAMAccountName");
	  addAttrSAN.add(netid);
	  addAttrs.put(addAttrSAN);
    
    // userPrincipalName
    Attribute addAttrUPN = new BasicAttribute("userPrincipalName");
    addAttrUPN.add(netid + "@win.duke.edu");
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
    attrs.add("duaddisplayname");
    attrs.add("duadmailboxoverride");
    attrs.add("duentryprivacy");
    attrs.add("dumailprivacy");
    attrs.add("dutelephone1privacy");
    attrs.add("duaclblock");
    attrs.add("dupsacadcareerdescc1");
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

private void readPWBlock() {
	try {
	File file = new File("/opt/idms/oracle_idm/common/conf/provisioning_connectors/WINAD_PROVISIONING/pwblock");
	FileInputStream is = new FileInputStream(file);
	long length = file.length();
	byte[] bytes = new byte[(int) length];
	int offset = 0;
	int numRead = 0;
	while (offset < bytes.length && (numRead=is.read(bytes, offset, (int) bytes.length-offset)) >= 0) {
		offset += numRead;
	}
	is.close();
	pwblock = bytes;
	} catch (Exception e) {
		throw new RuntimeException(connectorName + " Failed to open pw block byte file for reading " + e.getMessage());
	}
}

}
