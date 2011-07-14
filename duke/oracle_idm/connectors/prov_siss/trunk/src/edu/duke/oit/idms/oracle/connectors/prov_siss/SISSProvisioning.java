package edu.duke.oit.idms.oracle.connectors.prov_siss;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import PeopleSoft.Generated.CompIntfc.IDuIdmsStageCi;
import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;


/**
 * @author shilen
 *
 */
public class SISSProvisioning extends SimpleProvisioning {

  /** name of connector for logging purposes */
  public final static String connectorName = "SISS_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  /** affiliation columns that this connector uses in logic */
  private String[] affiliationColumns = { "USR_UDF_IS_STAFF", "USR_UDF_IS_STUDENT", "USR_UDF_IS_EMERITUS", "USR_UDF_IS_FACULTY" };
  
  public String deprovisionUser(tcDataProvider dataProvider, String duDukeID, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + duDukeID);
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
    
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    
    // if the record exists in the target, we'll issue a delete.
    PeopleSoftConnectionWrapper connectionWrapper = PeopleSoftConnectionWrapper.getInstance(dataProvider);
    IDuIdmsStageCi record = connectionWrapper.getNewRecordObject();
    if (connectionWrapper.getRecordData(record, duDukeID, provisioningData.getTargetMapping("duDukeID"), false)) {
      logger.info(connectorName + ": Deleting PeopleSoft record for " + duDukeID);
      connectionWrapper.deleteRecord(record);
      logger.info(connectorName + ": Finished deleting PeopleSoft record for " + duDukeID);
    }
    
    logger.info(connectorName + ": Deprovision user: " + duDukeID + ".  Returning success.");
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String duDukeID, String unused1) {
    logger.info(connectorName + ": Provision user: " + duDukeID);
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
    
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    
    PeopleSoftConnectionWrapper connectionWrapper = PeopleSoftConnectionWrapper.getInstance(dataProvider);

    // get attributes from OIM for the user
    Iterator<String> allSyncAttributesIter = provisioningData.getAllAttributes(true, true, true, true).iterator();
    Set<String> allSyncAttributesOIMNames = new HashSet<String>();
    
    // we need to add the affiliation columns since they're involved in logic
    allSyncAttributesOIMNames.addAll(Arrays.asList(affiliationColumns));
    
    while (allSyncAttributesIter.hasNext()) {
      String attribute = (String) allSyncAttributesIter.next();
      if (attribute.toUpperCase().startsWith("USR_UDF")) {
        allSyncAttributesOIMNames.add(attribute);
      } else {
        allSyncAttributesOIMNames.add(attributeData.getOIMAttributeName(attribute));
      }
    }
    
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, allSyncAttributesOIMNames.toArray(new String[0]));
    
    // attributes to save to PeopleSoft
    Map<String, String> attributes = new HashMap<String, String>();
    
    boolean isStaff = checkAffiliation(moResultSet, "USR_UDF_IS_STAFF");
    boolean isStudent = checkAffiliation(moResultSet, "USR_UDF_IS_STUDENT");
    boolean isEmeritus = checkAffiliation(moResultSet, "USR_UDF_IS_EMERITUS");
    boolean isFaculty = checkAffiliation(moResultSet, "USR_UDF_IS_FACULTY");

    allSyncAttributesIter = provisioningData.getAllAttributes(isStudent, isFaculty, isEmeritus, isStaff).iterator();

    while (allSyncAttributesIter.hasNext()) {
      String attribute = allSyncAttributesIter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String attributeOIM;
      String value;
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      // get the OIM name for this attribute
      if (attribute.toUpperCase().startsWith("USR_UDF")) {
        attributeOIM = new String(attribute);
      } else {
        attributeOIM = attributeData.getOIMAttributeName(attribute);
      }
      
      try {
        value = moResultSet.getStringValue(attributeOIM);
      } catch (Exception e) {
        throw new RuntimeException("Error while getting value for " + attribute, e);
      }

      // transform data
      value = this.transformAttribute(duDukeID, attribute, value);
      
      // save to map
      attributes.put(targetAttribute, value);
    }
    
    // update PeopleSoft
    IDuIdmsStageCi record = connectionWrapper.getNewRecordObject();
   
    /*
    if (isStudent) {
      // we're not going to create a new record if it's a student
      boolean result = connectionWrapper.getRecordData(record, duDukeID, provisioningData.getTargetMapping("duDukeID"), false);
      if (!result) {
        throw new RuntimeException("Student not found in PeopleSoft");
      }
    } else {
      connectionWrapper.getRecordData(record, duDukeID, provisioningData.getTargetMapping("duDukeID"), true);
    }
    */
    
    // always do create() if get returns nothing due to consolidations
    connectionWrapper.getRecordData(record, duDukeID, provisioningData.getTargetMapping("duDukeID"), true);

    
    logger.info(connectorName + ": Saving PeopleSoft record for " + duDukeID);
    connectionWrapper.saveRecord(record, duDukeID, attributes);
    logger.info(connectorName + ": Finished saving PeopleSoft record for " + duDukeID);

    // if this is a student and a uid exists, we may have to update the user alias for an applicant entry
    String uid = attributes.get(provisioningData.getTargetMapping("uid"));
    if (isStudent && uid != null && !uid.equals("")) {
      this.updateUserAliasForApplicantEntries(dataProvider, duDukeID, uid);
    }
    
    logger.info(connectorName + ": Provision user: " + duDukeID + ".  Returning success.");

    return SUCCESS;
  }

  @SuppressWarnings("unchecked")
  public String updateUser(tcDataProvider dataProvider, String duDukeID, String unused1,
      String attribute, String unused2, String newValue) {
    logger.info(connectorName + ": Update user: " + duDukeID + ", attribute=" + attribute + ", newValue=" + newValue);
    
    if (duDukeID == null || duDukeID.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
   
    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }
    
    PeopleSoftConnectionWrapper connectionWrapper = PeopleSoftConnectionWrapper.getInstance(dataProvider);

    // if one of the affiliation columns are changing and a value exists, then we'll just run through the provisioning code.
    if (OIMAPIWrapper.isOIMAffiliationField(attribute) && newValue != null && newValue.equals("1")) {
      logger.info(connectorName + ": " + duDukeID + " is gaining an affiliation.  Running through provisioning code.");
      return this.provisionUser(dataProvider, duDukeID, null);
    }
    
    // if dukeid is changing, then we need to issue a delete on all old dukeids and then run through the provisioning code.
    if (attribute.equalsIgnoreCase("duDukeID")) {
      logger.info(connectorName + ": DukeID is changing for " + duDukeID + ".  Running deprovision for all old DukeIDs and then provision for current DukeID.");
      String[] attrs = { attributeData.getOIMAttributeName("duDukeIDHistory") };
      tcResultSet moResultSet = this.getOIMAttributesForUser(dataProvider, duDukeID, attrs);
      
      String value;
      try {
        value = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeIDHistory"));
      } catch (Exception e) {
        throw new RuntimeException("Error while getting value for duDukeIDHistory", e);
      }
      
      List<String> history = OIMAPIWrapper.split(value);
      Set<String> historySet = new HashSet<String>(history);
      historySet.remove(duDukeID);
      
      Iterator<String> iter = historySet.iterator();
      while (iter.hasNext()) {
        String oldDukeID = iter.next();
        this.deprovisionUser(dataProvider, oldDukeID, null);
      }
      
      return this.provisionUser(dataProvider, duDukeID, null);
    }
    
    // if we get this far, then we're just going to update the attribute in PeopleSoft if it meets the affiliation criteria
    tcResultSet moResultSet = getOIMAttributesForUser(dataProvider, duDukeID, affiliationColumns);
    
    boolean isStaff = checkAffiliation(moResultSet, "USR_UDF_IS_STAFF");
    boolean isStudent = checkAffiliation(moResultSet, "USR_UDF_IS_STUDENT");
    boolean isEmeritus = checkAffiliation(moResultSet, "USR_UDF_IS_EMERITUS");
    boolean isFaculty = checkAffiliation(moResultSet, "USR_UDF_IS_FACULTY");
    
    Set<String> allSyncAttributes = provisioningData.getAllAttributes(isStudent, isFaculty, isEmeritus, isStaff);
    if (allSyncAttributes.contains(attribute.toLowerCase())) {
      // attributes to save to PeopleSoft
      Map<String, String> attributes = new HashMap<String, String>();
      
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      // transform data
      String value = this.transformAttribute(duDukeID, attribute, newValue);
      
      attributes.put(targetAttribute, value);
      
      // update PeopleSoft
      IDuIdmsStageCi record = connectionWrapper.getNewRecordObject();
      boolean result = connectionWrapper.getRecordData(record, duDukeID, provisioningData.getTargetMapping("duDukeID"), false);
      if (!result) {
        throw new RuntimeException(duDukeID + " not found in PeopleSoft");
      }
      
      logger.info(connectorName + ": Saving PeopleSoft record for " + duDukeID);
      connectionWrapper.saveRecord(record, duDukeID, attributes);
      logger.info(connectorName + ": Finished saving PeopleSoft record for " + duDukeID);
      
      // if this is a student and a uid exists, we may have to update the user alias for an applicant entry
      String uid = attributes.get(provisioningData.getTargetMapping("uid"));
      if (isStudent && uid != null && !uid.equals("")) {
        this.updateUserAliasForApplicantEntries(dataProvider, duDukeID, uid);
      } 
    }
    
    logger.info(connectorName + ": Update user: " + duDukeID + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  /**
   * transform an attribute
   * @param dukeid
   * @param attribute
   * @param value
   * @return new value
   */
  private String transformAttribute(String dukeid, String attribute, String value) {
    
    if (value != null && value.equals("")) {
      value = null;
    }
    
    if (attribute.equalsIgnoreCase("duDateOfBirth") || attribute.equalsIgnoreCase("duSAPLastDayWorked")) {

      // Format date YYYYMMDD -> MM/DD/YYYY
      if (value != null) {
        if (value.length() != 8) {
          throw new RuntimeException("Invalid " + attribute + " for " + dukeid);
        }
        
        String year = value.substring(0, 4);
        String month = value.substring(4, 6);
        String date = value.substring(6, 8);
        
        return month + "/" + date + "/" + year;
        
      } else {
        return null;
      }
      
    } else if (attribute.equalsIgnoreCase("USR_UDF_HAS_JPEGPHOTO")) {
      
      // replace value with epoch time if we have a jpeg photo
      if (value != null && value.equals("1")) {
        long time = System.currentTimeMillis() / 1000;
        String stringTime = "" + time;
        return stringTime;
      } else {
        return null;
      }
      
    } else if (attribute.equalsIgnoreCase("duDukePhysicalAddressCountry")) {
      
      // transform 2 character country codes to 3 character country codes
      if (value != null) {
        String newValue = provisioningData.getProperty("transform.country.code." + value);
        if (newValue != null && !newValue.equals("")) {
          return newValue;
        } else {
          return value;
        }
      } else {
        return null;
      }
      
    } else if (attribute.equalsIgnoreCase("USR_UDF_IS_STAFF") || attribute.equalsIgnoreCase("USR_UDF_IS_FACULTY") ||
        attribute.equalsIgnoreCase("USR_UDF_IS_EMERITUS") || attribute.equalsIgnoreCase("USR_UDF_IS_STUDENT")) {
      if (value != null && value.equals("1")) {
        return "Y";
      } else {
        return "N";
      }
    }

    return value;
  }
  
  /**
   * @param dataProvider
   * @param duDukeID
   * @param attrs
   * @return tcResultSet
   */
  private tcResultSet getOIMAttributesForUser(tcDataProvider dataProvider, String duDukeID, String[] attrs) {
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> mhSearchCriteria = new Hashtable<String ,String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"), duDukeID);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duDukeID " + duDukeID);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return moResultSet;
  }
  
  /**
   * @param dataProvider
   * @param duPSEmplID
   * @param attrs
   * @return tcResultSet
   */
  private tcResultSet getOIMAttributesForApplicant(tcDataProvider dataProvider, String duPSEmplID, String[] attrs) {
    tcResultSet results = null;
    
    try {
      tcUserOperationsIntf userUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      Hashtable<String, String> searchCriteria = new Hashtable<String ,String>();
      searchCriteria.put(attributeData.getOIMAttributeName("duPSEmplID"), duPSEmplID);
      searchCriteria.put("USR_UDF_ENTRYTYPE", "applicants");
      searchCriteria.put("Users.Status", "ACTIVE");
      results = userUtility.findUsersFiltered(searchCriteria, attrs);
      
      if (results.getRowCount() == 0) {
        return null;
      }
      
      if (results.getRowCount() != 1) {
        throw new RuntimeException("Found more than one applicant entry with duPSEmplID=" + duPSEmplID);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return results;
  }
  
  /**
   * @param dataProvider
   * @param duPSEmplID
   * @param attrs
   */
  private void setOIMAttributesForApplicant(tcDataProvider dataProvider, String duPSEmplID, Map<String, String> attrs) { 
    
    String[] attrsForQuery = new String[] { attributeData.getOIMAttributeName("duPSUserAlias") };
    tcResultSet results = getOIMAttributesForApplicant(dataProvider, duPSEmplID, attrsForQuery);
    
    if (results == null) {
      // no applicant entries
      return;
    }
    
    try {
      tcUserOperationsIntf userUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");

      try {
        logger.info(connectorName + ": Updating user alias for applicant with duPSEmplID=" + duPSEmplID + ".");
        userUtility.updateUser(results, attrs);
      } catch (tcStaleDataUpdateException e) {
        // It's possible to get this exception if another reconciliation
        // happens to be updating this user at the same time.
        logger.warn(connectorName + ": Received tcStaleDataUpdateException while updating duPSEmplID=" + duPSEmplID + ". Retrying update once.");
        results = getOIMAttributesForApplicant(dataProvider, duPSEmplID, attrsForQuery);
        
        if (results == null) {
          // this is odd...  maybe the applicant *just* got deleted...?
          return;
        }
        
        userUtility.updateUser(results, attrs);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while updating OIM: " + e.getMessage(), e);
    }
  }
  
  private boolean checkAffiliation(tcResultSet moResultSet, String attribute) {
    String value;
    
    try {
      value = moResultSet.getStringValue(attribute);
    } catch (Exception e) {
      throw new RuntimeException("Error while getting value for " + attribute, e);
    }
    
    if (value != null && value.equals("1")) {
      return true;
    }
    
    return false;
  }
  
  /**
   * This method checks to see if the emplid for the modified entry also exists as
   * an applicant entry.  If so, the applicant entries' user alias is updated with 
   * the person's netid.
   * @param dataProvider
   * @param duDukeID
   * @param uid
   */
  private void updateUserAliasForApplicantEntries(tcDataProvider dataProvider, String duDukeID, String uid) {
    String[] attrsForQuery = new String[] { attributeData.getOIMAttributeName("duPSEmplID") };
    tcResultSet results = getOIMAttributesForUser(dataProvider, duDukeID, attrsForQuery);
    
    String emplid;
    try {
      emplid = results.getStringValue(attributeData.getOIMAttributeName("duPSEmplID"));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting value for duPSEmplID", e);
    }
    
    if (emplid == null || emplid.equals("")) {
      // odd... we don't have an emplid
      logger.warn(connectorName + ": " + duDukeID + " does not appear to have an emplid.  If there's an applicant entry for the person, we're skipping the user alias update.");
      return;
    }
    
    Map<String, String> attrs = new HashMap<String, String>();
    attrs.put(attributeData.getOIMAttributeName("duPSUserAlias"), uid);
    this.setOIMAttributesForApplicant(dataProvider, emplid, attrs);
  }
}
