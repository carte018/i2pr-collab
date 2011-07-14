package edu.duke.oit.idms.oracle.connectors.prov_eprint2;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;


/**
 * @author shilen
 *
 */
public class EPrintProvisioning extends SimpleProvisioning {

  /**
   * 
   */
  public final static String connectorName = "EPRINT2_PROVISIONING";
  
  private final String TARGET_DEFAULT_PREFIX = "target.default.";

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

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);

    // here we're just deleting the user in the database table if the user exists

    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    databaseWrapper.deleteUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"));
    
    logger.info(connectorName + ": Deprovision user: " + duLDAPKey + ".  Returning success.");

    return SUCCESS;
  }

  @SuppressWarnings("unchecked")
  public String provisionUser(tcDataProvider dataProvider, String duLDAPKey, String unused1) {
    logger.info(connectorName + ": Provision user: " + duLDAPKey);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);

    // here we should get all attributes from OIM and sync them with the database.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    Map<String, String> attributes = new HashMap<String, String>();
    tcResultSet moResultSet = getUserData(dataProvider, duLDAPKey);
    //debug

    String netid = null;
    
    Iterator<String> iter = provisioningData.getSyncAttributes().iterator();
    while (iter.hasNext()) {
      String attribute = (String)iter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String attributeOIM = attributeData.getOIMAttributeName(attribute);
      String value;
      
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
      
      try {
        value = moResultSet.getStringValue(attributeOIM);
      } catch (Exception e) {
        throw new RuntimeException("Failed while retrieve attribute value for " + attributeOIM + ": " + e.getMessage(), e);
      }
      
      // netid cannot be null.  the access policy shouldn't allow it but we're just making sure...
      if (attribute.equals("uid") && (value == null || value.equals(""))) {
        throw new RuntimeException("Unable to provision because there's no NetID.");
      } else if (attribute.equals("uid")) {
        netid = new String(value);
      }
            
      if (value != null && !value.equals("")) {
        attributes.put(targetAttribute, value);
      } else {
        // this is needed in case we need to update all the fields rather than do an insert.
        attributes.put(targetAttribute, null);
      }
    }
    
    // take care of the group id
    String ePPA;
    String career;
    String prog;
    try {
      ePPA = moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation"));
      logger.info("value of ePPA = " + ePPA);
      career = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadCareerC1"));
      prog = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadProgC1"));
    } catch (Exception e) {
      throw new RuntimeException("Failed while retriving attribute value: " + e.getMessage(), e);
    }
    
    attributes.put("GROUP_ID", "" + getGroupId(ePPA, career, prog));
    attributes.put("active", "1");

    // provision CUSTOM2 with the academic plan or the functional group, depending on the ePPA
    String plan;
    String funcgroup;
    if (ePPA != null && !ePPA.isEmpty()) {
      try {
        if (ePPA.equals("student")) {
          plan = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadPlan10C1"));
          if (plan != null && !plan.isEmpty()) {
            attributes.put("CUSTOM2", plan);
          }
        } else if (ePPA.equals("staff") || ePPA.equals("affiliate")
            || ePPA.equals("faculty") || ePPA.equals("emeritus")) {
          funcgroup = moResultSet.getStringValue(attributeData.getOIMAttributeName("duFunctionalGroup"));
          if (funcgroup != null && !funcgroup.isEmpty()) {
            attributes.put("CUSTOM2", funcgroup);
          }
        } else if (ePPA.equals("alumni")) {
          // what should happen? nothing!
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed while setting attribute values: "
            + e.getMessage(), e);
      }
    } else {
      logger.info("ePPA was null or empty for user: " + duLDAPKey);
    }
    
    // if the netid exists (this may be part of a consolidation), rename it first.
    // it may be named back if this wasn't a consolidation.
    databaseWrapper.deleteUserByNetID(netid, provisioningData.getTargetMapping("uid"));
    
    // check to see if there's an existing dukecard and mark a conflict if there is
    String dukecard = null;
    try {
      dukecard = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeCardNbr"));
    } catch (Exception e) {
      throw new RuntimeException("Failed while getting dukecard value: "
          + e.getMessage(), e);
    }
    if (dukecard != null) {
      databaseWrapper.addConflictForDukeCard(dukecard, provisioningData.getTargetMapping("duDukeCardNbr"), duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"));
    }
    
    if (!databaseWrapper.isProvisioned(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"))) {
      // if the user isn't in the target system, add the user.  need to get defaults first...
      Map<String, String> allProperties = provisioningData.getAllProperties();
      Iterator<String> allPropertiesIter = allProperties.keySet().iterator();
      while (allPropertiesIter.hasNext()) {
        String property = allPropertiesIter.next();
        String value = allProperties.get(property);
        if (property.startsWith(TARGET_DEFAULT_PREFIX)) {
          String column = property.substring(TARGET_DEFAULT_PREFIX.length());
          attributes.put(column, value);
        }
      }
      
      databaseWrapper.addUser(attributes);
    } else {
      // if the user is in the target system, update the user.
      Iterator<String> iter2 = attributes.keySet().iterator();
      while (iter2.hasNext()) {
        String column = (String)iter2.next();
        String value = (String)attributes.get(column);
        
        databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), column, value);
      }
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

    EPrintDBConnectionWrapper databaseWrapper = EPrintDBConnectionWrapper.getInstance(dataProvider);
    
    // here we're just updating one attribute.
    
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      throw new RuntimeException("No duLDAPKey available.");
    }
    
    if (newValue != null && newValue.equals("")) {
      newValue = null;
    }
    
    // netid cannot be null.  the access policy shouldn't allow it but we're just making sure...
    if (attribute.equals("uid") && newValue == null) {
      return SUCCESS;
    }
    
    if (provisioningData.isSyncAttribute(attribute)) {
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      if (targetAttribute == null || targetAttribute.equals("")) {
        throw new RuntimeException(attribute + " does not have a target mapping.");
      }
     
      databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), targetAttribute, newValue);
    } else if (provisioningData.isLogicAttribute(attribute)) {
   //   String targetAttribute = provisioningData.getTargetMapping(attribute);
      // need to compute group_id...
      tcResultSet moResultSet = getUserData(dataProvider, duLDAPKey);

      String ePPA;
      String career;
      String prog;
      try {
        ePPA = moResultSet.getStringValue(attributeData.getOIMAttributeName("eduPersonPrimaryAffiliation"));
        career = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadCareerC1"));
        prog = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadProgC1"));
      } catch (Exception e) {
        throw new RuntimeException("Failed while retriving attribute value: " + e.getMessage(), e);
      }
      
      int groupId = getGroupId(ePPA, career, prog);
      databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), "GROUP_ID", "" + groupId);
  
      // if the ePPA is changing, update CUSTOM2 appropriately
      if (attribute.equals("eduPersonPrimaryAffiliation")) {
        String plan;
        String funcgroup;
        if (newValue != null && !newValue.isEmpty()) {
          try {
            if (newValue.equals("student")) {
              plan = moResultSet.getStringValue(attributeData.getOIMAttributeName("duPSAcadPlan10C1"));
              if (plan != null && !plan.isEmpty()) {
                databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), "CUSTOM2", plan);
              }
            } else if (newValue.equals("staff") || newValue.equals("affiliate")
                || newValue.equals("faculty") || newValue.equals("emeritus")) {
              funcgroup = moResultSet.getStringValue(attributeData.getOIMAttributeName("duFunctionalGroup"));
              if (funcgroup != null && !funcgroup.isEmpty()) {
                databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), "CUSTOM2", funcgroup);

              }
            } else if (newValue.equals("alumni")) {
              // what should happen? nothing!  OR SOMETHING?!?!
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed while setting attribute values: "
                + e.getMessage(), e);
          }
        } else {
          logger.info("newValue was null or empty for user: " + duLDAPKey);
        }
      } else if (attribute.equals("duPSAcadPlan10C1")) {
        // if the academic plan is changing, update CUSTOM2
        if (ePPA.equals("student")) {
          if (newValue != null && !newValue.isEmpty()) {
            databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), "CUSTOM2", newValue);  //or should we still give them the first one?
          }
        }       
      } else if (attribute.equals("duFunctionalGroup")) {
        // if he functional group is changing, update CUSTOM2
        if (ePPA.equals("staff") || ePPA.equals("affiliate")
            || ePPA.equals("faculty") || ePPA.equals("emeritus")) {
          if (newValue != null && !newValue.isEmpty()) {
            databaseWrapper.updateUser(duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"), "CUSTOM2", newValue);  
          }  // alums do not get new value
        }  
      }
    } else {
      throw new RuntimeException(attribute + " is not a logic or sync attribute");
    }
      
    if (attribute.equals("duDukeCardNbr")) {
      // check to see if there's an existing dukecard and mark a conflict if there is
      tcResultSet moResultSet = getUserData(dataProvider, duLDAPKey);
      String dukecard = null;
      try {
        dukecard = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeCardNbr"));
      } catch (Exception e) {
        throw new RuntimeException("Failed while getting dukecard value: "
            + e.getMessage(), e);
      }
      if (dukecard != null) {
        databaseWrapper.addConflictForDukeCard(dukecard, provisioningData.getTargetMapping("duDukeCardNbr"), duLDAPKey, provisioningData.getTargetMapping("duLDAPKey"));
      }
    }
    
    logger.info(connectorName + ": Update user: " + duLDAPKey + ", attribute=" + attribute + ", newValue=" + newValue + ".  Returning success.");

    return SUCCESS;
  }
  
  private tcResultSet getUserData(tcDataProvider dataProvider, String duLDAPKey) {
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
      
      String[] allAttributes = provisioningData.getAllAttributes();
      String[] allAttributesOIMNames = new String[allAttributes.length];
      for (int i = 0; i < allAttributes.length; i++) {
        allAttributesOIMNames[i] = attributeData.getOIMAttributeName(allAttributes[i]);
      }
      
      
      Hashtable<String, String> mhSearchCriteria = new Hashtable<String, String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, allAttributesOIMNames);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duLDAPKey " + duLDAPKey);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    return moResultSet;
  }
  
  private int getGroupId(String ePPA, String career, String prog) {

    ePPA = ePPA == null ? "" : ePPA;
    career = career == null ? "" : career;
    prog = prog == null ? "" : prog;
    
    int count = 1;
    while (true) {
      String value = provisioningData.getProperty("group.map." + count);
      if (value == null || value.equals("")) {
        break;
      }

      String[] parts = value.split(":");
      
      String currEPPA = parts[0];
      String currCareer = parts[1];
      String currProg = parts[2];
      int currGroupId = Integer.parseInt(parts[3]);
      
      if (currEPPA.equals(ePPA)) {
        if (currCareer.equals("")) {
          return currGroupId;
        }
        
        if (currCareer.equals(career)) {
          if (currProg.equals("")) {
            return currGroupId;
          }
          
          if (currProg.equals(prog)) {
            return currGroupId;
          }
        }
      }
      
      count++;
    }
    
    throw new RuntimeException("Did not get group id from ePPA=" + ePPA + ", career=" + career + ", prog=" + prog);
  }
}
