package edu.duke.oit.idms.oracle.connectors.prov_dynamic_groups;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import edu.duke.oit.idms.registry.util.IDMSRegistryException;
import edu.duke.oit.idms.urn.FilterException;
import edu.duke.oit.idms.urn.URNFilter;
import edu.duke.oit.idms.urn.URNLookup;


/**
 * @author shilen
 *
 */
public class DynamicGroupsProvisioning extends SimpleProvisioning {

  public final static String connectorName = "DYNGRP_PROVISIONING";

  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  
  public String deprovisionUser(tcDataProvider dataProvider, String dukeid, String unused1) {
    logger.info(connectorName + ": Deprovision user: " + dukeid);

    // Ideally, this should remove the user from all dynamic groups.
    // However, since this most often happens due to a consolidation, with our current Grouper set up, 
    // doing so would cause subject not found exceptions.
    
    return SUCCESS;
  }

  public String provisionUser(tcDataProvider dataProvider, String dukeid, String unused1) {
    logger.info(connectorName + ": Provision user: " + dukeid);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    GrouperWSWrapper grouperWS = GrouperWSWrapper.getInstance(dataProvider);
    
    // here we should get all attributes that are part of a filter from OIM and sync with grouper.
    
    if (dukeid == null || dukeid.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
    
    Set attributesToQuery = new HashSet();
    
    Iterator urnFiltersIter = grouperWS.getUrnFilters().keySet().iterator();
    while (urnFiltersIter.hasNext()) {
      String groupName = (String)urnFiltersIter.next();
      URNFilter urnFilter = (URNFilter)grouperWS.getUrnFilters().get(groupName);
      Set filterAttributes = urnFilter.getAllAttributes();
      attributesToQuery.addAll(filterAttributes);
    }
    
    syncGroups(dataProvider, grouperWS.getUrnFilters(), attributesToQuery, dukeid);

    logger.info(connectorName + ": Provision user: " + dukeid + ".  Returning success.");
    return SUCCESS;
  }

  public String updateUser(tcDataProvider dataProvider, String dukeid, String unused1,
      String attribute, String unused2, String unused3) {
    logger.info(connectorName + ": Update user: " + dukeid + ", attribute=" + attribute);

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(connectorName + ": Connector is disabled without errors.");
      return SUCCESS;
    }

    if (provisioningData.isConnectorDisabled()) {
      throw new RuntimeException("Connector is disabled.");
    }

    GrouperWSWrapper grouperWS = GrouperWSWrapper.getInstance(dataProvider);
    
    // if the attribute changing is duDukeID, we'll run through the provisioning process.
    // this normally happens during some consolidations.  but we're not removing memberships
    // for the old duDukeID.  we'll assume either the consolidation process or USDU will handle that.
    if (attribute.equalsIgnoreCase("duDukeID")) {
      logger.info(connectorName + ": duDukeID changing for " + dukeid + ". Running provisioning operation...");
      return this.provisionUser(dataProvider, dukeid, null);
    }
    
    // here we'll see which filters use this attribute, then query OIM for the attributes used by those filters
    // then sync with grouper.
    
    String attributeURN;
    
    try {
      attributeURN = URNLookup.getUrn(attribute).toLowerCase();
    } catch (IDMSRegistryException e) {
      throw new RuntimeException("Unable to get URN for attribute: " + attribute);
    }
    
    if (dukeid == null || dukeid.equals("")) {
      throw new RuntimeException("No duDukeID available.");
    }
    
    Set attributesToQuery = new HashSet();
    Map urnFiltersUsingAttribute = new HashMap();
    
    Iterator urnFiltersIter = grouperWS.getUrnFilters().keySet().iterator();
    while (urnFiltersIter.hasNext()) {
      String groupName = (String)urnFiltersIter.next();
      URNFilter urnFilter = (URNFilter)grouperWS.getUrnFilters().get(groupName);
      Set filterAttributes = urnFilter.getAllAttributes();
      if (filterAttributes.contains(attributeURN)) {
        attributesToQuery.addAll(filterAttributes);
        urnFiltersUsingAttribute.put(groupName, urnFilter);
      }
    }
    
    if (urnFiltersUsingAttribute.size() > 0) {
      syncGroups(dataProvider, urnFiltersUsingAttribute, attributesToQuery, dukeid);
    }

    logger.info(connectorName + ": Update user: " + dukeid + ", attribute=" + attribute + ".  Returning success.");
    return SUCCESS;
  }
  
  private void syncGroups(tcDataProvider dataProvider, Map urnFilters, Set attributesToQuery, String dukeid) {
    
    GrouperDBConnectionWrapper grouperDBConnection = GrouperDBConnectionWrapper.getInstance(dataProvider);
    GrouperWSWrapper grouperWS = GrouperWSWrapper.getInstance(dataProvider);

    // Map<String, List<String>> that can be used by URNFilter
    Map mapForURNFilter = new HashMap();

    // FIRST we query OIM for attributes
    Set attributesToQueryOIMNames = new HashSet();
    
    Iterator attributesToQueryIter = attributesToQuery.iterator();
    while (attributesToQueryIter.hasNext()) {
      String urn = (String)attributesToQueryIter.next();
      String attribute;
      try {
        attribute = URNLookup.getAttrName(urn);
      } catch (IDMSRegistryException e) {
        throw new RuntimeException("Unable to get LDAP attribute name for URN: " + urn);
      }
      
      // if the attribute is eduPersonAffiliation, we need to query each individual affiliation field.
      if (attribute.equalsIgnoreCase("eduPersonAffiliation")) {
        attributesToQueryOIMNames.addAll(OIMAPIWrapper.getOIMAffiliationFieldNames());
      } else {
        attributesToQueryOIMNames.add(attributeData.getOIMAttributeName(attribute));
      }
    }
    
    String attributesToQueryOIMNamesArray[] = new String[attributesToQueryOIMNames.size()];
    attributesToQueryOIMNames.toArray(attributesToQueryOIMNamesArray);
    
    tcResultSet moResultSet = null;
    
    try {
      tcUserOperationsIntf moUserUtility = 
        (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
      
      Hashtable mhSearchCriteria = new Hashtable();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"), dukeid);
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attributesToQueryOIMNamesArray);
      
      if (moResultSet.getRowCount() != 1) {
        throw new RuntimeException("Did not find exactly one entry in OIM for duDukeID " + dukeid);
      }
      
      List affiliationValues = new ArrayList();
      String columns[] = moResultSet.getColumnNames();
      for (int i = 0; i < columns.length; i++) {
        String columnName = columns[i];
        String value = moResultSet.getStringValue(columns[i]);
        
        if (value == null || value.equals("")) {
          continue;
        }
        
        if (!attributesToQueryOIMNames.contains(columnName)) {
          // this is an attribute that we didn't ask for (such as Users.Key), so we're skipping it....
          continue;
        }
        
        if (OIMAPIWrapper.isOIMAffiliationField(columnName)) {
          if (value != null && value.equals("1")) {
            affiliationValues.add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(columnName));
          }
        } else {
          List values;
          String attribute = attributeData.getLDAPAttributeName(columnName);
          String urn = URNLookup.getUrn(attribute);
          if (attributeData.isMultiValued(attribute)) {
            values = OIMAPIWrapper.split(value);
          } else {
            values = new ArrayList();
            values.add(value);
          }
          
          mapForURNFilter.put(urn, values);
        }
      }
      
      mapForURNFilter.put(URNLookup.getUrn("eduPersonAffiliation"), affiliationValues);
      
    } catch (Exception e) {
      throw new RuntimeException("Failed while querying OIM: " + e.getMessage(), e);
    }
    
    // SECOND we query grouper for all dynamic groups the user is a member of
    Set existingDynamicGroups = grouperDBConnection.getDynamicGroupsForUser(dukeid);

    // THIRD we'll see if the user's current information indicates that the user should or should
    // not be a member of each group.  Then we'll modify grouper if necessary.
    Iterator urnFiltersIter = urnFilters.keySet().iterator();
    while (urnFiltersIter.hasNext()) {
      String groupName = (String)urnFiltersIter.next();
      URNFilter urnFilter = (URNFilter)urnFilters.get(groupName);
      try {
        boolean shouldBeMember = urnFilter.personMatches(mapForURNFilter);
        if (shouldBeMember && !existingDynamicGroups.contains(groupName)) {
          // add user to group
          grouperWS.addMember(groupName, dukeid);
          logger.info(connectorName + ": Added user " + dukeid + " to group " + groupName);
        } else if (!shouldBeMember && existingDynamicGroups.contains(groupName)) {
          // delete user from group
          grouperWS.deleteMember(groupName, dukeid);
          logger.info(connectorName + ": Removed user " + dukeid + " from group " + groupName);
        }
      } catch (FilterException e) {
        throw new RuntimeException("Error while checking if user should be a member of group " + groupName);
      }
    }
    
  }
}
