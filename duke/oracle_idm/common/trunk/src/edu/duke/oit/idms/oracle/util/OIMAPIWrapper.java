package edu.duke.oit.idms.oracle.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import edu.duke.oit.idms.oracle.exceptions.OIMUserNotFoundException;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Operations.tcUserOperationsIntf;


/**
 * @author shilen
 *
 */
public class OIMAPIWrapper {
  
  /**
   * delim used for multi-valued attrs in oim. 
   */
  public static final String MULTI_VALUED_ATTR_DELIM = "|";
  
  private static AttributeData attributeData = null;
  
  /**
   * @param dn
   * @return duLDAPKey
   */
  public static String getLDAPKeyFromDN(String dn) {
    dn = dn.toLowerCase();
    dn = dn.replaceAll("^.*duldapkey=", "");
    dn = dn.replaceAll(",.*", "");
    
    return dn;
   }
  
  /**
   * @param moUserUtility
   * @param dn
   * @return tcResultSet
   */
  public static tcResultSet findOIMUser(tcUserOperationsIntf moUserUtility, String dn) {
    
    String[] attrs = new String[2];
    attrs[0] = "Users.Key";
    attrs[1] = "Users.Row Version";
    
    return findOIMUser(moUserUtility, dn, attrs);
  }
   
   /**
   * @param moUserUtility
   * @param dn
   * @param attrs 
   * @return tcResultSet
   */
  public static tcResultSet findOIMUser(tcUserOperationsIntf moUserUtility, String dn, String[] attrs) {
     String duLDAPKey = getLDAPKeyFromDN(dn);
     Hashtable mhSearchCriteria = new Hashtable();
     if (attributeData == null) {
       attributeData = AttributeData.getInstance();
     }
     mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);

     
     tcResultSet moResultSet;
     try {
       moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
       if (moResultSet.getRowCount() > 1) {
         throw new RuntimeException("Got " + moResultSet.getRowCount() + " rows for dn " + dn);
       }
     } catch (tcAPIException e) {
       throw new RuntimeException(e);
     }
     
     return moResultSet;
   }
  
  /**
   * @param moUserUtility 
   * @param dn 
   * @param attribute (column in OIM)
   * @return String
   */
  public static String getAttributeValue(tcUserOperationsIntf moUserUtility, String dn, String attribute) {
    
    String[] attrs = new String[3];
    attrs[0] = "Users.Key";
    attrs[1] = "Users.Row Version";
    attrs[2] = attribute;
    
    try {
      tcResultSet moResultSet = findOIMUser(moUserUtility, dn, attrs);
      if (moResultSet.getRowCount() != 1) {
        throw new OIMUserNotFoundException("Got " + moResultSet.getRowCount() + " rows for user with DN " + dn);
      }
      return moResultSet.getStringValue(attribute);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param list
   * @return String
   */
  public static String join(List list) {
    String value = "";
    Iterator iter = list.iterator();
    
    while (iter.hasNext()) {
      value += iter.next() + MULTI_VALUED_ATTR_DELIM;
    }
    
    if (value.length() > 0) {
      value = value.substring(0, value.length() - 1);
    }
    
    return value;
  }
  
  /**
   * @param value
   * @return List
   */
  public static List split(String value) {
    String[] values = value.split(Pattern.quote(MULTI_VALUED_ATTR_DELIM));
    return Arrays.asList(values);
  }
  
  /**
   * returns user key from dukeid.
   * @param moUserUtility 
   * @param dukeid
   * @return long
   * @throws OIMUserNotFoundException 
   * @throws tcAPIException 
   * @throws tcColumnNotFoundException 
   */
  public static long getUserKeyFromDukeID(tcUserOperationsIntf moUserUtility, String dukeid)
      throws OIMUserNotFoundException, tcAPIException, tcColumnNotFoundException {
    Hashtable mhSearchCriteria = new Hashtable();
    mhSearchCriteria.put("Users.User ID", dukeid);

    String[] attrs = new String[1];
    attrs[0] = "Users.Key";
    
    tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
    if (moResultSet.getRowCount() != 1) {
      throw new OIMUserNotFoundException("Got " + moResultSet.getRowCount() + " rows for user with dukeid " + dukeid);
    }
    
    return moResultSet.getLongValue("Users.Key");
  }
  
  /**
   * @return Map where keys are OIM field names and values are affiliation values.
   */
  public static Map<String, String> getAffiliationMap() {
    Map<String, String> fields = new HashMap<String, String>();
    
    fields.put("USR_UDF_IS_STAFF", "staff");
    fields.put("USR_UDF_IS_STUDENT", "student");
    fields.put("USR_UDF_IS_EMERITUS", "emeritus");
    fields.put("USR_UDF_IS_FACULTY", "faculty");
    fields.put("USR_UDF_IS_ALUMNI", "alumni");
    fields.put("USR_UDF_IS_AFFILIATE", "affiliate");
    
    return fields;
  }
  
  /**
   * @return Set of OIM field names used for affiliations.
   */
  public static Set<String> getOIMAffiliationFieldNames() {
    return getAffiliationMap().keySet();
  }
  
  /**
   * @param field
   * @return true if the given field is an affiliation field.
   */
  public static boolean isOIMAffiliationField(String field) {
    if (getOIMAffiliationFieldNames().contains(field.toUpperCase())) {
      return true;
    }
    
    return false;
  }
  
  /**
   * @param field
   * @return the affiliation value for a given affiliation field.
   */
  public static String getAffiliationValueFromOIMFieldName(String field) {
    return getAffiliationMap().get(field.toUpperCase());
  }
}
