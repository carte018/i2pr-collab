package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Properties;

/**
 * @author shilen
 */
public class PatientRecordsFeed extends SimpleFeed {

  /**
   * @param feedName
   * @param commonProperties
   */
  public PatientRecordsFeed(String feedName, Properties commonProperties) {
    super(feedName, commonProperties);
  }
  
  /**
   * 
   * @see edu.duke.oit.idms.oracle.scheduled_file_feeds.SimpleFeed#transformCurrentData(java.util.LinkedHashMap)
   */
  public void transformCurrentData(Properties props, LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data) {

    // get privacy attributes
    String attributesProperty = props.getProperty("feed.attributes");
    String[] attributes = attributesProperty.split(",\\s*");
    
    ArrayList<String> privacyAttributes = new ArrayList<String>();
    for (int i = 0; i < attributes.length; i++) {
      String attribute = attributes[i].toLowerCase();
      if (attribute.endsWith("privacy")) {
        privacyAttributes.add(attribute);
      }
    }
    
    Iterator<String> entriesIter = data.keySet().iterator();
    while (entriesIter.hasNext()) {
      String key = entriesIter.next();
      LinkedHashMap<String, LinkedList<String>> entry = data.get(key);
      
      // transform privacy attributes:  N -> private, Y -> public
      Iterator<String> privacyIter = privacyAttributes.iterator();
      while (privacyIter.hasNext()) {
        String privacyAttribute = privacyIter.next();
        LinkedList<String> values = entry.get(privacyAttribute);
        
        if (values != null && values.size() == 1) {
          String value = values.getFirst();
          boolean changed = false;
          
          if (value.equalsIgnoreCase("N")) {
            value = "private";
            changed = true;
          } else if (value.equalsIgnoreCase("Y")) {
            value = "public";
            changed = true;
          }
          
          if (changed) {
            values.remove();
            values.add(value);
            entry.put(privacyAttribute, values);
          }
        }
      }
      
      
      // set dn
      String dn = "";
      LinkedList<String> duLDAPKeySet = entry.get("duLDAPKey".toLowerCase());
      if (duLDAPKeySet.size() == 1) {
        String duLDAPKey = duLDAPKeySet.iterator().next();
        dn = "duLDAPKey=" + duLDAPKey + ",ou=People,dc=duke,dc=edu";
      }
      
      LinkedList<String> dnSet = new LinkedList<String>();
      dnSet.add(dn);
      entry.put("dn", dnSet);
      

      // set fullname as givenName + duMiddleName1 + sn
      String fullname = "";
      if (entry.get("givenName".toLowerCase()).size() == 1 && entry.get("sn".toLowerCase()).size() == 1) {
        fullname = entry.get("givenName".toLowerCase()).iterator().next();
        
        if (entry.get("duMiddleName1".toLowerCase()).size() == 1) {
          fullname += " " + entry.get("duMiddleName1".toLowerCase()).iterator().next();
        }
        
        fullname += " " + entry.get("sn".toLowerCase()).iterator().next();
      }
      
      LinkedList<String> values = new LinkedList<String>();
      values.add(fullname);
      
      entry.put("fullname", values);
      
      // set isDukeMedicine flag as 0 or 1
      // isDukeMedicine = 1 if employee and duSAPPersonnelArea != 1000 or student and academic career = ALHC|ALHG|MED|NBSN|NURS|PT|PT-D
      boolean isStudent = false;
      boolean isEmployee = false;
      String isDukeMedicine = "0";
      Iterator<String> affiliations = entry.get("eduPersonAffiliation".toLowerCase()).iterator();
      while (affiliations.hasNext()) {
        String affiliation = affiliations.next();
        if (affiliation.equals("student")) {
          isStudent = true;
        } else if (affiliation.equals("staff") || affiliation.equals("faculty") || affiliation.equals("emeritus")) {
          isEmployee = true;
        }
      }
      
      if (isEmployee && entry.get("duSAPPersonnelArea".toLowerCase()).size() == 1 && 
          !entry.get("duSAPPersonnelArea".toLowerCase()).iterator().next().equals("1000")) {
        isDukeMedicine = "1";
      } else if (isStudent) {
        LinkedList<String> allAcadCareers = new LinkedList<String>();
        allAcadCareers.addAll(entry.get("duPSAcadCareerC1".toLowerCase()));
        allAcadCareers.addAll(entry.get("duPSAcadCareerC2".toLowerCase()));
        allAcadCareers.addAll(entry.get("duPSAcadCareerC3".toLowerCase()));
        allAcadCareers.addAll(entry.get("duPSAcadCareerC4".toLowerCase()));
        
        Iterator<String> iter = allAcadCareers.iterator();
        while (iter.hasNext()) {
          String acadCareer = iter.next();
          if (acadCareer.equals("ALHC") || acadCareer.equals("ALHG") || acadCareer.equals("MED") || acadCareer.equals("NBSN") ||
              acadCareer.equals("NURS") || acadCareer.equals("PT") || acadCareer.equals("PT-D")) {
            isDukeMedicine = "1";
          }
        }
      }
      
      LinkedList<String> isDukeMedicineSet = new LinkedList<String>();
      isDukeMedicineSet.add(isDukeMedicine);
      entry.put("isDukeMedicine".toLowerCase(), isDukeMedicineSet);
    }
    
    // perform common transformations
    super.transformCurrentData(props, data);
  }

}
