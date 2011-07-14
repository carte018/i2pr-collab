package edu.duke.oit.idms.oracle.connectors.prov_mail_routing.logic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.LDAPConnectionWrapper;
import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.MailRoutingProvisioning;
import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.ProvisioningDataImpl;

/**
 * @author shilen
 */
public class FuquaLogic implements Logic {

  public boolean updateMailDrop(ProvisioningDataImpl provisioningData, LDAPConnectionWrapper ldapConnectionWrapper, 
      String duLDAPKey, String entryType, Map<String, String> attrs, SearchResult result) {

    // if fuqua only now, routing to sun mail, and has exchange, change routing to exchange unless...
    //   1.  mailDropOverride is set.
    //   2.  user is fuqua student and within blackout period
    
    if (!isFuquaOnly(attrs)) {
      return false;
    }
    
    String uid = attrs.get("uid");
    if (uid == null || uid.isEmpty()) {
      throw new RuntimeException("missing netid.");
    }
    
    Attribute allMailDrops = result.getAttributes().get("mailDrop");
    Attribute allMailAcceptingGeneralIds = result.getAttributes().get("mailAcceptingGeneralId");

    if (allMailDrops == null || allMailAcceptingGeneralIds == null || allMailDrops.size() != 1 || allMailAcceptingGeneralIds.size() == 0) {
      return false;
    }
    
    if (result.getAttributes().get("mailDropOverride") != null && result.getAttributes().get("mailDropOverride").contains("1")) {
      return false;
    }
    
    if (!allMailDrops.contains(uid + "@duke.edu")) {
      return false;
    }

    if (!allMailAcceptingGeneralIds.contains(uid + "@win.duke.edu")) {
      return false;
    }
    
    if (isFuquaStudent(attrs) && isInFuquaBlackoutPeriod(provisioningData)) {
      return false;
    }
    
    Attributes modAttrs = new BasicAttributes();
    
    allMailDrops.clear();
    allMailDrops.add(uid + "@win.duke.edu");
    modAttrs.put(allMailDrops);
    
    allMailAcceptingGeneralIds.remove(uid + "@win.duke.edu");
    allMailAcceptingGeneralIds.add(uid + "@duke.edu");
    modAttrs.put(allMailAcceptingGeneralIds);
    
    MailRoutingProvisioning.logger.info(MailRoutingProvisioning.connectorName + ": Updating routing from Sun mail to Exchange for " + duLDAPKey + " (" + uid + ").");
    ldapConnectionWrapper.replaceAttributes(duLDAPKey, entryType, modAttrs);
   
    return true;
  }

  /**
   * @param attrs
   * @return boolean
   */
  public static boolean isFuqua(Map<String, String> attrs) {    
    
    if (attrs.get("USR_UDF_IS_STAFF").equals("1") || attrs.get("USR_UDF_IS_EMERITUS").equals("1") ||
        attrs.get("USR_UDF_IS_FACULTY").equals("1") || attrs.get("USR_UDF_IS_AFFILIATE").equals("1")) {
      if (attrs.get("duFunctionalGroup").equals("Fuqua")) {
        return true;
      }
    }
    
    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      if (attrs.get("duPSAcadCareerC1").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC2").equalsIgnoreCase("FUQ") ||
          attrs.get("duPSAcadCareerC3").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC4").equalsIgnoreCase("FUQ")) {
        return true;
      }
      
      if (attrs.get("duPSAcadProgC1").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC2").equalsIgnoreCase("G-BUS") ||
          attrs.get("duPSAcadProgC3").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC4").equalsIgnoreCase("G-BUS")) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * @param attrs
   * @return boolean
   */
  public static boolean isFuquaStudent(Map<String, String> attrs) {    

    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      if (attrs.get("duPSAcadCareerC1").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC2").equalsIgnoreCase("FUQ") ||
          attrs.get("duPSAcadCareerC3").equalsIgnoreCase("FUQ") || attrs.get("duPSAcadCareerC4").equalsIgnoreCase("FUQ")) {
        return true;
      }
      
      if (attrs.get("duPSAcadProgC1").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC2").equalsIgnoreCase("G-BUS") ||
          attrs.get("duPSAcadProgC3").equalsIgnoreCase("G-BUS") || attrs.get("duPSAcadProgC4").equalsIgnoreCase("G-BUS")) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * @param attrs
   * @return boolean
   */
  public static boolean isFuquaOnly(Map<String, String> attrs) {
    if (isFuqua(attrs) && !isActiveAndNotOnlyFuqua(attrs)) {
      return true;
    }
    
    return false;
  }
  
  /**
   * @param attrs
   * @return boolean
   */
  public static boolean isActiveAndNotOnlyFuqua(Map<String, String> attrs) {    
    
    if (attrs.get("USR_UDF_IS_STAFF").equals("1") || attrs.get("USR_UDF_IS_EMERITUS").equals("1") ||
        attrs.get("USR_UDF_IS_FACULTY").equals("1") || attrs.get("USR_UDF_IS_AFFILIATE").equals("1")) {
      if (!attrs.get("duFunctionalGroup").equals("Fuqua")) {
        return true;
      }
    }
    
    if (attrs.get("USR_UDF_IS_STUDENT").equals("1")) {
      for (int i = 1; i <= 4; i++) {
        if (!attrs.get("duPSAcadCareerC" + i).isEmpty()) {
          if (!attrs.get("duPSAcadCareerC" + i).equalsIgnoreCase("FUQ") && !attrs.get("duPSAcadProgC" + i).equalsIgnoreCase("G-BUS")) {
            return true;
          }
        }
      }
    }
    
    return false;
  }
  
  /**
   * @param provisioningData
   * @return boolean
   */
  public static boolean isInFuquaBlackoutPeriod(ProvisioningDataImpl provisioningData) {
    
    String startDateString = provisioningData.getProperty("fuqua.blackout.start.date");
    String endDateString = provisioningData.getProperty("fuqua.blackout.end.date");
    if (startDateString == null || endDateString == null || startDateString.isEmpty() || endDateString.isEmpty()) {
      throw new RuntimeException("Missing blackout dates in config.");
    }
    
    SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
    
    int year = Calendar.getInstance().get(Calendar.YEAR);
    startDateString += year;
    endDateString += year;

    try {
      Date startDate = sdf.parse(startDateString);
      Date endDate = sdf.parse(endDateString);
      Date now = new Date();
      
      if (now.after(startDate) && now.before(endDate)) {
        return true;
      }

    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    return false;
  }
}
