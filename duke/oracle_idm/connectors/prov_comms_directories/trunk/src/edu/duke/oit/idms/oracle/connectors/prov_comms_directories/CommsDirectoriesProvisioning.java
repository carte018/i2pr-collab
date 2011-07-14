package edu.duke.oit.idms.oracle.connectors.prov_comms_directories;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.MailRoutingProvisioning;

/**
 * @author tsalazar
 * @author Michael Meadows (mm310)
 *
 */
public class CommsDirectoriesProvisioning extends SimpleProvisioning {

  public final static String connectorName = "COMMSDIR_PROVISIONING2";
  private AttributeData attributeData = AttributeData.getInstance();
  private ProvisioningDataImpl provisioningData = ProvisioningDataImpl.getInstance();
  private String duLDAPKey;

  /**
   * Create a Sun mail user, or nothing at all. Comms directory entries that do not have Sun mail are no longer
   *   supported. If duLDAPKey exists in pubdir, trigger an update from oim to pubdir. If netid exists in pubdir,
   *   trigger a consolidation
   *   @author Michael Meadows (mm310)
   *   @param dataProvider
   *   @param duLDAPKey
   *   @param entryType
   *   @return String SUCCESS
   */

  public String provisionUser(tcDataProvider dataProvider, String duLDAPKey, String entryType) {

    this.duLDAPKey = duLDAPKey;
    logger.info(addLogEntry("BEGIN", "Starting provisioning for user"));

    if (isConnectorDisabled()) {
      return SUCCESS;
    }

    // connect to pubdir
    LDAPConnectionWrapper ldapConnectionWrapper =  LDAPConnectionWrapper.getInstance(dataProvider);

    // check pubdir to see if entry already exists
    Attributes attributes = null;

    tcUserOperationsIntf moUserUtility = null;
    try {
      moUserUtility = (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
      tcResultSet moResultSet = getOIMResultsSet(moUserUtility);
      // set ldap dn from duLDAPKey value
      String dn = getDNString();
      SearchResult ldapKeySearch = ldapConnectionWrapper.findEntry(dn,new String[] {"duLDAPKey"});

      if (ldapKeySearch == null || ldapKeySearch.getAttributes().size() == 0) { // create new user
        attributes = getLDAPAttributesFromOIM(moResultSet);
        calcLDAPAttributes(attributes);
        setObjectClassAttributes (attributes);
        ldapConnectionWrapper.createEntry(dn,attributes);
        logger.info(addLogEntry("DEBUG", "Attributes: " + attributes.toString()));
      } else {
        // activate deleted user
        updateUser(dataProvider, duLDAPKey, entryType, "mailUserStatus", null, "active");
      }
      // can't do this from within connector due to OIM bug, where OIM would see the field update, and then re-spawn provisioning for SISS, even though
      // a SISS provisioning instance was already queued (but not completed). Using scheduled task to update this field.
      //setDuACMailBoxExists(moUserUtility, moResultSet, "1");
    } catch (tcAPIException e) {
      String msg = addLogEntry("FAILURE", "tcAPIException: " + e.getMessage());
      logger.info(msg);
      throw new RuntimeException(msg, e);
    } catch (NamingException e) {
      String msg = addLogEntry("FAILURE", "NamingException: " + e.getExplanation());
      logger.info(msg);
      moUserUtility.close();
      throw new RuntimeException(msg, e);
    }/* catch (tcUserNotFoundException e) {
			String msg = addLogEntry("FAILURE", "User with duLDAPKey: " + duLDAPKey + "not found.");
			logger.info(msg);
			moUserUtility.close();
			throw new RuntimeException(msg, e);
		} catch (tcStaleDataUpdateException e) {
			long [] failedKeys = e.getFailedKeys();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < failedKeys.length; ++i)
				sb.append(String.valueOf(failedKeys[i]) + " ");
			String msg = addLogEntry("FAILURE", "Update of stale data attempted for the following keys: " + sb);
			logger.info(msg);
			moUserUtility.close();
			throw new RuntimeException(msg, e);
		}*/
    moUserUtility.close();

    // update routing
    new MailRoutingProvisioning().provisionSunMailRouting(dataProvider, duLDAPKey);

    logger.info(addLogEntry("SUCCESS", "Provisioning successfully completed"));
    return SUCCESS;
  }

  /**
   * Remove/Deactivate a SunMail user. If the user has a null uid in OIM or duNetIDStatus = inactive, delete the pubdir entry, and unset
   * 		duAcMailboxExists OIM attribute.
   * @author Michael Meadows (mm310)
   * @param dataProvider
   * @param duLDAPKey
   * @param entryType
   * @return String SUCCESS
   */
  public String deprovisionUser(tcDataProvider dataProvider, String duLDAPKey, String entryType) {

    this.duLDAPKey = duLDAPKey;
    logger.info(addLogEntry("BEGIN", "Starting deprovisioning for user"));

    if (isConnectorDisabled()) {
      return SUCCESS;
    }

    tcUserOperationsIntf moUserUtility = null;
    try {
      moUserUtility = (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider,"Thor.API.Operations.tcUserOperationsIntf");
      tcResultSet moResultSet = getOIMResultsSet(moUserUtility);
      String netID = moResultSet.getStringValue(attributeData.getOIMAttributeName("uid"));
      LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
      String dn = getDNString();
      if (netID == null || netID.equals("")) {	// orphaned mailbox
        ldapConnectionWrapper.deleteEntry(dn);
        setDuACMailBoxExists(moUserUtility, moResultSet, "");

        // update routing
        new MailRoutingProvisioning().deprovisionSunMailRouting(dataProvider, duLDAPKey);

        return "DELETED";
      } else {	// mark user as deleted in comms, allow mbox cleanup, remove entry via ScheduledTask
        SearchResult result = ldapConnectionWrapper.findEntry(dn, new String [] {"dn", "mailUserStatus"});
        if (result != null && result.getAttributes().size() > 0) {
          String oldValue = result.getAttributes().get("mailUserStatus").get().toString();
          if (oldValue != null) {
            updateUser(dataProvider, duLDAPKey, entryType, "mailUserStatus", oldValue, "deleted");
          } else {
            ldapConnectionWrapper.deleteEntry(dn);
          }
        }
        String status = moResultSet.getStringValue("Users.Status");
        if (status != null && !status.equals("Deleted")) {
          setDuACMailBoxExists(moUserUtility, moResultSet, "");
        }
      }
    } catch (Exception e) {
      String msg = addLogEntry("FAILURE", "tcAPIException: " + e.getMessage());
      logger.info(msg);
      moUserUtility.close();
      throw new RuntimeException(msg, e);
    }

    moUserUtility.close();

    // update routing
    new MailRoutingProvisioning().deprovisionSunMailRouting(dataProvider, duLDAPKey);

    logger.info(addLogEntry("SUCCESS", "Completed deprovisioning for user"));
    return SUCCESS;
  }

  /**
   * Trigger attribute update in commsdir when the value of a paired attribute changes in OIM. If attribute is NetIDStatus, and it is disabled, then disable mail
   * 		forwarding by updating mailSieveRuleSource attribute in commsdir, otherwise enable mail forwarding
   * @param dataProvider
   * @param duLDAPKey
   * @param entryType
   * @param attribute
   * @param oldValue
   * @param newValue
   * @return
   */

  public String updateUser(tcDataProvider dataProvider, String duLDAPKey, String entryType, String attribute, String oldValue, String newValue) {

    this.duLDAPKey = duLDAPKey;
    logger.info(addLogEntry("BEGIN", "Changing attribute from " + oldValue + " to " + newValue));
    logger.info(addLogEntry("DEBUG", "attribute=" + attribute + ", oldValue=" + oldValue + ", newValue=" + newValue));

    if (isConnectorDisabled()) {
      return SUCCESS;
    }

    if (newValue != null && oldValue != null && newValue.equals(oldValue)) {
      return SUCCESS;
    }

    // connect to pubdir
    LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(dataProvider);
    // set ldap dn from duLDAPKey value
    String dn = getDNString();
    SearchResult result = ldapConnectionWrapper.findEntry(dn, new String [] {attribute});
    if (result != null && result.getAttributes().size() > 0) {
      if (attribute.equals("uid") && (newValue.equals("") || newValue == null)) {
        return (deprovisionUser(dataProvider, duLDAPKey, entryType));
      } else if (attribute.equals("duNetIDStatus")) {
        try {
          String oldMailSieveRule = getMailSieveRule(ldapConnectionWrapper, dn, newValue);
          String newMailSieveRule = "";
          if (oldMailSieveRule.contains(("redirect").toLowerCase())) {
            if (newValue.equals("inactive")) {
              newMailSieveRule = toggleMailSieveForwarding(oldMailSieveRule, true);
            } else {
              newMailSieveRule = toggleMailSieveForwarding(oldMailSieveRule, false);
            }
          }

          if (newMailSieveRule.length() > 0) {
            updateUser(dataProvider, duLDAPKey, entryType, "mailSieveRuleSource;binary", oldMailSieveRule, newMailSieveRule);
          }

        } catch (NamingException e) {
          String msg = addLogEntry("FAILURE","NamingException: " + e.getMessage());
          logger.info(msg);
          throw new RuntimeException(msg, e);
        }
      } else {
        Attributes attributes = new BasicAttributes();
        Attribute modAttribute = new BasicAttribute(attribute);

        logger.info(addLogEntry("DEBUG", attribute + "sync attrs: " + provisioningData.getSyncAttributes().toString()));

        if (attributeData.isMultiValued(attribute)) {
          @SuppressWarnings("rawtypes")
          Iterator newValues = OIMAPIWrapper.split(newValue).iterator();
          while (newValues.hasNext()) {
            modAttribute.add((String)newValues.next());
          }
        } else {
          modAttribute.add(newValue);
        }

        attributes.put(modAttribute);
        ldapConnectionWrapper.replaceAttributes(dn, entryType, attributes);
        logger.info(addLogEntry("DEBUG", "dn:" + dn + " entryType:" + entryType + " attributes:" + attributes.toString()));
      }
    } else {
      if (attribute.equals("uid")) {
        SearchResult previousEntry = getPreviousNetIDEntry(ldapConnectionWrapper, newValue);

        if (previousEntry != null) {
          if (previousEntry.getAttributes().get("mailUserStatus").equals(new BasicAttribute("mailUserStatus", "deleted"))) {
            logger.info(addLogEntry("DEBUG:", "Updating mailUserStatus to active."));
            previousEntry.getAttributes().put("mailUserStatus", "active");
          }
          provisionUser(dataProvider, duLDAPKey, entryType);
          consolidateUser(ldapConnectionWrapper, previousEntry, dn, entryType);
        }
      } else {
        logger.info(addLogEntry("WARNING", "LDAP entry not found."));
      }
    }

    logger.info(addLogEntry("SUCCESS", "Attribute updated."));
    return SUCCESS;
  }

  private SearchResult getPreviousNetIDEntry(LDAPConnectionWrapper ldapConnectionWrapper, String netID) {
    //Check for 2 ldap entries with same netID, sync attributes from non-triggering entry(non-duLDAPKey) to triggering duLDAPKey
    // then delete non-triggering duldapkey
    HashMap<String, String> mailOnlyAttrs = provisioningData.getMailOnlyAttributes();
    String [] mailAttrsArray = mailOnlyAttrs.keySet().toArray(new String[mailOnlyAttrs.size()]);
    NamingEnumeration<SearchResult> namingEnum = ldapConnectionWrapper.findEntryByUid(netID, mailAttrsArray);
    SearchResult previousEntry = null;
    if (namingEnum != null) {
      try {
        if (namingEnum.hasMore()) { // if an entry already exists, a consolidation is required
          previousEntry = namingEnum.next();
        }
        if (namingEnum.hasMore()) { // if more than one entry, manual intervention for a consolidation is needed
          String msg = addLogEntry("FAILED", "Consolidation attempted where duplicate netID set is greater than 2 - Manual intervention is required.");
          logger.info(msg);
          throw new RuntimeException(msg);
        }
      } catch (NamingException e) {
        String msg = addLogEntry("FAILURE", "Error when querying LDAP. Can't complete consolidation: " + e.getMessage());
        logger.info(msg);
        throw new RuntimeException(msg, e);
      }

    }
    return previousEntry;
  }

  private void consolidateUser(LDAPConnectionWrapper ldapConnectionWrapper, SearchResult previousEntry, String dn, String entryType) {
    if (previousEntry != null) { // consolidate entry
      ldapConnectionWrapper.replaceAttributes(dn, entryType, previousEntry.getAttributes());
      ldapConnectionWrapper.deleteEntry(previousEntry.getNameInNamespace());
    }
  }

  /**
   * Creates the host string based on DUId
   * @return host
   */
  private String determineHost(String id, String format) {

    //TODO: redesign based on metrics (available storage, load avg, etc.) - this has been moved to next version
    // check if format is a string literal, if so, we will use that to allow a specific store to be used
    String rangeStr = provisioningData.getProperty("mailHost.count");
    if (rangeStr == null)
      throw new RuntimeException(addLogEntry("ERROR", "Expected properties value for 'mailHost.count', but none was found."));
    if (!format.contains("%"))
      return format;
    int range = Integer.parseInt(rangeStr);
    int i = (Integer.parseInt(id) % range) + 1;
    String host = String.format(format, i);
    return host;
  }

  private String determineMessageStore (String host, String id, String format) {
    logger.info(addLogEntry("DEBUG", "determineMessageStore: host:" + host + " id:" + id + " format:" + format));
    final int asciiOffset = 96; // stores start with lowercase 'a'
    char store;
    if (!format.contains("%"))
      return format;

    int start = "comms-stor-0".length();
    int end = host.lastIndexOf(".oit.duke.edu");
    host = host.substring(0, end);
    int hostID = Integer.parseInt(host.substring(start));
    String rangeStr = provisioningData.getProperty("mailMessageStore.count." + host);
    if (rangeStr == null)
      return null;
    int range = Integer.parseInt(rangeStr);

    Random gen = new Random();
    int i = gen.nextInt(range) + 1;
    store = (char) (asciiOffset + i);
    logger.info(addLogEntry("DEBUG", "mailMessageStore:" + String.format(format, hostID, store)));
    return String.format(format, hostID, store);
  }

  /**
   * Checks if connector is disabled. If disabled with errors, throw exception and exit. If disabled without errors, assume disabled status
   *   is intentional.
   * @param service
   * @return boolean for disabled status of service component
   * @author mm310
   */
  private boolean isConnectorDisabled() {

    if (provisioningData.isConnectorDisabledWithoutErrors()) {
      logger.info(addLogEntry("DISABLED_WITHOUT_ERRORS", ""));
      return true;
    }
    if (provisioningData.isConnectorDisabled()) {
      String msg = addLogEntry("DISABLED", "");
      logger.info(msg);
      throw new RuntimeException(msg);
    }
    return false;
  }

  private String getDNString() {
    if (duLDAPKey == null || duLDAPKey.equals("")) {
      String msg = addLogEntry("FAILURE", "duLDAPKey value is null.");
      logger.info(msg);
      throw new RuntimeException(msg);
    }
    return ("duLDAPKey=" + duLDAPKey + ",o=Comms,dc=duke,dc=edu");
  }

  @SuppressWarnings("unchecked")
  private tcResultSet getOIMResultsSet(tcUserOperationsIntf tcInterface) {
    tcResultSet results = null;
    try {

      Set<String> attributes = new HashSet<String>();
      attributes.addAll(provisioningData.getSyncAttributes());
      attributes.addAll(provisioningData.getLogicAttributes());

      String[] allSyncAttributes = attributes.toArray(new String[attributes.size()]);
      String[] allSyncAttributesOIMNames = new String[allSyncAttributes.length + 1];
      for (int i = 0; i < allSyncAttributes.length; i++) {
        allSyncAttributesOIMNames[i] = attributeData.getOIMAttributeName(allSyncAttributes[i]);
      }
      allSyncAttributesOIMNames[allSyncAttributes.length] = "Users.Status";

      Iterator<String> iter = attributes.iterator();
      StringBuffer syncAttrBuf = new StringBuffer();
      while (iter.hasNext()) {
        syncAttrBuf.append(iter.next() + " ");
      }
      logger.info(addLogEntry("DEBUG", "LDAP Sync Attribute Names: " + syncAttrBuf.toString()));
      StringBuffer allOIMAttrsBuf = new StringBuffer();
      int i = 0;
      for(i = 0; i < allSyncAttributesOIMNames.length; i++) {
        allOIMAttrsBuf.append(allSyncAttributesOIMNames[i] + " ");
      }
      logger.info(addLogEntry("DEBUG", "OIM Sync Attribute Names: " + allOIMAttrsBuf.toString()));


      Hashtable<String, String> mhSearchCriteria = new Hashtable<String, String>();
      mhSearchCriteria.put(attributeData.getOIMAttributeName("duLDAPKey"), duLDAPKey);
      results = (tcInterface.findUsersFiltered(mhSearchCriteria,allSyncAttributesOIMNames));

      if (results.getRowCount() != 1) {
        String msg = addLogEntry("FAILURE", "Did not find exactly one entry in OIM for duLDAPKey = " + duLDAPKey);
        logger.info(msg);
        throw new RuntimeException(msg);
      }

      StringBuffer oimResultsBuf = new StringBuffer();
      results.goToRow(0);
      String[] colName = results.getColumnNames();
      for (int j = 0; j < colName.length; j++) {
        try {
          oimResultsBuf.append(colName[j] + ": " + results.getStringValue(colName[j]) + " ");
        } catch (tcColumnNotFoundException e) {
          String msg = addLogEntry("FAILURE", e.getMessage());
          logger.info(msg);
          throw new RuntimeException(msg, e);
        }
      }
      logger.info(addLogEntry("DEBUG", "OIM results: " + oimResultsBuf.toString()));

    } catch (tcAPIException e) {
      String msg = addLogEntry("FAILURE", e.getMessage());
      logger.info(msg);
      throw new RuntimeException(msg);
    }
    return results;
  }

  private Attributes getLDAPAttributesFromOIM(tcResultSet results) {

    logger.info(addLogEntry("DEBUG", "provisioningData.getSyncAttributes:" + provisioningData.getSyncAttributes().toString()));
    logger.info(addLogEntry("DEBUG", "provisioningData.getLogicAttributes:" + provisioningData.getLogicAttributes().toString()));

    Attributes attrs = new BasicAttributes();
    Iterator<?> iter = provisioningData.getSyncAttributes().iterator();

    while (iter.hasNext()) {

      String attribute = (String) iter.next();
      String targetAttribute = provisioningData.getTargetMapping(attribute);
      String value;


      if (targetAttribute == null || targetAttribute.equals("")) {
        String msg = addLogEntry("FAILURE", attribute + " does not have a target mapping.");
        logger.info(msg);
        throw new RuntimeException(msg);
      }

      Attribute addAttr = new BasicAttribute(targetAttribute);
      String attributeOIM = null;
      try {
        attributeOIM = attributeData.getOIMAttributeName(attribute);
        value = results.getStringValue(attributeOIM);

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

        // if the entry does not exist, only add attribute if it has values
        if (addAttr.size() > 0) {
          attrs.put(addAttr);
        }
      } catch (Exception e) {
        String msg = addLogEntry("FAILURE", e.getMessage());
        logger.info(msg);
        throw new RuntimeException(msg);
      }
    }

    return attrs;
  }

  private void calcLDAPAttributes (Attributes attributes) throws NamingException {

    HashMap<String, String> mailDefaultAttributes = provisioningData.getMailOnlyAttributes();
    Set<String> attributeNames = mailDefaultAttributes.keySet();
    Iterator<String> attributeNameIterator = attributeNames.iterator();
    String mailHost = null;
    while (attributeNameIterator.hasNext()) {
      String attributeName = attributeNameIterator.next();
      String attributeValue = mailDefaultAttributes.get(attributeName);

      if (attributeName.equals("mail")) {
        attributeValue = attributeValue.replace("UID", attributes.get("uid").get().toString());
      } else if (attributeName.equals("mailhost")) {
        if (mailHost == null) {
          mailHost = determineHost(attributes.get("duDukeID").get().toString(), attributeValue);
        }
        attributeValue = mailHost;
      } else if (attributeName.equals("mailmessagestore")) {
        if (mailHost == null) {
          mailHost = determineHost(attributes.get("duDukeID").get().toString(), mailDefaultAttributes.get("mailhost"));
        }
        attributeValue = determineMessageStore(mailHost, attributes.get("duDukeID").get().toString(), attributeValue);
        logger.info(addLogEntry("DEBUG", "attributeValue: " + attributeValue));
        if (attributeValue == null) // skip if mailStore is not subdivided into mailMessageStores
          continue;
      }
      attributes.put(new BasicAttribute(attributeName, attributeValue));
    }
  }

  private Attributes setObjectClassAttributes(Attributes attrs) {

    Attribute attrOC = new BasicAttribute("objectClass");
    attrOC.add("top");
    attrOC.add("duPerson");
    attrOC.add("organizationalPerson");
    attrOC.add("person");
    attrOC.add("inetOrgPerson");
    attrOC.add("inetUser");
    attrOC.add("inetMailUser");
    attrOC.add("inetLocalMailRecipient");
    attrOC.add("mailRecipient");
    attrs.put(attrOC);

    return attrs;
  }

  private void setDuACMailBoxExists(tcUserOperationsIntf utility, tcResultSet results, String value) throws tcAPIException,
  tcUserNotFoundException, tcStaleDataUpdateException {

    Hashtable<String, String> userValues = new Hashtable<String, String>();
    userValues.put(attributeData.getOIMAttributeName("duAcMailboxExists"), value);
    utility.updateUser(results, userValues);
  }

  private String getMailSieveRule(LDAPConnectionWrapper ldapConnectionWrapper, String dn, String duNetIDStatus) throws NamingException {
    SearchResult mailSieveResult = ldapConnectionWrapper.findEntry(dn, new String [] {"mailSieveRuleSource;binary"});
    String mailSieveRuleString = "";

    if (mailSieveResult!= null && mailSieveResult.getAttributes().size() > 0) {

      byte[] mailSieveRule64 = null;
      mailSieveRule64 = (byte []) mailSieveResult.getAttributes().get("mailSieveRuleSource;binary").get();
      mailSieveRuleString = new String(mailSieveRule64);

      logger.info(addLogEntry("DEBUG", "mailSieveRuleString: " + mailSieveRuleString));
    }
    return mailSieveRuleString;
  }

  private static String toggleMailSieveForwarding(String initial, boolean inactive) {
    String initialStartPattern = "redirect";
    String initialEndPattern = ";";
    String replaceStartPattern = "/*";
    String replaceEndPattern = "*/";
    int offSet = 1;

    if (!inactive) {
      initialStartPattern = "/*";
      initialEndPattern = "*/";
      replaceStartPattern = "";
      replaceEndPattern = "";
      offSet += initialStartPattern.length();
    }

    StringBuffer replaceString = new StringBuffer();
    int start = 0;
    int end = initial.indexOf(initialStartPattern);

    while (end > 0) {
      replaceString.append(initial.substring(start, end));
      start = end;
      end = initial.indexOf(initialEndPattern, start);

      String initialForward = initial.substring(start, end + offSet);

      if (inactive) {
        if (!initial.substring(start - 2, start).equals(replaceStartPattern))
          replaceString.append(replaceStartPattern + initialForward + replaceEndPattern + " ");
        else
          replaceString.append(initialForward);
      } else {
        if (initialForward.substring(0, initialStartPattern.length()).equals(initialStartPattern))
          replaceString.append(initialForward.substring(2, initialForward.length() - offSet));
        else
          replaceString.append(initialForward);
      }

      start = end + offSet;
      end = initial.indexOf(initialStartPattern, start);
    }

    if (replaceString.length() > 0)
      replaceString.append(initial.substring(start));

    return replaceString.toString();
  }

  /**
   * Formats and returns string for consistent message logging
   * @param status
   * @param message
   * @return
   */
  public String addLogEntry(String status, String message) {
    return (connectorName + ": " + status + " - " + duLDAPKey + " - " + message);
  }

  // legacy support - return SUCCESS only
  public String createMailbox(tcDataProvider dataProvider, String duLDAPKey, String entryType) {
    return SUCCESS;
  }

}
