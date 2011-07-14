package edu.duke.oit.idms.oracle.connectors.recon_service_directories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.SortControl;

import netscape.ldap.LDAPAttribute;
import netscape.ldap.LDAPModification;
import netscape.ldap.util.LDIF;
import netscape.ldap.util.LDIFAddContent;
import netscape.ldap.util.LDIFContent;
import netscape.ldap.util.LDIFModifyContent;
import netscape.ldap.util.LDIFRecord;
import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcStaleDataUpdateException;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.Logic;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.util.ConnectorConfig;
import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import edu.duke.oit.idms.registry.DBAttribute;
import edu.duke.oit.idms.urn.URNLookup;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

class Shutdown extends Thread {
  private RunConnector connector;
  public Shutdown(RunConnector connector) {
    super();
    this.connector = connector;
  }
  public void run() {
    if (!connector.isResync) {
      connector.LOG.error("SHUTDOWN: Received shutdown signal.");
    }
    connector.isShuttingDown = true;
    while (connector.isShutDown == false) {
      try { 
        Thread.sleep(2000); 
      } catch (Exception e) {
        // this shouldn't happen...
      }
    }
    if (!connector.isResync) {
      connector.LOG.info("SHUTDOWN: Finished running shutdown routine.");
    }
  }
}

/**
 * Used to parse the changelog on the service directories so that we can apply the changes
 * to Oracle IdM and the Person Registry.
 * 
 * @author shilen
 */
public class RunConnector {
  
  protected boolean isShuttingDown = false;
  protected boolean isShutDown = false;
  protected org.apache.log4j.Logger LOG = Logger.getLogger(RunConnector.class);
  private ConnectorConfig cfg = ConnectorConfig.getInstance();
  private AttributeData attributeData = AttributeData.getInstance();
  private tcUtilityFactory ioUtilityFactory = null;
  private tcUserOperationsIntf moUserUtility;
  private LdapContext context = null;
  private PersonRegistryHelper personRegistryHelper = null;
  protected boolean isResync = false;
  private boolean disablePRFeed = false;

  /**
   * @param resync
   */
  public RunConnector(boolean resync) {
    this.isResync = resync;
  }

  /**
   * @param args To resync one or more DNs, the first arg should be a file of DNs.
   */
  public static void main(String args[]) {
    PropertyConfigurator.configure(System.getenv("OIM_CONNECTOR_HOME") + "/conf/log4j.properties");
    boolean isResync = false;
    if (args.length > 0) {
      isResync = true;
    }
    
    RunConnector connector = new RunConnector(isResync);

    try {
      Runtime.getRuntime().addShutdownHook(new Shutdown(connector));
      connector.init();
      if (connector.isResync) {
        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String dn;
        while ((dn = br.readLine()) != null) {
          connector.resync(dn);
          System.out.println("Resync completed successfully for " + dn);
        }
        br.close();
      } else {
        while (connector.isShuttingDown == false) {
          try {
            boolean result = connector.getNewChanges();
            if (!result) {
              Thread.sleep(10000); // if there were no changes, sleep for 10 seconds
            }
          } catch (NamingException e) {
            
            // If we get an LDAP error, rather than stopping the connector, we'll keep trying to reconnect
            // every 5 minutes.
            connector.LOG.error("LDAP Error while reading changelog.  Sleeping for 5 minutes and then reconnecting...", e);
 
            boolean success = false;
            
            while (success == false) {
              Thread.sleep(300000);
              connector.shutdown();

              try {
                connector.init();
                success = true;
              } catch (RuntimeException e2) {
                connector.LOG.error("Error while initilizing connector.  Sleeping for 5 minutes.", e2);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      if (connector.isResync) {
        e.printStackTrace();
        System.out.println("");
        System.out.println("Resync aborting due to a failure.  Any DNs in the input file after this failure are not being processed.");
        System.out.println("Check the log file to determine the entry that failed.");
      } else {
        connector.LOG.fatal("Connector is shutting down.  Error while running connector", e);
      }
    } finally {
      connector.shutdown();
    }

    System.exit(0);
  }
  
  protected void shutdown() {
    if (!isResync) {
      LOG.info("SHUTDOWN: Connector is closing connections.");
    }
    
    try {
      if (personRegistryHelper != null) {
        personRegistryHelper.shutdown();
        personRegistryHelper = null;
      }
    } catch (SQLException e) {
      // this is okay
    }
      
    if (ioUtilityFactory != null) {
      ioUtilityFactory.close();
      ioUtilityFactory = null;
    }
      
    try {
      if (context != null) {
        context.close();
        context = null;
      }
    } catch (NamingException e) {
      // this is okay
    }
    
    if (!isResync) {
      LOG.info("SHUTDOWN: Connector finished closing connections.");
    }

    this.isShutDown = true;
  }
  
  private void init() {
    try {
      
      this.isShutDown = false;
      
      // initialize oim
      ConfigurationClient.ComplexSetting config = ConfigurationClient
          .getComplexSettingByPath("Discovery.CoreServer");

      ioUtilityFactory = new tcUtilityFactory(config.getAllSettings(), cfg
          .getProperty("oim.login.username"), cfg.getProperty("oim.login.password"));
      moUserUtility = (tcUserOperationsIntf) ioUtilityFactory
      .getUtility("Thor.API.Operations.tcUserOperationsIntf");
      
      
      
      // initialize ldap
      
      Hashtable<String, String> environment = new Hashtable<String, String>();
      environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, cfg.getProperty("ldap.provider"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, cfg.getProperty("ldap.binddn"));
      environment.put(Context.SECURITY_CREDENTIALS, cfg.getProperty("ldap.password"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      
      context = new InitialLdapContext(environment, null);
      
      if (cfg.getProperty("pr.disable") != null && cfg.getProperty("pr.disable").toLowerCase().equals("true")) {
        this.disablePRFeed = true;
      }
      
      
      // initialize person registry
      
      if (this.disablePRFeed == false) {
        personRegistryHelper = new PersonRegistryHelper();
      }
 
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return true if changes were processed
   * @throws NamingException 
   * @throws IOException 
   * 
   */
  public boolean getNewChanges() throws NamingException, IOException {  
    
    String attrs[] = { "changeNumber", "changeType", "targetDn", "changes" };
    Control[] ctls = new Control[] {new SortControl(new String[]{"changeNumber"}, Control.CRITICAL)};
    context.setRequestControls(ctls);
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0,
        attrs, false, false);

    
    BufferedReader br = new BufferedReader(new FileReader(System.getenv("OIM_CONNECTOR_HOME")
        + "/work/lastchangelog"));
    
    String line = br.readLine();
    br.close();
    
    int maxChangeNumber = Integer.parseInt(line);
    
    int startQuery = Integer.parseInt(line);
    int endQuery = startQuery + 2000;
    
    String filter = "(&(changenumber>=" + startQuery + ")(changenumber<=" + endQuery + "))";
    LOG.info("Query LDAP with filter: " + filter);
    
    NamingEnumeration<SearchResult> results = context.search("cn=changelog",
        filter, cons);
    context.setRequestControls(null); // clear out sort by change number
    if (!results.hasMoreElements()) {
      return false;
    }
    
    while (results.hasMoreElements()) {
      SearchResult entry = results.next();
      if (!isShuttingDown) {
        int next = processChange(entry);
        if (next > maxChangeNumber) {
          maxChangeNumber = next;
        }
      }
    }
    
    maxChangeNumber++;
    BufferedWriter bw = new BufferedWriter(new FileWriter(System.getenv("OIM_CONNECTOR_HOME")
        + "/work/lastchangelog"));
    
    bw.write("" + maxChangeNumber);
    bw.close();
    
    return true;
  }

  /**
   * @param entry
   * @return changeNumber
   */
  public int processChange(SearchResult entry) {

    String changeNumber = null;
    String dn = null;
    try {
      Attributes attributes = entry.getAttributes();
      dn = ((String) attributes.get("targetDn").get()).toLowerCase().replaceAll("\\s+",
          "");
      String changeType = (String) attributes.get("changeType").get();
      changeNumber = (String) attributes.get("changeNumber").get();
      String changes = "";
      LOG.info("Working on change number " + changeNumber + " - " + changeType + " - "
          + dn);
      if (attributes.get("changes") != null) {
        changes = ((String) attributes.get("changes").get()).trim();
      }

      if (!dn.endsWith("ou=people,dc=duke,dc=edu")
          && !dn.endsWith("ou=test,dc=duke,dc=edu")
          && !dn.endsWith("ou=accounts,dc=duke,dc=edu")) {
        return Integer.parseInt(changeNumber);
      }

      String ldifString = "dn: " + dn + "\n";
      ldifString += "changetype: " + changeType + "\n";
      ldifString += changes;

      LOG.info("LDIF: --\n" + ldifString);

      LDIF ldif = new LDIF(new DataInputStream(new ByteArrayInputStream(ldifString
          .getBytes())));
      LDIFRecord record = ldif.nextRecord();
      int type = record.getContent().getType();

      if (type == LDIFContent.ADD_CONTENT) {
        addUser(record, true, true);
      } else if (type == LDIFContent.MODIFICATION_CONTENT) {
        updateUser(record);
      } else if (type == LDIFContent.DELETE_CONTENT) {
        deleteUser(record);
      } else {
        throw new RuntimeException("Unsupported changetype.");
      }
    } catch (Exception e) {
      LOG.error("Exception while working on changeNumber " + changeNumber + ", DN: " + dn
          + ".  You may need to re-execute this change.", e);
    }
    
    return Integer.parseInt(changeNumber);
  }
  
  
  private void deleteUser(LDIFRecord record) {
    try {
      tcResultSet moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, record.getDN());
      moUserUtility.deleteUser(moResultSet.getLongValue("Users.Key"));
      LOG.info("User deleted in OIM.");

      if (this.disablePRFeed == false) {
        personRegistryHelper.deleteUser(OIMAPIWrapper.getLDAPKeyFromDN(record.getDN()));
        LOG.info("User deleted in the Person Registry.");
      }
      
      LOG.info("Delete finished.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void updateUser(LDIFRecord record) {
    try {

      Map<String, String> attrsForOIM = new HashMap<String, String>();
      Map<String, PersonRegistryAttribute> attrsForPR = new HashMap<String, PersonRegistryAttribute>();
      
      tcResultSet moResultSet = null;

      LDIFModifyContent modifyContent = (LDIFModifyContent) record.getContent();
      LDAPModification[] modArray = modifyContent.getModifications();

      // loop through attributes to see if either OIM or PR want them.
      for (int i = 0; i < modArray.length; i++) {
        LDAPModification mod = modArray[i];
        LDAPAttribute modAttribute = mod.getAttribute();
        String attributeName = mod.getAttribute().getName();

        // if we're sync'ing to OIM
        if (cfg.isOIMSyncAttribute(attributeName)) {
          String value = "";
          if (moResultSet == null) {
            moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, record.getDN());
            if (moResultSet.getRowCount() == 0) {
              LOG.info("User with dn " + record.getDN() + " is not in OIM.");
              resync(record.getDN());
              return;
            }
          }

          if (!attributeData.isMultiValued(attributeName)) {
            if ((mod.getOp() == LDAPModification.ADD
                || mod.getOp() == LDAPModification.REPLACE) &&
                modAttribute.getStringValueArray() != null &&
                modAttribute.getStringValueArray().length > 0) {
              value = modAttribute.getStringValueArray()[0];
            } else {
              value = "";
            }
          } else {
            List<String> valuesAsList = Arrays.asList(modAttribute.getStringValueArray());

            if (mod.getOp() == LDAPModification.DELETE
                && (modAttribute.getStringValueArray() == null || modAttribute
                    .getStringValueArray().length == 0)) {
              value = "";
            } else if (mod.getOp() == LDAPModification.DELETE) {
              throw new RuntimeException(
                  "A delete with values doesn't appear to actually be happening.");
            } else if (mod.getOp() == LDAPModification.REPLACE) {
              value = OIMAPIWrapper.join(valuesAsList);
            } else {
              String newValue = OIMAPIWrapper.join(valuesAsList);

              String oldvalue = OIMAPIWrapper.getAttributeValue(moUserUtility, record
                  .getDN(), attributeData.getOIMAttributeName(attributeName));
              if (oldvalue == null || oldvalue.equals("")) {
                value = newValue;
              } else {
                // lets make sure we don't add any duplicates here
                // duplicates can be added if a changelog entry gets run twice
                value = oldvalue + OIMAPIWrapper.MULTI_VALUED_ATTR_DELIM + newValue;
                Set<String> uniqueValues = new LinkedHashSet<String>();
                uniqueValues.addAll(OIMAPIWrapper.split(value));
                value = OIMAPIWrapper.join(new ArrayList(uniqueValues));
              }
            }
          }

          attrsForOIM.put(attributeData.getOIMAttributeName(attributeName), value);
        }

        // if we're sync'ing to PR
        if (cfg.isPRSyncAttribute(attributeName) && this.disablePRFeed == false) {
          String urn = URNLookup.getUrn(attributeName);
          PersonRegistryAttribute prAttr = new PersonRegistryAttribute(urn);
          DBAttribute attributeClass = personRegistryHelper.getDBAttribute(urn);
          if (attributeClass.isSingleValue()) {
            if ((mod.getOp() == LDAPModification.ADD
                || mod.getOp() == LDAPModification.REPLACE) &&
                modAttribute.getStringValueArray() != null &&
                modAttribute.getStringValueArray().length > 0) {
              prAttr.addValueToAdd(modAttribute.getStringValueArray()[0]);
            } else {
              prAttr.setRemoveAllValues(true);
            }
          } else {
            if (mod.getOp() == LDAPModification.DELETE
                && (modAttribute.getStringValueArray() == null || modAttribute
                    .getStringValueArray().length == 0)) {
              prAttr.setRemoveAllValues(true);
            } else if (mod.getOp() == LDAPModification.DELETE) {
              throw new RuntimeException(
                  "A delete with values doesn't appear to actually be happening.");
            } else if (mod.getOp() == LDAPModification.REPLACE) {
              // For a replace in the person registry, we need to find out which values are actually changing.
              // Otherwise, history attributes will have their values "stop" and "start".
              if (modAttribute.getStringValueArray() == null
                  || modAttribute.getStringValueArray().length == 0) {
                prAttr.setRemoveAllValues(true);
              } else {
                List<String> newValues = Arrays.asList(modAttribute.getStringValueArray());
                List<String> oldValues = personRegistryHelper.getAttributeValues(
                    OIMAPIWrapper.getLDAPKeyFromDN(record.getDN()), urn);
                
                List<String> valuesToAdd = new ArrayList<String>();
                valuesToAdd.addAll(newValues);
                valuesToAdd.removeAll(oldValues);
                
                List<String> valuesToRemove = new ArrayList<String>();
                valuesToRemove.addAll(oldValues);
                valuesToRemove.removeAll(newValues);
                
                prAttr.addAllValuesToAdd(valuesToAdd);
                prAttr.addAllValuesToRemove(valuesToRemove);
              }
            } else {
              for (int j = 0; j < modAttribute.getStringValueArray().length; j++) {
                prAttr.addValueToAdd(modAttribute.getStringValueArray()[j]);
              }
            }
          }

          attrsForPR.put(urn, prAttr);
        }
      }

      // loop through attributes to see if they involve any business logic.
      for (int i = 0; i < modArray.length; i++) {
        LDAPModification mod = modArray[i];
        LDAPAttribute modAttribute = mod.getAttribute();
        String attributeName = mod.getAttribute().getName();

        if (cfg.isLogicAttribute(attributeName)) {
          if (moResultSet == null) {
            moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, record.getDN());
            if (moResultSet.getRowCount() == 0) {
              LOG.info("User with dn " + record.getDN() + " is not in OIM.");
              resync(record.getDN());
              return;
            }
          }

          Iterator<Logic> allLogicIter = cfg.getLogicClass(attributeName).iterator();
          while (allLogicIter.hasNext()) {
            Logic logic = allLogicIter.next();
            logic.doSomething(attrsForOIM, attrsForPR, attributeName, modAttribute
                .getStringValueArray(), mod.getOp(), moUserUtility, context,
                personRegistryHelper, record.getDN());
          }
        }
      }

      // make sure we're not updating sn and givenName.  Those are only added for new entries..
      attrsForOIM.remove(attributeData.getOIMAttributeName("sn"));
      attrsForOIM.remove(attributeData.getOIMAttributeName("givenName"));
      
      if (attrsForOIM.size() > 0) {        
        LOG.info("Attributes used to update OIM user: ");
        Iterator<String> iter = attrsForOIM.keySet().iterator();
        while (iter.hasNext()) {
          String key = iter.next();
          LOG.info(key + " - " + attrsForOIM.get(key));
        }
        
        try {
          moUserUtility.updateUser(moResultSet, attrsForOIM);
        } catch (tcStaleDataUpdateException e) {
          // It's possible to get this exception if another reconciliation
          // happens to be updating this user at the same time.  Tests in 
          // development reveal that we're seeing this issue at least
          // once a day.  To resolve this, we will get the user's result set
          // again and try the update again.  If it still fails, we'll let
          // the exception be thrown.
          LOG.warn("Received tcStaleDataUpdateException while updating " + record.getDN() + ". Retrying update once.");
          moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, record.getDN());
          moUserUtility.updateUser(moResultSet, attrsForOIM);
        } catch (tcAPIException e) {
          // if we get tcAPIException, we'll retry once after sleeping.
          LOG.error("Received tcAPIException while updating " + record.getDN() + ". Retrying update once after sleeping.");
          Thread.sleep(10000);
          moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, record.getDN());
          moUserUtility.updateUser(moResultSet, attrsForOIM);
        }
        
        LOG.info("User updated in OIM.");
      }

      if (attrsForPR.size() > 0 && this.disablePRFeed == false) {
        long prid = personRegistryHelper.getPRIDFromLDAPKey(OIMAPIWrapper.getLDAPKeyFromDN(record.getDN()));
        if (prid < 0) {
          LOG.info("User with dn " + record.getDN() + " is not in the Person Registry.");
          resync(record.getDN());
          return;    
        }

        LOG.info("Attributes used to update the Person Registry user: ");
        Iterator<String> iter = attrsForPR.keySet().iterator();
        while (iter.hasNext()) {
          String key = iter.next();
          PersonRegistryAttribute prAttr = attrsForPR.get(key);
          if (prAttr.isRemoveAllValues()) {
            LOG.info(key + " - remove all values");
          }
          
          Iterator<String> iter2 = prAttr.getValuesToAdd().iterator();
          while (iter2.hasNext()) {
            String value = iter2.next();
            LOG.info(key + " - add value - " + value);
          }
          
          iter2 = prAttr.getValuesToRemove().iterator();
          while (iter2.hasNext()) {
            String value = iter2.next();
            LOG.info(key + " - remove value - " + value);
          }
        }

        personRegistryHelper.updateUser(prid, attrsForPR);
        
        LOG.info("User updated in the Person Registry.");
      }

      LOG.info("Update finished.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
  
  private void addUser(LDIFRecord record, boolean addToOIM, boolean addToPR) {
    try {
      Map<String, String> attrsForOIM = new HashMap<String, String>();
      Map<String, PersonRegistryAttribute> attrsForPR = new HashMap<String, PersonRegistryAttribute>();
      
      LDIFAddContent addContent = (LDIFAddContent) record.getContent();
      LDAPAttribute[] addAttributes = addContent.getAttributes();
      
      // loop through attributes to see if either OIM or PR want them.
      for (int i = 0; i < addAttributes.length; i++) {
        LDAPAttribute addAttribute = addAttributes[i];
        String attributeName = addAttribute.getName();
        
        // if we're sync'ing to OIM
        if (addToOIM && cfg.isOIMSyncAttribute(attributeName)) {
          String value = "";
          if (!attributeData.isMultiValued(attributeName)) {
            value = addAttribute.getStringValueArray()[0];
          } else {
            for (int j = 0; j < addAttribute.getStringValueArray().length; j++) {
              value += addAttribute.getStringValueArray()[j] + OIMAPIWrapper.MULTI_VALUED_ATTR_DELIM;
            }
            value = value.substring(0, value.length() - 1);
          }

          attrsForOIM.put(attributeData.getOIMAttributeName(attributeName), value);
        }
        
        // if we're sync'ing to PR
        if (addToPR && cfg.isPRSyncAttribute(attributeName) && this.disablePRFeed == false) {
          String urn = URNLookup.getUrn(attributeName);
          PersonRegistryAttribute prAttr = new PersonRegistryAttribute(urn);
          DBAttribute attributeClass = personRegistryHelper.getDBAttribute(urn);
          if (attributeClass.isSingleValue()) {
            prAttr.addValueToAdd(addAttribute.getStringValueArray()[0]);
          } else {
            for (int j = 0; j < addAttribute.getStringValueArray().length; j++) {
              prAttr.addValueToAdd(addAttribute.getStringValueArray()[j]);
            }
          }

          attrsForPR.put(urn, prAttr);
        }
      }
      
      // loop through attributes to see if they involve any business logic.
      for (int i = 0; i < addAttributes.length; i++) {
        LDAPAttribute addAttribute = addAttributes[i];
        String attributeName = addAttribute.getName();
        
        if (cfg.isLogicAttribute(attributeName)) {
          Iterator<Logic> allLogicIter = cfg.getLogicClass(attributeName).iterator();
          while (allLogicIter.hasNext()) {
            Logic logic = allLogicIter.next();
            logic.doSomething(attrsForOIM, attrsForPR, attributeName, addAttribute
                .getStringValueArray(), LDAPModification.ADD, moUserUtility, context,
                personRegistryHelper, record.getDN());
          }
        }
      }
       
      if (addToOIM) {
        LOG.info("Attributes used to create OIM user: ");
        Iterator<String> iter = attrsForOIM.keySet().iterator();
        while (iter.hasNext()) {
          String key = iter.next();
          LOG.info(key + " - " + attrsForOIM.get(key));
        }
        moUserUtility.createUser(attrsForOIM);
        LOG.info("User created in OIM.");
      }
      

      if (addToPR && this.disablePRFeed == false) {
        LOG.info("Attributes used to create the Person Registry user: ");
        Iterator<String> iter = attrsForPR.keySet().iterator();
        while (iter.hasNext()) {
          String key = iter.next();
          PersonRegistryAttribute prAttr = attrsForPR.get(key);
  
          Iterator<String> iter2 = prAttr.getValuesToAdd().iterator();
          while (iter2.hasNext()) {
            String value = iter2.next();
            LOG.info(key + " - add value - " + value);
          }
        }
        
        personRegistryHelper.createUser(attrsForPR);
        LOG.info("User created in the Person Registry.");
      }
      
      LOG.info("Create finished.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void resync(String dn) {
    LOG.info("RESYNC: Starting resync on " + dn);
    String ldapKey = OIMAPIWrapper.getLDAPKeyFromDN(dn);

    try {   
      // get the ldap entry
      SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0,
          null, false, false);
      NamingEnumeration<SearchResult> results = context.search("dc=duke,dc=edu",
          "(duLDAPKey=" + ldapKey + ")", cons);
      
      if (!results.hasMoreElements()) {
        throw new Exception("Cannot find entry in LDAP with DN: " + dn);
      }
      
      SearchResult entry = results.next();
      Attributes attributes = entry.getAttributes();
      
      // first if the entry doesn't exist in OIM, let's create it.
      boolean addToPR = false;
      boolean addToOIM = false;
      
      tcResultSet moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, dn);
      if (moResultSet.getRowCount() == 0) {
        addToOIM = true;
      }
      
      if (this.disablePRFeed == false) {
        long prid = personRegistryHelper.getPRIDFromLDAPKey(ldapKey);
        if (prid < 0) {
          addToPR = true;
        }
      }
            
      if (addToOIM || addToPR) {
        LOG.info("RESYNC: User does not exist in OIM and/or the Person Registry.  Creating user first.");
        LDAPAttribute[] attrsForLDIF = new LDAPAttribute[2];
        attrsForLDIF[0] = new LDAPAttribute("duLDAPKey", (String)attributes.get("duLDAPKey").get());
        attrsForLDIF[1] = new LDAPAttribute("duDukeID", (String)attributes.get("duDukeID").get());
        LDIFRecord record = new LDIFRecord(dn, new LDIFAddContent(attrsForLDIF));
        addUser(record, addToOIM, addToPR);
        moResultSet = OIMAPIWrapper.findOIMUser(moUserUtility, dn);
        LOG.info("RESYNC: The user is now in OIM and the Person Registry if the Person Registry feed is enabled.");
      }
      
      LOG.info("RESYNC: Updating all attributes for DN: " + dn);
      
      // now we'll update all the attributes
      LDIFModifyContent content = new LDIFModifyContent();
      NamingEnumeration<? extends Attribute> em = attributes.getAll();
      while (em.hasMoreElements()) {
        Attribute attr = em.nextElement();
        LDAPAttribute attrForLDIF = new LDAPAttribute(attr.getID());

        NamingEnumeration<String> em2 = (NamingEnumeration<String>) attr.getAll();
        while (em2.hasMoreElements()) {
          Object value = em2.next();
          
          if (value instanceof String) {
            attrForLDIF.addValue((String)value);
          } else if (value instanceof byte[]) {
            attrForLDIF.addValue((byte[])value);
          } else {
            throw new Exception("RESYNC: Failed to resync dn " + dn + " -- Received an unknown attribute type.");
          }
        }
        LDAPModification modForLDIF = new LDAPModification(LDAPModification.REPLACE, attrForLDIF);
        content.addElement(modForLDIF);
      }
      LDIFRecord record = new LDIFRecord(dn, content);
      updateUser(record);
      LOG.info("RESYNC: Completed for DN: " + dn);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
