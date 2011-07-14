package edu.duke.oit.idms.oracle.connectors.prov_comms_directories;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.directory.Attribute;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.NameNotFoundException;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;

/**
 * @author shilen
 */
public class LDAPConnectionWrapper {

  private static LDAPConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  private LdapContext context = null;

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();

    SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static LDAPConnectionWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new LDAPConnectionWrapper(dataProvider);
    } else {
      instance.dataProvider = dataProvider;
    }

    return instance;
  }

  /**
   * Create a new connection to LDAP based on properties defined in the IT Resource.
   * @return ldap context
   */
  private LdapContext createConnection() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
      .getUtility(dataProvider,
          "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map<String, String> parameters = new HashMap<String, String>();
      Map<String, String> resourceMap = new HashMap<String, String>();
      resourceMap.put("IT Resources.Name", "COMMSDIR_PROVISIONING");
      tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
      long resourceKey = moResultSet.getLongValue("IT Resources.Key");

      moResultSet = null;
      moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
        String value = moResultSet
        .getStringValue("IT Resources Type Parameter Value.Value");
        parameters.put(name, value);
      }

      Hashtable<String, String> environment = new Hashtable<String, String>();
      environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
      environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      environment.put("java.naming.ldap.attributes.binary", "mailSieveRuleString");  // return mailSieveRuleString as byte []
      return new InitialLdapContext(environment, null);
    } catch (Exception e) { 	
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while creating LDAP connection: " + e.getMessage(), e);
      throw new RuntimeException(CommsDirectoriesProvisioning.connectorName + ": Failed while creating LDAP connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }

  /**
   * Get an LDAP entry
   * @param dn
   * @param attrs
   * @return SearchResult
   */
  public SearchResult findEntry(String dn, String[] attrs) {
    SearchControls cons = new SearchControls();
    cons.setReturningAttributes(attrs);
    cons.setSearchScope(SearchControls.OBJECT_SCOPE);

    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
      }
      if (results.hasMoreElements()) {
        return (SearchResult) results.next();
      } else {
        return null;
      }
    } catch (NameNotFoundException ne) {
      // entry not found, code 32 - No Such Object
      return null;
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while querying LDAP: " + e.getMessage(), e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }

  /**
   * Get an LDAP entry by uid - useful for finding one or more entries by uid
   * @param uid
   * @param attrs
   * @return SearchResult
   */
  public NamingEnumeration<SearchResult> findEntryByUid(String uid, String[] attrs) {
    SearchControls cons = new SearchControls();
    cons.setReturningAttributes(attrs);
    cons.setSearchScope(SearchControls.ONELEVEL_SCOPE);
    String baseDn = "o=comms,dc=duke,dc=edu";

    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(baseDn, "(uid=" + uid + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(baseDn, "(uid=" + uid + ")", cons);
      }

      if (results.hasMoreElements()) {
        return results;
      } else {
        return null;
      }
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while querying LDAP: " + e.getMessage(), e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }

  /**
   * Update LDAP attributes for a given entry.
   * @param duLDAPKey
   * @param entryType
   * @param modAttrs
   */
  public void replaceAttributes(String dn, String entryType, Attributes modAttrs) {
    CommsDirectoriesProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ":replaceAttributes");


    if (modAttrs.size() == 0) {
      return;
    }

    try {
      try {
        context.newInstance(null).modifyAttributes(dn, LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      } catch (NamingException e) {
        // let's try reconnecting and then updating again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        context.newInstance(null).modifyAttributes(dn, LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      }
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while updating LDAP: " + e.getMessage(), e);
      throw new RuntimeException("Failed while updating LDAP: " + e.getMessage(), e);
    }
  }

  /**
   * Add a new entry to the OID.
   * Does no checking of incoming data.
   * @param dn
   * @param attributes
   */
  public void createEntry(String dn, Attributes attributes) {
    CommsDirectoriesProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ":createEntry (attributes)");

    try {
      context.newInstance(null).createSubcontext(new CompositeName().add(dn),attributes);
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed creating new LDAP entry for dn=" + dn + " because of " + e.getMessage(),e);
      throw new RuntimeException("Failed creating new LDAP entry for dn=" + dn + " because of " + e.getMessage(),e);
    }
  }

  /**
   * Add an object class to an entry.
   * @param dn
   * @param objectClass
   */
  public void checkAndAddObjectClass(String dn, String objectClass) {

    SearchResult result = findEntry(dn, new String[] {"objectClass"});
    if (result != null) {
      boolean check = result.getAttributes().get("objectClass").contains(objectClass);
      if (!check) {
        Attributes modAttrs = new BasicAttributes();
        Attribute addAttr = new BasicAttribute("objectClass");
        addAttr.add(objectClass);
        modAttrs.put(addAttr);

        try {
          context.newInstance(null).modifyAttributes(dn, LdapContext.ADD_ATTRIBUTE, modAttrs);
        } catch (NamingException e) {
          SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while adding objectClass " + objectClass +
              " to LDAP: " + e.getMessage(),e);
          throw new RuntimeException("Failed while adding objectClass " + objectClass + " to LDAP: " + e.getMessage(), e);
        }
      }
    } else {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + 
          ": No results found in checkAndAddObjectClass() for DN: " + dn);
    }
  }

  public void deleteEntry(String dn) {

    try {
      // what checks do are needed?
      // search first?
      context.newInstance(null).destroySubcontext(new CompositeName().add(dn));
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Failed while querying LDAP: " + e.getMessage(),e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }

  /**
   * Reconnect to ldap
   */
  private void reconnect() {
    try {
      context.close();
    } catch (NamingException e) {
      // this is okay
    }

    this.context = createConnection();
    SimpleProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + ": Reconnected to LDAP.");
  }

  /**
   * Close ldap connection when this class is about to be garbage collected.
   */
  protected void finalize() throws Throwable {
    if (context != null) {
      try {
        context.close();
      } catch (NamingException e) {
        // this is okay
      }
    }
    super.finalize();
  }
}
