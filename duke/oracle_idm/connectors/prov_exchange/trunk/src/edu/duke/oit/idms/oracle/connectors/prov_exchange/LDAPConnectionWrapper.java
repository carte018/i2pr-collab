package edu.duke.oit.idms.oracle.connectors.prov_exchange;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

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
  
  private String peopleContainer = null;
  
  private String activeDirectoryHostname = null;

  private String[] attrs = { "proxyAddresses", "sAMAccountName" };
  
  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
    
    SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      resourceMap.put("IT Resources.Name", "WINAD_PROVISIONING");
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
      environment.put("java.naming.ldap.factory.socket", edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());

      peopleContainer = parameters.get("peopleContainer");
      activeDirectoryHostname = parameters.get("hostname");
      return new InitialLdapContext(environment, null);
    } catch (Exception e) {
      SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Received Exception", e);
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }
  
  /**
   * @return active directory hostname
   */
  public String getActiveDirectoryHostname() {
    return this.activeDirectoryHostname;
  }
  
  /**
   * Get an LDAP entry
   * @param duLDAPKey 
   * @return SearchResult
   */
  public SearchResult findEntryByLDAPKey(String duLDAPKey) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + duLDAPKey + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + duLDAPKey + ")", cons);
      }
      if (results.hasMoreElements()) {
        return results.next();
      } else {
        return null;
      }
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Received Exception", e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Update LDAP attributes for a given entry.
   * @param duLDAPKey
   * @param entryType
   * @param modAttrs
   */
  protected void replaceAttributes(String duLDAPKey, Attributes modAttrs) {
    
    SearchResult result = findEntryByLDAPKey(duLDAPKey);
    if (result != null) {
      try {
        context.newInstance(null).modifyAttributes(result.getNameInNamespace(), LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      } catch (NamingException e) {
        SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Received Exception", e);
        throw new RuntimeException("Failed while updating LDAP: " + e.getMessage(), e);
      }
    } else {
      throw new RuntimeException("Entry doesn't exist for " + duLDAPKey);
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
    SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Reconnected to LDAP.");
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
