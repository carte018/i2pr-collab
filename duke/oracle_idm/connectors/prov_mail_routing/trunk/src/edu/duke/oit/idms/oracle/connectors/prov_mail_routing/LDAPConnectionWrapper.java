package edu.duke.oit.idms.oracle.connectors.prov_mail_routing;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
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
  
  private String[] attrs = { "mailDrop", "mailAcceptingGeneralId", "mailDropOverride" };
  
  private String[] requiredAttributes = { "cn", "sn", "givenName" };

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
    
    SimpleProvisioning.logger.info(MailRoutingProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      resourceMap.put("IT Resources.Name", "MAIL_ROUTING_PROVISIONING");
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

      return new InitialLdapContext(environment, null);
    } catch (Exception e) {
      SimpleProvisioning.logger.info(MailRoutingProvisioning.connectorName + ": Received Exception", e);
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }
  
  /**
   * Get an LDAP entry
   * @param duLDAPKey 
   * @param baseDn
   * @return SearchResult
   */
  protected NamingEnumeration<SearchResult> findEntriesByLDAPKey(String duLDAPKey, String baseDn) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(baseDn, "(duLDAPKey=" + duLDAPKey + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(baseDn, "(duLDAPKey=" + duLDAPKey + ")", cons);
      }

      return results;
    } catch (NamingException e) {
      SimpleProvisioning.logger.info(MailRoutingProvisioning.connectorName + ": Received Exception", e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  protected SearchResult findEntryByLDAPKey(String duLDAPKey, String baseDn) {
    NamingEnumeration<SearchResult> results = findEntriesByLDAPKey(duLDAPKey, baseDn);
    if (results.hasMoreElements()) {
      return results.nextElement();
    }
    
    return null;
  }
  
  /**
   * Delete entries
   * @param duLDAPKey 
   */
  protected void deleteEntriesByLDAPKey(String duLDAPKey) {
    NamingEnumeration<SearchResult> results = findEntriesByLDAPKey(duLDAPKey, "dc=duke,dc=edu");
    
    while (results.hasMoreElements()) {
      SearchResult result = results.nextElement();
      String dn = result.getNameInNamespace();

      try {
        context.newInstance(null).destroySubcontext(dn);
      } catch (NamingException e) {
        throw new RuntimeException("Failed while deleting from LDAP: " + e.getMessage(), e);
      } 
    }
  }
  
  /**
   * @param duLDAPKey
   * @param entryType
   * @param attributes
   */
  protected void createEntry(String duLDAPKey, String entryType, Attributes attributes) {

    String container = getBaseDnByEntryType(entryType);

    // add the object classes
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("person");
    objectClassAttr.add("organizationalPerson");
    objectClassAttr.add("inetOrgPerson");
    objectClassAttr.add("duPerson");
    objectClassAttr.add("mailRouting");
    attributes.put(objectClassAttr);

    // cn must have a value
    Attribute cn = attributes.get("cn");
    if (cn == null) {
      cn = new BasicAttribute("cn");
      cn.add("BOGUS");
      attributes.put(cn);
    }
   
    try {
      String dn = "duLDAPKey=" + duLDAPKey + "," + container;
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with duLDAPKey=" + duLDAPKey + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * Update LDAP attributes for a given entry.
   * @param duLDAPKey
   * @param entryType
   * @param modAttrs
   */
  public void replaceAttributes(String duLDAPKey, String entryType, Attributes modAttrs) {

    // we want to make sure not to clear out the required attributes.
    for (int i = 0; i < requiredAttributes.length; i++) {
      String requiredAttribute = requiredAttributes[i];
      Attribute attr = modAttrs.get(requiredAttribute);
      if (attr != null && attr.size() == 0) {
        modAttrs.remove(requiredAttribute);
      }
    }

    if (modAttrs.size() == 0) {
      return;
    }

    SearchResult result = findEntryByLDAPKey(duLDAPKey, getBaseDnByEntryType(entryType));
    if (result != null) {
      try {
        context.newInstance(null).modifyAttributes(result.getNameInNamespace(), LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      } catch (NamingException e) {
        throw new RuntimeException("Failed while updating LDAP: " + e.getMessage(), e);
      }
    } else {
      throw new RuntimeException("Entry doesn't exist for " + duLDAPKey);
    }
  }
  
  protected String getBaseDnByEntryType(String entryType) {
    String baseDn = null;
    if (entryType.equals("people")) {
      baseDn = "ou=people,dc=duke,dc=edu";
    } else if (entryType.equals("accounts")) {
      baseDn = "ou=accounts,dc=duke,dc=edu";
    } else if (entryType.equals("test")){
      baseDn = "ou=people,ou=test,dc=duke,dc=edu";
    } else {
      throw new RuntimeException("unexpected entry type: " + entryType);
    } 
    
    return baseDn;
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
    SimpleProvisioning.logger.info(MailRoutingProvisioning.connectorName + ": Reconnected to LDAP.");
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
