package edu.duke.oit.idms.oracle.connectors.prov_service_directories;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.directory.Attribute;
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
  
  private String[] requiredAttributes = { "cn", "sn", "givenName", "displayName" };

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
    
    SimpleProvisioning.logger.info(ServiceDirectoriesProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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

      Map parameters = new HashMap();
      Map resourceMap = new HashMap();
      resourceMap.put("IT Resources.Name", "SVCDIR_PROVISIONING");
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

      Hashtable environment = new Hashtable();
      environment
          .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
      environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      return new InitialLdapContext(environment, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
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
    SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration results = null;
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
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
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
    
    String dn = getDn(duLDAPKey, entryType);

    try {
      try {
        context.newInstance(null).modifyAttributes(dn, LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      } catch (NamingException e) {
        // let's try reconnecting and then updating again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        context.newInstance(null).modifyAttributes(dn, LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while updating LDAP: " + e.getMessage(), e);
    }
  }
  

  /**
   * Add an object class to an entry.
   * @param duLDAPKey
   * @param entryType
   * @param objectClass
   */
  public void checkAndAddObjectClass(String duLDAPKey, String entryType, String objectClass) {
    String dn = getDn(duLDAPKey, entryType);
    
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
          throw new RuntimeException("Failed while adding objectClass " + objectClass + " to LDAP: " + e.getMessage(), e);
        }
      }
    } else {
      SimpleProvisioning.logger.warn(ServiceDirectoriesProvisioning.connectorName + 
          ": No results found in checkAndAddObjectClass() for DN: " + dn);
    }
  }
  
  private String getDn(String duLDAPKey, String entryType) {
    String dn = null;
    if (entryType.equals("people")) {
      dn = "ou=people,dc=duke,dc=edu";
    } else if (entryType.equals("accounts")) {
      dn = "ou=accounts,dc=duke,dc=edu";
    } else {
      dn = "ou=people,ou=test,dc=duke,dc=edu";
    }

    dn = "duLDAPKey=" + duLDAPKey + "," + dn;

    return dn;
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
    SimpleProvisioning.logger.info(ServiceDirectoriesProvisioning.connectorName + ": Reconnected to LDAP.");
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
