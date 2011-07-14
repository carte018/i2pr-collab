package edu.duke.oit.idms.oracle.connectors.prov_edir;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.Rdn;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;

/**
 *  @author liz, with mucho code stolen from shilen's VOICE_AD and ServiceDir providers
 */
public class LDAPConnectionWrapper {

  private static LDAPConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;
  private LdapContext context = null;
  private String peopleContainer = "OU=People,DC=duke,DC=edu";

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
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
      resourceMap.put("IT Resources.Name", "EDIR_PROVISIONING");
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
   * Update LDAP attributes for a given entry.
   * @param dn
   * @param modAttrs
   */
  public void replaceAttributes(String dn, Attributes modAttrs) {

    if (modAttrs.size() == 0) {
      return;
    }
    
    try {
      context.newInstance(null).modifyAttributes(new CompositeName().add(dn), LdapContext.REPLACE_ATTRIBUTE, modAttrs);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while updating LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Create a new people entry.
   * @param ldapKey 
   * @param cn
   * @param attributes 
   */

  public void createEntry(String ldapKey, String cn, Attributes attributes) {

    try {
      String dn = "duLDAPKey=" + Rdn.escapeValue(cn) + "," + peopleContainer;     
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * Add an object class to an entry.
   * @param dn
   * @param objectClass
   */
  public void checkAndAddObjectClass(String dn, String objectClass) {
     
    SearchResult result = findEntryByDN(dn, new String[] {"objectClass"});
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
          SimpleProvisioning.logger.warn(EDirectoryProvisioning.connectorName + ": Failed while adding objectClass " + objectClass + " to LDAP: ",e);
          throw new RuntimeException("Failed while adding objectClass " + objectClass + " to LDAP: " + e.getMessage(), e);
        }
      }
    } else {
      SimpleProvisioning.logger.warn(EDirectoryProvisioning.connectorName + 
          ": No results found in checkAndAddObjectClass() for DN: " + dn);
    }
  }
 
  /**
   * Get an LDAP entry
   * @param ldapKey
   * @return SearchResult
   */
  public SearchResult findEntry(String ldapKey) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + ldapKey + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + ldapKey + ")", cons);
      }
      if (results.hasMoreElements()) {
        return (SearchResult) results.next();
      } else {
        return null;
      }
    } catch (NamingException e) {
      SimpleProvisioning.logger.warn(EDirectoryProvisioning.connectorName + ": Error - can't find in EDIR:"+ldapKey,e);
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Get an LDAP entry's object classes
   * @param dn
   * @return SearchResult
   */
  private SearchResult findEntryByDN(String dn, String[] attrs) {
    SearchControls cons = new SearchControls();
    cons.setReturningAttributes(attrs);
    cons.setSearchScope(SearchControls.OBJECT_SCOPE);

    NamingEnumeration<? extends SearchResult> results = null;
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
      SimpleProvisioning.logger.warn(EDirectoryProvisioning.connectorName + ": Error - can't find:"+dn,e);
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
    SimpleProvisioning.logger.info(EDirectoryProvisioning.connectorName + ": Reconnected to LDAP.");
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
