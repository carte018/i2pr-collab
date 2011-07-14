package edu.duke.oit.idms.oracle.connectors.prov_voice_ad;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
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
 * @author shilen
 */
public class LDAPConnectionWrapper {

  private static LDAPConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  private LdapContext context = null;
  
  private String peopleContainer = null;

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
    
    SimpleProvisioning.logger.info(VoiceADProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      resourceMap.put("IT Resources.Name", "VOICEAD_PROVISIONING");
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
      
      peopleContainer = (String)parameters.get("peopleContainer");

      Hashtable<String, String> environment = new Hashtable<String, String>();
      environment
          .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
      environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      environment.put("java.naming.ldap.factory.socket", edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());
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
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Get an LDAP entry by CN
   * @param cn
   * @return SearchResult
   */
  public SearchResult findEntryByCN(String cn) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      // perform the search.... escape the CN if needed.
      results = context.newInstance(null).search(peopleContainer, "(cn=" + escapeLDAPFilter(cn) + ")", cons);
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
   * Create a new entry.
   * @param ldapKey 
   * @param cn
   * @param attributes 
   */
  public void createEntry(String ldapKey, String cn, Attributes attributes) {

    try {
      renameIfConflict(ldapKey, cn);
      String dn = "CN=" + Rdn.escapeValue(cn) + "," + peopleContainer;
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * Rename an entry.
   * @param ldapKey 
   * @param oldCn
   * @param newCn
   */
  public void renameEntry(String ldapKey, String oldCn, String newCn) {

    // oldCn has to be unescaped since it's escaped already.
    if (newCn.equalsIgnoreCase((String)Rdn.unescapeValue(oldCn))) {
      return;
    }
    
    // build the DN for the old and new names
    String oldDn = "CN=" + oldCn + "," + peopleContainer;
    String newDn = "CN=" + Rdn.escapeValue(newCn) + "," + peopleContainer;
    
    try {
      renameIfConflict(ldapKey, newCn);
      VoiceADProvisioning.logger.info(VoiceADProvisioning.connectorName + ": Renaming " + oldDn + " to " + newDn);
      context.newInstance(null).rename(new CompositeName().add(oldDn), new CompositeName().add(newDn));
    } catch (NamingException e) {
      throw new RuntimeException("Failed while renaming LDAP entry when processing entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param ldapKey
   * @param cn
   * @throws NamingException 
   */
  private void renameIfConflict(String ldapKey, String cn) {
    // if the new name is already taken, rename that entry by prepending "conflict-"
    if (findEntryByCN(cn) != null) {
      try {
        String newCnForConflict = "conflict-" + cn;
        String oldDnForConflict = "CN=" + Rdn.escapeValue(cn) + "," + peopleContainer;
        String newDnForConflict = "CN=" + Rdn.escapeValue(newCnForConflict) + "," + peopleContainer;
        
        // well if the conflict name also exists, we'll prepend "conflict-" once again...
        // but we'll only do this mess twice.  if more is needed, there might be something else wrong...
        if (findEntryByCN(newCnForConflict) != null) {
          String newCnForConflictConflict = "conflict-" + newCnForConflict;
          String oldDnForConflictConflict = "CN=" + Rdn.escapeValue(newCnForConflict) + "," + peopleContainer;
          String newDnForConflictConflict = "CN=" + Rdn.escapeValue(newCnForConflictConflict) + "," + peopleContainer;

          VoiceADProvisioning.logger.info(VoiceADProvisioning.connectorName + ": Renaming " + oldDnForConflictConflict + " to " + newDnForConflictConflict);
          context.newInstance(null).rename(new CompositeName().add(oldDnForConflictConflict), new CompositeName().add(newDnForConflictConflict));
        }
        
        VoiceADProvisioning.logger.info(VoiceADProvisioning.connectorName + ": Renaming " + oldDnForConflict + " to " + newDnForConflict);
        context.newInstance(null).rename(new CompositeName().add(oldDnForConflict), new CompositeName().add(newDnForConflict));
      } catch (NamingException e) {
        throw new RuntimeException("Failed while renaming conflict when processing LDAP entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
      }
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
    SimpleProvisioning.logger.info(VoiceADProvisioning.connectorName + ": Reconnected to LDAP.");
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
  
  /**
   * Code copied from URNFilter...
   * escape the expression.  replaces the following characters with their escape codes:
   *
   * - *
   * - (
   * - )
   * - \
   * - NUL
   *
   * @param value
   * @return
   */
  protected String escapeLDAPFilter(String value) {
    StringBuffer escaped = new StringBuffer();
    char[] valueBytes = value.toCharArray();
    for (int i = 0; i < valueBytes.length; i++) {
      switch (valueBytes[i]) {
        case '*':
          if (i - 1 >= 0 && valueBytes[i - 1] == '\\')
            escaped.append("\\2a");
          else
            escaped.append("*");
          break;

        // \ is special:  append it ONLY if the next character is a \.
        // otherwise, do nothing.
        case '\\':
          if (i + 1 < valueBytes.length && valueBytes[i + 1] == '\\')
            escaped.append("\\5c");
          break;

        case '(':
          escaped.append("\\28");
          break;

        case ')':
          escaped.append("\\29");
          break;

        case '\0':
          escaped.append("\\00");
          break;

        default:
          escaped.append(valueBytes[i]);
      }
    }

    return escaped.toString();
  }
}
