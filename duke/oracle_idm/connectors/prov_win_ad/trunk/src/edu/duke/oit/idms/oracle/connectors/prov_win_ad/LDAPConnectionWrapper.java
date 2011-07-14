package edu.duke.oit.idms.oracle.connectors.prov_win_ad;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Iterator;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.ldap.Control;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.Rdn;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.oracle.ssl.*;

/**
 * @author shilen
 */
public class LDAPConnectionWrapper {

  private static LDAPConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;

  private LdapContext context = null;
  
  private String peopleContainer = null;
  
  private String rootContainer = null;

  private LDAPConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    this.context = createConnection();
    
    SimpleProvisioning.logger.info(WinADProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      
      peopleContainer = (String)parameters.get("peopleContainer");
      rootContainer = (String)parameters.get("rootContainer");

      Hashtable<String, String> environment = new Hashtable<String, String>();
      environment
          .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
      environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      
      // Ignore SSL certificate issues with an overridden socket factory
      environment.put("java.naming.ldap.factory.socket",edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());
            
      // RGC
      Control[] adctrls = new Control[] {new ADControl()};
      //return new InitialLdapContext(environment, null);
      InitialLdapContext ILC = null;
      try {
      ILC = new InitialLdapContext(environment,null);
      } catch (Exception e) {
    	  SimpleProvisioning.logger.info(WinADProvisioning.connectorName + " Failed during initial context - " + e.getMessage());
    	  e.printStackTrace(System.out);
    	  throw new RuntimeException(e);
      }
      ILC.setRequestControls(adctrls);
      return ILC;
      //return new InitialLdapContext(environment, adctrls);
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
	  String[] lattrs = {"sAMAccountName","givenName","sn","userAccountControl"};
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, /*new String[0]*/ lattrs,
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
   * Get an LDAP entry for a conflicting uid
   * @param uid
   * @return SearchResult
   */
  public SearchResult findEntryFromRootByUid(String uid) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(rootContainer, "(sAMAccountName=" + uid + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(rootContainer, "(SAMAccountName=" + uid + ")", cons);
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
   * Get an LDAP entry for a conflicting uid from the peopleContainer
   * @param uid
   * @return SearchResult
   */
  public SearchResult findEntryFromPeopleByUid(String uid) {
	  String[] lattrs = {"sAMAccountName","givenName","sn","duLDAPKey","dudukeid"};

    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, /*new String[0]*/ lattrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(peopleContainer, "(sAMAccountName=" + uid + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(peopleContainer, "(SAMAccountName=" + uid + ")", cons);
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
    WinADProvisioning.logger.info(WinADProvisioning.connectorName + "Replacing attrs for DN = " + dn + " now");
    NamingEnumeration n = modAttrs.getAll();
    try{
    while (n.hasMore()) {
    	Attribute a = (Attribute) n.next();
    	WinADProvisioning.logger.info(WinADProvisioning.connectorName + " including " + a.getID() + " set to " + a.get());
    }
    } catch (Exception e) {
    	// nothing to do 
    	// RGC DEBUG
    	WinADProvisioning.logger.info(WinADProvisioning.connectorName + " Exception thrown " + e.getMessage());
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
      //String dn = "CN=" + Rdn.escapeValue(cn) + "," + peopleContainer;
      String dn = "CN=" + Rdn.escapeValue(cn) + "," + getOuSuffix(ldapKey, attributes);
      WinADProvisioning.logger.info(WinADProvisioning.connectorName + "About to try create");
      WinADProvisioning.logger.info(WinADProvisioning.connectorName + "Trying to create entry with dn of " + dn);
      NamingEnumeration it = attributes.getAll();
      while (it.hasMore()) {
    	  WinADProvisioning.logger.info(WinADProvisioning.connectorName + "Creating with attribute " + (String) ((Attribute)it.next()).getID());
      }
      WinADProvisioning.logger.info(WinADProvisioning.connectorName + "Attribute list complete");
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
    }
  }
  
  public String getOuSuffix(String ldapKey, Attributes attributes) {
	  // Default for the moment
	  // return("ou=users," + peopleContainer);
	  // Now the real mess
	  // We first must retrieve the user's duFunctionalGroup value from OIM
	  // If the user has 0 for USR_UDF_HAS_FUNCTIONALGROUP we assume there is no
	  // group assigned and we slot the user at the top of the OU tree.  If the user
	  // has 1 for USR_UDF_HAS_FUNCTIONALGROUP we assume there is a decided upon 
	  // gorup and if the group is empty, we slot the user at the top of the OU tree
	  // anyway.  Eventually we may choose to return differently in those two cases.
	  // 
	  // First, acquire the user in OIM
	  try {
	  tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf)tcUtilityFactory.getUtility(dataProvider, "Thor.API.Operations.tcUserOperationsIntf");
	  Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
	  mhSearchCriteria.put("USR_UDF_LDAPKEY",ldapKey);
	  
	  String [] moAttrs = new String [3];
	  moAttrs[0] = "USR_UDF_HAS_FUNCTIONALGROUP";
	  moAttrs[1] = "USR_UDF_FUNCTIONALGROUP";
	  moAttrs[2] = "USR_UDF_EPPA";
	  
	  tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,moAttrs);
	  if (moResultSet.getRowCount() != 1) {
		  throw new RuntimeException("Did not find exactly one user matching ldap key " + ldapKey + " in OIM");
	  }
	  
	  // Check for alumni-only status by looking at primary affiliation.
	  // We short-circuit alumni into their own location in order to keep them from 
	  // muddying the top tier of the OU tree.  Alums get moved into ou=users,ou=alums.
	  if (moResultSet.getStringValue("USR_UDF_EPPA") != null && moResultSet.getStringValue("USR_UDF_HAS_FUNCTIONALGROUP").equalsIgnoreCase("1") && moResultSet.getStringValue("USR_UDF_EPPA").equalsIgnoreCase("alumni")) {
		  WinADProvisioning.logger.info("Returning ou=users,ou=alums," + peopleContainer + " from getOuSuffix");
		  return("ou=users,ou=alums," + peopleContainer);
	  }
	  
	  // Now we have the user in moResultSet with the two attributes we need
	  //
	  // If the user has 0 in has_functionalgroup, slot him at the top.
	  if (moResultSet.getStringValue("USR_UDF_HAS_FUNCTIONALGROUP").equalsIgnoreCase("0")) {
		  WinADProvisioning.logger.info("Returning ou=users," + peopleContainer + " from getOuSuffix");
		  return("ou=users," + peopleContainer);
	  }
	  
	  // If the user has 1 however, we need to convert...
	  String func = moResultSet.getStringValue("USR_UDF_FUNCTIONALGROUP");
	  if (func == null || func.equalsIgnoreCase("")) {
		  // Same as above -- no group, so slot at the top
		  WinADProvisioning.logger.info("No value in functionalgroup so returning ou=users," + peopleContainer + " from getOuSuffix");
		  return("ou=users," + peopleContainer);
	  }
	  // Otherwise, compute
	  String targetOU = "ou=Users,ou=" + func;
	  targetOU = targetOU.replaceAll(":",",ou=");
	  targetOU = targetOU + "," + peopleContainer;
	  WinADProvisioning.logger.info("Returning " + targetOU + " from getOuSuffix");
	  return(targetOU);
	  } catch (Exception e) {
		  throw new RuntimeException(e.getMessage(),e);
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
    //String oldDn = "CN=" + oldCn + "," + peopleContainer;
    //String newDn = "CN=" + Rdn.escapeValue(newCn) + "," + peopleContainer;
    // DN suffixes are no longer so predictable.
    // We need to find the DN for the oldCN value and compute it for the new one.
    //
    SearchResult foundEntry = findEntryByCN(oldCn);
    if (foundEntry == null) {
    	foundEntry = findEntry(ldapKey);
    	if (foundEntry == null) {
    	WinADProvisioning.logger.info("Failed to find oldCn in directory during rename operation");
    	throw new RuntimeException("Failed to find oldCn in directory during rename operation");
    	}
    }
    String oldDn = foundEntry.getNameInNamespace();
    String newDn = "CN=" + newCn + "," + getOuSuffix(ldapKey,null);  // Attributes passed are irrelevant now
    WinADProvisioning.logger.info("Trying to rename " + oldDn + " to " + newDn);
    try {
      renameIfConflict(ldapKey, newCn);
      WinADProvisioning.logger.info(WinADProvisioning.connectorName + ": Renaming " + oldDn + " to " + newDn);
      context.newInstance(null).rename(new CompositeName().add(oldDn), new CompositeName().add(newDn));
    } catch (NamingException e) {
      throw new RuntimeException("Failed while renaming LDAP entry when processing entry with duLDAPKey=" + ldapKey + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * Rename an entry.
   * @param ldapKey 
   * @param oldCn
   * @param newCn
   */
  public void renameEntryForOU(String ldapKey, String oldCn, String newCn) {
    
    // build the DN for the old and new names
    //String oldDn = "CN=" + oldCn + "," + peopleContainer;
    //String newDn = "CN=" + Rdn.escapeValue(newCn) + "," + peopleContainer;
    // DN suffixes are no longer so predictable.
    // We need to find the DN for the oldCN value and compute it for the new one.
    //
	  WinADProvisioning.logger.info(WinADProvisioning.connectorName + " Using oldCN value of " + oldCn);
    SearchResult foundEntry = findEntryByCN(oldCn);
    if (foundEntry == null) {
    	foundEntry = findEntry(ldapKey);
    	if (foundEntry == null) {
    	WinADProvisioning.logger.info(WinADProvisioning.connectorName + " Failed to find oldCn in directory during rename operation - cn " + oldCn);
    	throw new RuntimeException("Failed to find oldCn in directory during rename operation");
    	}
    }
    String oldDn = foundEntry.getNameInNamespace();
    String newDn = "CN=" + newCn + "," + getOuSuffix(ldapKey,null);  // Attributes passed are irrelevant now
    WinADProvisioning.logger.info("Trying to rename " + oldDn + " to " + newDn);
    try {
      WinADProvisioning.logger.info(WinADProvisioning.connectorName + ": Renaming " + oldDn + " to " + newDn);
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
        //String oldDnForConflict = "CN=" + Rdn.escapeValue(cn) + "," + peopleContainer;
        //String newDnForConflict = "CN=" + Rdn.escapeValue(newCnForConflict) + "," + peopleContainer;
        // This becomes more complicated now, as well
        String oldDnForConflict = findEntryByCN(cn).getNameInNamespace();
        String newDnForConflict = "CN=" + Rdn.escapeValue(newCnForConflict) + "," + getOuSuffix(ldapKey,null);
        WinADProvisioning.logger.info("Conflict renaming " + oldDnForConflict + " to " + newDnForConflict);
        
        // well if the conflict name also exists, we'll prepend "conflict-" once again...
        // but we'll only do this mess twice.  if more is needed, there might be something else wrong...
        if (findEntryByCN(newCnForConflict) != null) {
          String newCnForConflictConflict = "conflict-" + newCnForConflict;
          //String oldDnForConflictConflict = "CN=" + Rdn.escapeValue(newCnForConflict) + "," + peopleContainer;
          //String newDnForConflictConflict = "CN=" + Rdn.escapeValue(newCnForConflictConflict) + "," + peopleContainer;
          // Complexity strikes here, as well...
          String oldDnForConflictConflict = findEntryByCN(newCnForConflict).getNameInNamespace();
          String newDnForConflictConflict = "CN=" + Rdn.escapeValue(newCnForConflictConflict) + "," + getOuSuffix(ldapKey,null);
         
          WinADProvisioning.logger.info(WinADProvisioning.connectorName + ": Renaming " + oldDnForConflictConflict + " to " + newDnForConflictConflict);
          context.newInstance(null).rename(new CompositeName().add(oldDnForConflictConflict), new CompositeName().add(newDnForConflictConflict));
        }
        
        WinADProvisioning.logger.info(WinADProvisioning.connectorName + ": Renaming " + oldDnForConflict + " to " + newDnForConflict);
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
    SimpleProvisioning.logger.info(WinADProvisioning.connectorName + ": Reconnected to LDAP.");
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

class ADControl implements Control {

	  public byte[] getEncodedValue() {
	          return new byte[] {};
	  }

	  public String getID() {
	    return "1.2.840.113556.1.4.801";
	  }

	  public boolean isCritical() {
	    return true;
	  }
	}
