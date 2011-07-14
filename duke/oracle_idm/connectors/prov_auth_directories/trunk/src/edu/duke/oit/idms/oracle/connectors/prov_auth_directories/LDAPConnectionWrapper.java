package edu.duke.oit.idms.oracle.connectors.prov_auth_directories;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.directory.Attribute;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.AttributeInUseException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.NoSuchAttributeException;
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
    
    SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      resourceMap.put("IT Resources.Name", "AUTHDIR_PROVISIONING");
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
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }
  
  /**
   * Get an LDAP entry
   * @param attrName 
   * @param attrValue 
   * @return SearchResult
   */
  private SearchResult findEntryByAttr(String attrName, String attrValue) {
    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search("dc=duke,dc=edu", "(" + attrName + "=" + attrValue + ")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search("dc=duke,dc=edu", "(" + attrName + "=" + attrValue + ")", cons);
      }
      if (results.hasMoreElements()) {
        return results.next();
      } else {
        return null;
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Get child entries
   * @param dn
   * @param attrs
   * @return NamingEnumeration
   */
  private NamingEnumeration<SearchResult> findChildEntriesByDn(String dn, String[] attrs) {
    SearchControls cons = new SearchControls(SearchControls.ONELEVEL_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
      return results;
    } catch (NameNotFoundException e) {
      return null;
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * Get an LDAP entry
   * @param dn
   * @param attrs
   * @return SearchResult
   */
  private SearchResult findEntryByDn(String dn, String[] attrs) {
    SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
      } catch (NameNotFoundException e) {
        return null;
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
      }
      if (results.hasMoreElements()) {
        return results.next();
      } else {
        return null;
      }
    } catch (NameNotFoundException e) {
      return null;
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
  protected void replaceAttributes(String duLDAPKey, String entryType, Attributes modAttrs) {
    
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
    
    SearchResult result = findEntryByAttr("duLDAPKey", duLDAPKey);
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
    SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Reconnected to LDAP.");
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
   * @param duLDAPKey
   * @param groups 
   */
  protected void deleteEntryByLDAPKey(String duLDAPKey, Set<String> groups) {
    SearchResult result = findEntryByAttr("duLDAPKey", duLDAPKey);
    
    if (result != null) {
      deleteEntry(result, groups);
    }
  }
  
  /**
   * @param uid
   * @param groups 
   */
  protected void deleteEntryByNetID(String uid, Set<String> groups) {
    SearchResult result = findEntryByAttr("uid", uid);
    
    if (result != null) {
      deleteEntry(result, groups);
    }
  }
  
  /**
   * @param result
   * @param groups
   */
  private void deleteEntry(SearchResult result, Set<String> groups) {
    String dn = result.getNameInNamespace();

    removeFromAllGroups(dn, groups);

    try {
      context.newInstance(null).destroySubcontext(dn);
    } catch (NameNotFoundException e) {
      // just ignore if the entry isn't in ldap
    } catch (NamingException e) {
      throw new RuntimeException("Failed while deleting from LDAP: " + e.getMessage(), e);
    }
  }
  
  /**
   * @param result
   */
  private void deleteEntry(String dn) {
    try {
      try {
        context.newInstance(null).destroySubcontext(dn);
        SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Deleted entry " + dn);
      } catch (NameNotFoundException e) {
        // if the entry doesn't exist, just ignore
      } catch (NamingException e) {
        // try reconnecting
        reconnect();
        context.newInstance(null).destroySubcontext(dn);
        SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Deleted entry " + dn);
      }
    } catch (NameNotFoundException e) {
      // if the entry doesn't exist, just ignore
    } catch (NamingException e) {
      throw new RuntimeException("Failed while deleting from LDAP: " + e.getMessage(), e);
    }
  }

  /**
   * @param uid
   * @param entryType
   * @param attributes
   * @param groups
   */
  protected void createEntry(String uid, String entryType, Attributes attributes, Set<String> groups) {
    String container = null;
    if (entryType.equals("people")) {
      container = "ou=people,dc=duke,dc=edu";
    } else if (entryType.equals("accounts")) {
      container = "ou=accounts,dc=duke,dc=edu";
    } else if (entryType.equals("test")){
      container = "ou=people,ou=test,dc=duke,dc=edu";
    } else {
      throw new RuntimeException("unexpected entry type: " + entryType);
    }
    
    // add the object classes
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("person");
    objectClassAttr.add("organizationalPerson");
    objectClassAttr.add("inetOrgPerson");
    objectClassAttr.add("duPerson");
    objectClassAttr.add("eduPerson");
    attributes.put(objectClassAttr);
    
    // cn must have a value
    Attribute cn = attributes.get("cn");
    if (cn == null) {
      cn = new BasicAttribute("cn");
      cn.add("BOGUS");
      attributes.put(cn);
    }
    
    try {
      String dn = "uid=" + uid + "," + container;
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
      
      // now add groups
      Iterator<String> iter = groups.iterator();
      while (iter.hasNext()) {
        String groupDn = getLDAPGroupDnFromGroupName(iter.next());
        
        Attributes modAttrs = new BasicAttributes();
        Attribute modAttr = new BasicAttribute("member");
        modAttr.add(dn);
        modAttrs.put(modAttr);
        context.newInstance(null).modifyAttributes(groupDn, LdapContext.ADD_ATTRIBUTE, modAttrs);
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with uid=" + uid + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param dn
   * @param ou
   */
  private void createOu(String dn, String ou) {

    // add the object classes
    Attributes attributes = new BasicAttributes();
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("organizationalUnit");
    attributes.put(objectClassAttr);
    
    // add the ou attribute
    Attribute ouAttr = new BasicAttribute("ou");
    ouAttr.add(ou);
    attributes.put(ouAttr);
    
    try {
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
      SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Created entry " + dn);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with DN: " + dn + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param dn
   * @param cn
   */
  private void createGroup(String dn, String cn) {

    // add the object classes
    Attributes attributes = new BasicAttributes();
    Attribute objectClassAttr = new BasicAttribute("objectClass");
    objectClassAttr.add("top");
    objectClassAttr.add("groupOfNames");
    attributes.put(objectClassAttr);
    
    // add the cn attribute
    Attribute cnAttr = new BasicAttribute("cn");
    cnAttr.add(cn);
    attributes.put(cnAttr);
    
    try {
      context.newInstance(null).createSubcontext(new CompositeName().add(dn), attributes);
      SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Created group " + dn);
    } catch (NameAlreadyBoundException e) {
      // if the group already exists, just ignore
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP entry with DN: " + dn + ": " + e.getMessage(), e);    
    }
  }
  
  /**
   * @param oldDn
   * @param newDn
   */
  private void renameEntry(String oldDn, String newDn) {
    if (findEntryByDn(oldDn, new String[0]) == null && findEntryByDn(newDn, new String[0]) != null) {
      // apparently this was already renamed...
      return;
    }
    
    try {
      context.newInstance(null).rename(oldDn, newDn);
      SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Renamed entry " + oldDn + " to " + newDn);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while renaming LDAP entry with DN: " + oldDn + ": " + e.getMessage(), e);    
    }
  }
  
  /**
   * We're deleting based on groups in OIM just to be sure that if a group is being added
   * while this is running, then an error will occur.  Then just to be safe, we make sure
   * the user isn't in any other groups.
   * 
   * @param dn
   * @param groups
   */
  private void removeFromAllGroups(String dn, Set<String> groups) {

    try {
      
      if (groups != null) {
        // delete user from groups.  
        // we're intentionally letting this throw an exception if the user isn't in the group in ldap
        // since that might indicate a timing issue...
        Iterator<String> iter = groups.iterator();
        while (iter.hasNext()) {
          String groupDn = getLDAPGroupDnFromGroupName(iter.next());
          
          Attributes modAttrs = new BasicAttributes();
          Attribute modAttr = new BasicAttribute("member");
          modAttr.add(dn);
          modAttrs.put(modAttr);
          context.newInstance(null).modifyAttributes(groupDn, LdapContext.REMOVE_ATTRIBUTE, modAttrs);
        }
      }
      
      // in case we missed any memberships...
      SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0], false, false);
      NamingEnumeration<SearchResult> results = context.newInstance(null).search("ou=groups,dc=duke,dc=edu", "(member=" + dn + ")", cons);
      while (results.hasMoreElements()) {
        SearchResult result = results.next();
        Attributes modAttrs = new BasicAttributes();
        Attribute modAttr = new BasicAttribute("member");
        modAttr.add(dn);
        modAttrs.put(modAttr);
        context.newInstance(null).modifyAttributes(result.getNameInNamespace(), LdapContext.REMOVE_ATTRIBUTE, modAttrs);
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while removing user from groups: " + e.getMessage(), e);
    }
  }
  
  
  /**
   * @param groupName
   * @return string
   */
  private static String getLDAPGroupDnFromGroupName(String groupName) {
    
    String[] parts = groupName.split(":");
    String dn = "cn=" + parts[parts.length - 1];

    for (int i = parts.length - 2; i >= 0; i--) {
      dn += ",ou=" + parts[i];
    }
    
    dn += ",ou=groups,dc=duke,dc=edu";
    
    return dn;
  }
  
  /**
   * @param stemName
   * @return string
   */
  private static String getLDAPOuDnFromStemName(String stemName) {
    
    String[] parts = stemName.split(":");
    String dn = "ou=" + parts[parts.length - 1];

    for (int i = parts.length - 2; i >= 0; i--) {
      dn += ",ou=" + parts[i];
    }
    
    dn += ",ou=groups,dc=duke,dc=edu";
    
    return dn;
  }

  /**
   * @param groupName
   * @param dukeid
   */
  protected void addMember(String groupName, String dukeid) {
    String groupDn = getLDAPGroupDnFromGroupName(groupName);
    SearchResult result = findEntryByAttr("duDukeID", dukeid);
    
    if (result == null) {
      return;
    }
    
    String userDn = result.getNameInNamespace();
    
    Attributes modAttrs = new BasicAttributes();
    Attribute modAttr = new BasicAttribute("member");
    modAttr.add(userDn);
    modAttrs.put(modAttr);
    
    try {
      context.newInstance(null).modifyAttributes(groupDn, LdapContext.ADD_ATTRIBUTE, modAttrs);
      SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Added member " + dukeid + " to " + groupName);
    } catch (AttributeInUseException e) {
      // if the person is a member of the group, just ignore;
    } catch (NamingException e) {
      throw new RuntimeException("Failed while adding member " + dukeid + " to group " + groupName + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param groupName
   * @param dukeid
   */
  protected void deleteMember(String groupName, String dukeid) {
    String groupDn = getLDAPGroupDnFromGroupName(groupName);
    SearchResult result = findEntryByAttr("duDukeID", dukeid);
    
    if (result == null) {
      return;
    }
    
    String userDn = result.getNameInNamespace();
    
    Attributes modAttrs = new BasicAttributes();
    Attribute modAttr = new BasicAttribute("member");
    modAttr.add(userDn);
    modAttrs.put(modAttr);
    
    try {
      context.newInstance(null).modifyAttributes(groupDn, LdapContext.REMOVE_ATTRIBUTE, modAttrs);
      SimpleProvisioning.logger.info(AuthDirectoriesProvisioning.connectorName + ": Deleted member " + dukeid + " from " + groupName);
    } catch (NoSuchAttributeException e) {
      // if the person is not a member of the group, just ignore
    } catch (NamingException e) {
      throw new RuntimeException("Failed while deleting member " + dukeid + " from group " + groupName + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param groupName
   */
  protected void createGroup(String groupName) {
    String[] parts = groupName.split(":");

    // first create OUs
    createOusForGroup(groupName);
    
    // okay now that we have the OUs created, we'll create the group.
    String dn = getLDAPGroupDnFromGroupName(groupName);
    createGroup(dn, parts[parts.length - 1]);
  }
  
  private void createOusForGroup(String groupName) {
    String[] parts = groupName.split(":");
    
    // we're going to work through the stems from the deepest  
    // OU backwards to find out which OUs need to be created.
    int stemExists = -1;
    for (int i = parts.length - 2; i >= 0; i--) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }

      // check if this stem exists in ldap
      String dn = getLDAPOuDnFromStemName(stemName);
      SearchResult result = findEntryByDn(dn, new String[0]);
      
      if (result != null) {
        stemExists = i;
        break;
      }
    }
    
    if (stemExists == -1) {
      throw new RuntimeException("unexpected");
    }
    

    // now we'll create some stems...
    for (int i = stemExists + 1; i < parts.length - 1; i++) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }
      
      // create OU in ldap
      String dn = getLDAPOuDnFromStemName(stemName);
      String ou = parts[i];
      createOu(dn, ou);
    }
  }
  
  /**
   * @param groupName
   */
  protected void deleteGroup(String groupName) {
    
    // delete the group
    deleteEntry(getLDAPGroupDnFromGroupName(groupName));
    
    // now delete empty OUs
    deleteEmptyOus(groupName);
  }
  
  /**
   * @param groupName
   */
  private void deleteEmptyOus(String groupName) {
    String[] parts = groupName.split(":");
    
    // we're going to work through the stems from the deepest  
    // OU backwards to find out which OUs need to be deleted.
    for (int i = parts.length - 2; i >= 1; i--) {
      // recalculate current stem
      String stemName = "";
      for (int j = 0; j <= i; j++) {
        if (stemName.equals("")) {
          stemName = parts[j];
        } else {
          stemName += ":" + parts[j];
        }
      }

      // check if this stem has children
      String dn = getLDAPOuDnFromStemName(stemName);
      NamingEnumeration<SearchResult> results = findChildEntriesByDn(dn, new String[0]);
      if (results == null) {
        continue;
      } else if (results.hasMoreElements()) {
        return;
      } else {
        deleteEntry(dn);
      }
    }
  }
  
  /**
   * @param oldGroupName
   * @param newGroupName
   */
  protected void renameGroup(String oldGroupName, String newGroupName) {
    
    // add OUs for new location
    createOusForGroup(newGroupName);
    
    // rename entry
    renameEntry(getLDAPGroupDnFromGroupName(oldGroupName), getLDAPGroupDnFromGroupName(newGroupName));
    
    // delete OUs for old location
    deleteEmptyOus(oldGroupName);
  }
}
