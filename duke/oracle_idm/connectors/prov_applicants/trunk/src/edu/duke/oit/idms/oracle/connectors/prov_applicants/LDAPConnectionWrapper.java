package edu.duke.oit.idms.oracle.connectors.prov_applicants;

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
    
    SimpleProvisioning.logger.info(ApplicantsProvisioning.connectorName + ": Created new instance of LDAPConnectionWrapper.");
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
      resourceMap.put("IT Resources.Name", "APPLICANT_PROVISIONING");
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
   * Rename an LDAP entry
   * @param oldDN
   * @param newDN
   */
  
  public void rename(String oldDN,String newDN) {
      try {
              try {
                      context.newInstance(null).rename(oldDN,newDN);
              } catch (NamingException e) {
                      // Possible we just got disconnected
                      reconnect();
                      context.newInstance(null).rename(oldDN,newDN);
              }
      } catch (NamingException e) {
              throw new RuntimeException("Failed performing modDN operation from " + oldDN + " to " + newDN + " due to " + e.getMessage(),e); 
      }
}

  
  /**
   * Get an LDAP entry
   * @param dn
   * @param attrs
   * @return SearchResult
   */
  public SearchResult findEntryByEmplID(String emplID, String[] attrs) {

    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs,
        false, false);
    NamingEnumeration results = null;
    try {
      try {
        results = context.newInstance(null).search("ou=external", "(dupsemplid="+emplID+")", cons);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.newInstance(null).search("ou=external", "(dupsemplid="+emplID+")", cons);
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
   * Replace LDAP attributes for an entry
   * If we need to do an add or a remove, we first recompute the set
   * of attributes needed and then perform a full replace.  JNDI is 
   * someone's friend, but not really mine.
   * Since we're operating in the OID, we use the uid as our key
   * @param uid
   * @param entryType
   * @param modAttrs
   */
  public void addOrReplaceAttributes(String uid,String entryType, Attributes modAttrs) {
          //
          // Take care not to clear out required attributes.
          // If a required attribute is in the list and there's no value,
          // remove it from the list
          for (int i = 0; i < requiredAttributes.length; i++) {
                  String requiredAttribute = requiredAttributes[i];
                  Attribute attr = modAttrs.get(requiredAttribute);
                  if (attr != null && attr.size() == 0) {
                          modAttrs.remove(requiredAttribute);
                  }
          }
          // Now the attribute list is clean...
          // It may be empty, though...
          if (modAttrs.size() == 0) {
                  return;
          }
          
          // Otherwise, we move on
          
          String dn = getDn(uid,entryType);
          try {
                  try {
                          context.newInstance(null).modifyAttributes(dn,LdapContext.ADD_ATTRIBUTE, modAttrs);
                  } catch (NamingException e) {
                          // reconnect and retry
                          reconnect();
                          context.newInstance(null).modifyAttributes(dn,LdapContext.ADD_ATTRIBUTE,modAttrs);
                  } 
          } catch (NamingException e) {
                          // Ignore -- we'll try the modify route first
          }
          
          try {
                  try {
                          context.newInstance(null).modifyAttributes(dn,LdapContext.REPLACE_ATTRIBUTE, modAttrs);
          
                  } catch (NamingException e) {
                          // reconnect and retry
                          reconnect();
                          context.newInstance(null).modifyAttributes(dn,LdapContext.REPLACE_ATTRIBUTE,modAttrs);
                  }
          } catch (NamingException e) {
                  throw new RuntimeException("Failed while updating LDAP: "+ e.getMessage(), e);
          }
  }
  
  /**
   * Add a new entry to the OID.
   * Does no checking of incoming data.
   * @param cn
   * @param attributes
   */
  public void createEntry(String uid, Attributes attributes) {
          try {
        	  try{
                  context.newInstance(null).createSubcontext(getDn(uid,null),attributes);
          } catch (NamingException e) {
        	  	  reconnect();
        	  	  context.newInstance(null).createSubcontext(getDn(uid,null),attributes);
          } 
          } catch (NamingException e) {
                  throw new RuntimeException("Failed creating new LDAP entry for uid="+uid+" because of " + e.getMessage(),
e);
          }
  }
  
  
  /**
   * Update LDAP attributes for a given entry.
   * @param duLDAPKey
   * @param entryType
   * @param modAttrs
   */
  public void replaceAttributes(String uid, String entryType, Attributes modAttrs) {
    
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
    
    String dn = getDn(uid, entryType);

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
      SimpleProvisioning.logger.warn(ApplicantsProvisioning.connectorName + 
          ": No results found in checkAndAddObjectClass() for DN: " + dn);
    }
  }
  
  protected String getDn(String uid, String entryType) {
    String dn = null;
    
    // No options for entryType here at the moment -- that may change later
    // for now, entryType defaults and we always use ou=applicants, ou=external
    
    dn = "ou=applicants,ou=external";

    dn = "uid=" + uid + "," + dn;

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
    SimpleProvisioning.logger.info(ApplicantsProvisioning.connectorName + ": Reconnected to LDAP.");
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
