package edu.duke.oit.idms.oracle.connectors.recon_edir;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import Thor.API.tcResultSet;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;


public class LDAPConnectionWrapper {

  private LdapContext context = null;
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private String baseDn = "ou=people,dc=duke,dc=edu";
  private tcITResourceInstanceOperationsIntf moITResourceUtility;
  
  public LDAPConnectionWrapper(tcITResourceInstanceOperationsIntf resUtility) {
    //BasicConfigurator.configure();
    moITResourceUtility =resUtility;
    this.context = createConnection();
  }

 
  public HashMap<String,Map<String,String>> doSearch(String filter) {
    HashMap<String,Map<String,String>> retval = new HashMap<String, Map<String,String>>();

    String[] attrs = {"duDukeID","duNotesTargetAddress", "duEmailAlias", "duEmailAliasTarget"};
    SearchControls ctls = new SearchControls();
    ctls. setReturningObjFlag (true);
    ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);  
    ctls.setReturningAttributes(attrs);

    NamingEnumeration<SearchResult> eDirUsers = null;
    try {
      try {
        eDirUsers = context.search(baseDn, filter, ctls);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        eDirUsers = context.search(baseDn, filter, ctls);
      }
      
      while (eDirUsers.hasMoreElements()) {
        SearchResult sr = eDirUsers.nextElement();
        Attributes ar = sr.getAttributes();
        String id = getAttr(ar, "duDukeID");
        Map<String, String> eDirResults = new HashMap<String, String>();
        eDirResults.put("duNotesTargetAddress", getAttr(ar, "duNotesTargetAddress"));
        eDirResults.put("duEmailAlias", getAttr(ar, "duEmailAlias"));
        eDirResults.put("duEmailAliasTarget", getAttr(ar, "duEmailAliasTarget"));
     
        retval.put(id, eDirResults);
      }
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
    return retval;
  }

  private LdapContext createConnection() {
 
    try {

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
  
  private void reconnect() {
    try {
      context.close();
    } catch (NamingException e) {
      // this is okay
    }

    this.context = createConnection();
    logger.info(": Reconnected to LDAP.");
  }
  /**
   * Utility method to correctly extract the LDAP value or set it to something usable
   */

  private String getAttr(Attributes ar, String key){
    String val = RunConnector.MISSING;
    Attribute a = ar.get(key);
    if (a != null) {
      try {
        val = (String)a.get();
      } catch (NamingException e) {
        // I don't care, val is already set to "missing" in this case
      }
    }
    return val;
  }
 

}
