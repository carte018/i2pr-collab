package edu.duke.oit.idms.oracle.connectors.tasks_email.reporting;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcITResourceNotFoundException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;


/**
 * @author shilen
 */
public class LDAPConnectionWrapper {
	
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private static LDAPConnectionWrapper instance = null;
	private tcITResourceInstanceOperationsIntf moITResourceUtility = null;
	private LdapContext context = null; 
	private String peopleContainer = "ou=people,dc=duke,dc=edu";
	private String[] attrs = { "duStudentCareerHistory" };
  
	private LDAPConnectionWrapper(String connectorName, tcITResourceInstanceOperationsIntf moITResourceUtility) {
		this.moITResourceUtility = moITResourceUtility;
		try {
			this.context = createConnection(connectorName);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Created new instance of LDAPConnectionWrapper."));
	}

	/**
	* @param dataProvider 
	* @return new instance of this class
	*/
	public static LDAPConnectionWrapper getInstance(String connectorName, tcITResourceInstanceOperationsIntf moITResourceUtility) {
		if (instance == null) {
			instance = new LDAPConnectionWrapper(connectorName, moITResourceUtility);
		} else {
			instance.moITResourceUtility = moITResourceUtility;
		}
	
		return instance;
	}

	/**
	* Create a new connection to LDAP based on properties defined in the IT Resource.
	* @return ldap context
	* @throws tcAPIException 
	* @throws tcColumnNotFoundException 
	* @throws tcITResourceNotFoundException 
	* @throws NamingException 
	*/
	private LdapContext createConnection(String connectorName) throws Exception {

		Map<String, String> parameters = new HashMap<String, String>();
		Map<String, String> resourceMap = new HashMap<String, String>();
		resourceMap.put("IT Resources.Name", "SVCDIR_PROVISIONING");
		tcResultSet moResultSet;
		LdapContext cxt = null;
		Hashtable<String, String> environment = new Hashtable<String, String>();
		try {
			moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
			long resourceKey = moResultSet.getLongValue("IT Resources.Key");
			moResultSet = null;
			moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
			for (int i = 0; i < moResultSet.getRowCount(); i++) {
				moResultSet.goToRow(i);
				String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
				String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
				parameters.put(name, value);
			}

			environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			environment.put(Context.PROVIDER_URL, parameters.get("providerUrl"));
			environment.put(Context.SECURITY_AUTHENTICATION, "simple");
			environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
			environment.put(Context.SECURITY_CREDENTIALS, parameters.get("userPassword"));
			environment.put(Context.SECURITY_PROTOCOL, "ssl");
			environment.put("java.naming.ldap.factory.socket", edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());
			cxt = new InitialLdapContext(environment, null);
	
		} catch (Exception e) {
			String exceptionName = e.getClass().getSimpleName();
			if (exceptionName.contains("tcAPIException") || exceptionName.contains("tcColumnNotFoundException") || exceptionName.contains("tcITResourceNotFoundException")) {
				throw new RuntimeException(EmailUsers.addLogEntry(connectorName, "ERROR", "Could not retrieve LDAP connection information from OIM."), e);
			} else if (exceptionName.contains("NamingException")) {
				throw new RuntimeException(EmailUsers.addLogEntry(connectorName, "ERROR", "Could not instantiate LdapContext for new LDAP connection."), e);
			} else {
				logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Unhandled exception caught: " + e.getClass().getCanonicalName() + ":" + e.getMessage()));
			}
		}
		return cxt;
	}
  
	/**
	* Get an LDAP entry by DN
	* @param duLDAPKey 
	* @return SearchResult
	* @throws tcITResourceNotFoundException 
	* @throws tcColumnNotFoundException 
	* @throws tcAPIException 
	* @throws NamingException 
	*/
	public SearchResult findEntryByLDAPKey(String connectorName, String duLDAPKey) {
	    SearchControls cons = new SearchControls();
	    cons.setReturningAttributes(attrs);
	    cons.setSearchScope(SearchControls.ONELEVEL_SCOPE);

	    NamingEnumeration<SearchResult> results = null;
	    try {
	      try {
	        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + duLDAPKey + ")", cons);
	      } catch (NamingException e) {
	        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
	        try {
				reconnect(connectorName);
			} catch (Exception e1) {
				throw new RuntimeException(e1.getMessage(), e1);
			}
	        results = context.newInstance(null).search(peopleContainer, "(duLDAPKey=" + duLDAPKey + ")", cons);
	      }
	      if (results.hasMoreElements()) {
	        return (SearchResult) results.next();
	      } else {
	        return null;
	      }
	    } catch (NamingException e) {
	    	logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", "Could not retrieve entry for duLDAPKey: " + duLDAPKey), e);
	    	return null;
	    }
	  }
  
	/**
	 * Reconnect to ldap
	 * @throws NamingException 
	 * @throws tcITResourceNotFoundException 
	 * @throws tcColumnNotFoundException 
	 * @throws tcAPIException 
	 */
	private void reconnect(String connectorName) throws Exception {
		try {
			context.close();
		} catch (NamingException e) {
			// this is okay
		}

		try {
			this.context = createConnection(connectorName);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Reconnected to LDAP."));
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
