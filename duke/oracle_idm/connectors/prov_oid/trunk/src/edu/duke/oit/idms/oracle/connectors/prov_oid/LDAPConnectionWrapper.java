package edu.duke.oit.idms.oracle.connectors.prov_oid;

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
 * @author rob
 */

public class LDAPConnectionWrapper {
		
		private static LDAPConnectionWrapper instance = null;
		
		private tcDataProvider dataProvider = null;
		
		private LdapContext context = null;
		
		private String[] requiredAttributes = { "cn", "sn", "givenName", "displayName", "employeenumber", "uid" };
		
		private LDAPConnectionWrapper(tcDataProvider dataProvider)
		{
			this.dataProvider = dataProvider;
			this.context = createConnection();
			
			SimpleProvisioning.logger.info(OIDProvisioning.connectorName + "LDAPConnectWrapper instance created" );
			
		}
		/**
		 * @param dataProvider
		 * @return new instance of the LDAPConnectionWrapper class
		 */
		
		public static LDAPConnectionWrapper getInstance(tcDataProvider dataProvider)
		{
			if (instance == null) {
				instance = new LDAPConnectionWrapper(dataProvider);
			} else {
				instance.dataProvider = dataProvider;
			}
			return instance;
		}
		/**
		 * Use properties defined in IT Resource in OIM to connet to the OID
		 * and produce a new connection
		 * @return ldap context
		 */
		private LdapContext createConnection() {
			tcITResourceInstanceOperationsIntf moITResourceUtility = null;
			
			try {
				moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory.
					getUtility(dataProvider,"Thor.API.Operations.tcITResourceInstanceOperationsIntf");
				
				Map parameters = new HashMap();
				Map resourceMap = new HashMap();
				resourceMap.put("IT Resources.Name","OID_PROVISIONING");
				tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
				long resourceKey = moResultSet.getLongValue("IT Resources.Key");
				
				moResultSet = null;
				moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
				
				for (int i = 0; i < moResultSet.getRowCount(); i++) {
					moResultSet.goToRow(i);
					String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
					String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
					parameters.put(name, value);
				}
				
				Hashtable environment = new Hashtable();
				environment.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
				environment.put(Context.PROVIDER_URL,parameters.get("providerURL"));
				environment.put(Context.SECURITY_AUTHENTICATION, "simple");
				environment.put(Context.SECURITY_PRINCIPAL, parameters.get("userDN"));
				environment.put(Context.SECURITY_CREDENTIALS,parameters.get("userPassword"));
				// environment.put(Context.SECURITY_PROTOCOL, "ssl");
				return new InitialLdapContext(environment, null);
			} catch (Exception e) {
				throw new RuntimeException("Failed creating LDAP connect: " + e.getMessage(), e);
			} finally {
				if (moITResourceUtility != null) {
					moITResourceUtility.close();
				}
			}
		}
		/**
		 * Return an LDAP entry
		 * @param dn
		 * @param attrs
		 * @return SearchResult
		 */
		public SearchResult findEntry(String dn, String[] attrs) {
			SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs, false, false);
			NamingEnumeration results = null;
			
			try {
				try {
					results = context.newInstance(null).search(dn, "(ObjectClass=*)", cons);
				} catch (NamingException e) {
					// Maybe we just got disconnected 
					reconnect();
					results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
				}
				if (results.hasMoreElements()) {
					return (SearchResult) results.next();
				} else {
					return null;
				}
			} catch (NamingException e) {
				throw new RuntimeException("Failed querying LDAP for " + dn + " with " + e.getMessage(), e);
			}
		}
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
		 * Return an LDAP entry
		 * @param uniqueID
		 * @param attrs
		 * @return SearchResult
		 */
		public SearchResult findEntryByUniqueID(String uniqueID, String[] attrs) {
			SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs, false, false);
			SearchControls initial = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, requiredAttributes, false, false);
			NamingEnumeration results = null;
			NamingEnumeration<SearchResult> iresults = null;
			String dn = null;
			
			try {
				try {
					iresults = context.newInstance(null).search("cn=users,dc=oit,dc=duke,dc=edu","(employeenumber=" + uniqueID + ")",initial);
				} catch (NamingException e) {
					// Try reconnecting
					reconnect();
					iresults = context.newInstance(null).search("cn=users,dc=oit,dc=duke,dc=edu","(employeenumber=" + uniqueID + ")",initial);
				}
				if (iresults.hasMoreElements()) {
					SearchResult resval = iresults.next();
					Attributes retval = resval.getAttributes();
					String uidValue = (String) retval.get("uid").get();
					dn = "cn=" + uidValue + ",cn=users,dc=oit,dc=duke,dc=edu";
				} else {
					return null;
				}
			} catch (NamingException e) {
				throw new RuntimeException("Failed querying OID for user with uniqueID " + uniqueID + " message: " + e.getMessage(),e);
			}
			
			try {
				try {
					results = context.newInstance(null).search(dn, "(ObjectClass=*)", cons);
				} catch (NamingException e) {
					// Maybe we just got disconnected 
					reconnect();
					results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
				}
				if (results.hasMoreElements()) {
					return (SearchResult) results.next();
				} else {
					return null;
				}
			} catch (NamingException e) {
				throw new RuntimeException("Failed querying LDAP for " + dn + " with " + e.getMessage(), e);
			}
		}
		public SearchResult findEntryByUniqueIDUsingCN(String uniqueID, String[] attrs) {
			SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs, false, false);
			SearchControls initial = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, requiredAttributes, false, false);
			NamingEnumeration results = null;
			NamingEnumeration<SearchResult> iresults = null;
			String dn = null;
			
			try {
				try {
					iresults = context.newInstance(null).search("cn=users,dc=oit,dc=duke,dc=edu","(employeenumber=" + uniqueID + ")",initial);
				} catch (NamingException e) {
					// Try reconnecting
					reconnect();
					iresults = context.newInstance(null).search("cn=users,dc=oit,dc=duke,dc=edu","(employeenumber=" + uniqueID + ")",initial);
				}
				if (iresults.hasMoreElements()) {
					SearchResult resval = iresults.next();
					Attributes retval = resval.getAttributes();
					String uidValue = (String) retval.get("cn").get();
					dn = "cn=" + uidValue + ",cn=users,dc=oit,dc=duke,dc=edu";
				} else {
					throw new RuntimeException("Failed to find uid value for user with unique id " + uniqueID);
				}
			} catch (NamingException e) {
				throw new RuntimeException("Failed querying OID for user with uniqueID " + uniqueID + " message: " + e.getMessage(),e);
			}
			
			try {
				try {
					results = context.newInstance(null).search(dn, "(ObjectClass=*)", cons);
				} catch (NamingException e) {
					// Maybe we just got disconnected 
					reconnect();
					results = context.newInstance(null).search(dn, "(objectClass=*)", cons);
				}
				if (results.hasMoreElements()) {
					return (SearchResult) results.next();
				} else {
					return null;
				}
			} catch (NamingException e) {
				throw new RuntimeException("Failed querying LDAP for " + dn + " with " + e.getMessage(), e);
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
		public void createEntry(String cn, Attributes attributes) {
			try {
				context.newInstance(null).createSubcontext("cn="+cn+",cn=users,dc=oit,dc=duke,dc=edu",attributes);
			} catch (NamingException e) {
				throw new RuntimeException("Failed cerating new LDAP entry for cn="+cn+" because of " + e.getMessage(),e);
			}
		}
		
		/**
		 * Add a new objectclass to an entry
		 * It's unclear if we have to actually use this in the OID case,
		 * but for now, it's likely that we will.
		 * @param uid
		 * @param entryType
		 * @param objectClass
		 */
		public void checkAndAddObjectClass(String uid,String entryType,String objectClass) {
			String dn = getDn(uid,entryType);
			
			SearchResult result = findEntry(dn, new String[] {"objectClass"});
			if (result != null) {
				boolean check = result.getAttributes().get("objectClass").contains(objectClass);
				if (!check) {
					Attributes modAttrs = new BasicAttributes();
					Attribute addAttr = new BasicAttribute("objectClass");
					addAttr.add(objectClass);
					modAttrs.put(addAttr);
					
					try {
						context.newInstance(null).modifyAttributes(dn, LdapContext.ADD_ATTRIBUTE,modAttrs);
					} catch (NamingException e) {
						throw new RuntimeException("Failed while adding objectClass " + objectClass + "to OID with " + e.getMessage(), e);
					}
				}
			} else {
				SimpleProvisioning.logger.warn(OIDProvisioning.connectorName + ": No results found in checkAndAddObjectClass() for DN: " + dn);
			}
		}
		
		/**
		 * Given a uid value, get a DN to go with it.
		 */
		private String getDn(String uid, String entryType) {
			String dn = null;
			//
			// We really don't do anything meaningful in the OID with types,
			// but for the sake of maintaining the interface, we push an 
			// entryType around in this connector anyway.
			//
			dn = "cn=" + uid + ",cn=users,dc=oit,dc=duke,dc=edu";
			return dn;
		}
		
		/**
		 * Reconnect to the LDAP
		 */
		private void reconnect() {
			try {
				context.close();
			} catch (NamingException e) {
				// OK if we're already dropped
			}
			this.context = createConnection();
			SimpleProvisioning.logger.info(OIDProvisioning.connectorName + "Reconnected to LDAP");
		}
		
		/**
		 * Clean up connection as w get garbage collected
		 */
		protected void finalize() throws Throwable {
			if (context != null) {
				try {
					context.close();
				} catch (NamingException e) {
					// No issue if we're already gone
				}
			}
			super.finalize();  // Walk up the tree
		}
}