package edu.duke.idms.oracle.security;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.ldap.Control;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;

public class ADConnectionWrapper {

	  private static ADConnectionWrapper instance = null;

	  private LdapContext context = null;
	  	  
	  private String rootContainer = null;
	  
	  private String configContainer = null;
	  
	  private String schemaContainer = null;
	  
	  private String peopleContainer = null;
	  
	  private String ldapURL;
	  
	  private String userDN;
	  
	  private String userPassword;
	  
	  private ADConnectionWrapper(String ldapURL, String userDN, String userPassword) {
		  this.context = createConnection(ldapURL, userDN, userPassword);  
	  }
	  
	  public static ADConnectionWrapper getInstance(String ldapURL, String userDN, String userPassword) {
		  if (instance == null) {
			  instance = new ADConnectionWrapper(ldapURL,userDN,userPassword);
		  } else {
			  instance.rootContainer = "dc=SUPPRESSED,dc=SUPPRESSED,dc=SUPPRESSED";  // root of the domain
			  instance.configContainer = "cn=configuration," + instance.rootContainer;  // root of the config tree
			  instance.schemaContainer = "cn=schema,"+instance.configContainer; // root of the schema tree
			  instance.peopleContainer = "ou=DukePeople,"+instance.rootContainer;  // root of tree where real people user objects reside
			  instance.ldapURL = ldapURL;
			  instance.userDN = userDN;
			  instance.userPassword = userPassword;
		  }
		  return (instance);
	  }
	  
	  private LdapContext createConnection(String ldapURL, String userDN, String userPassword) {
		  
		  Hashtable<String, String> environment = new Hashtable<String, String>();
	      environment
	          .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
	      environment.put(Context.PROVIDER_URL, ldapURL);
	      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
	      environment.put(Context.SECURITY_PRINCIPAL, userDN);
	      environment.put(Context.SECURITY_CREDENTIALS, userPassword);
	      environment.put(Context.SECURITY_PROTOCOL, "ssl");
	      environment.put("java.naming.ldap.attributes.binary","ntSecurityDescriptor objectSID objectGUID schemaIDGUID rightsGUID");
	      
	      if (rootContainer == null) {
	    	  rootContainer = "dc=SUPPRESSED,dc=SUPPRESSED,dc=SUPPRESSED";  // default root container
	      }
	      if (configContainer == null) {
	    	  configContainer = "cn=configuration," + rootContainer;
	      }
	      if (schemaContainer == null) {	
	    	  //schemaContainer = "cn=schema,cn=configuration,dc=win,dc=duke,dc=edu";
	    	  schemaContainer = "cn=schema," + configContainer;
	      }
	      if (peopleContainer == null) {
	    	  peopleContainer = "ou=DukePeople," + rootContainer;  // default user tree root 
	      }
	      
	      // Ignore SSL certificate issues with an overridden socket factory
	      environment.put("java.naming.ldap.factory.socket",edu.duke.oit.idms.oracle.ssl.BlindSSLSocketFactory.class.getName());
	      
	      // RGC
	      Control[] adctrls = new Control[] {new ADControl()};
	      
	      InitialLdapContext ILC = null;
	      try {
	      ILC = new InitialLdapContext(environment,null);
	      ILC.setRequestControls(adctrls);
	      } catch (Exception e) {
	    	  throw new RuntimeException(e);
	      }
	      return ILC;
	  }
	  
	  public String convertSIDtoDN(MsSID sid) {
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0],
			        false, false);
		  NamingEnumeration<SearchResult> results = null;
		  String sidString = sid.toString();
		  try {
		      try {
		        results = context.newInstance(null).search(rootContainer, "(objectSID=" + sidString + ")", cons);
		      } catch (NamingException e) {
		        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
		        reconnect(ldapURL,userDN,userPassword);
		        //System.out.println("Reconnected to LDAP using " + ldapURL + "," + userDN + "," + userPassword);
		        results = context.newInstance(null).search(rootContainer, "(objectSID=" + sidString + ")", cons);
		      }
		      if (results.hasMoreElements()) {
		    	  return results.next().getNameInNamespace();
		      } else {
		        return null;
		      }
		    } catch (NamingException e) {
		      throw new RuntimeException("Failed while querying AD: " + e.getMessage(), e);
		    }
	  }
	  
	  public String convertSANtoDN(String sAMAccountName) {
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0], false, false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(rootContainer, "(samaccountname=" + sAMAccountName + ")",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search(rootContainer, "(samaccountname=" + sAMAccountName + ")",cons);
			  }
			  if (results.hasMoreElements()) {
				  return results.next().getNameInNamespace();
			  } else {
				  return null;
			  }
		  } catch (NamingException e) {
			  throw new RuntimeException("Failed while querying AD: " + e.getMessage(),e);
		  }
	  }
	  
	  public String convertNametoDN(String name) {
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, new String[0], false, false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(rootContainer,"(|(samaccountname=" + name + ")(name=" + name +"))",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search(rootContainer,"(|(samaccountname=" + name + ")(name=" + name + "))",cons);
			  }
			  if (results.hasMoreElements()) {
				  return results.next().getNameInNamespace();
			  } else {
				  return null;
			  }
		  } catch (NamingException e) {
			  throw new RuntimeException("Failed while querying AD: " + e.getMessage(),e);
		  }
	  }
			  
		  
	  
	  
	  public void replaceBinaryAttribute(String dn, String attribute, byte[] value) {
		  // Given an Attributes object replace the specified DN's attributes accordingly
		  BasicAttribute attr = new BasicAttribute(attribute);
		  attr.add(value);
		  BasicAttributes attrs = new BasicAttributes();
		  attrs.put(attr);
		  try {
			  context.newInstance(null).modifyAttributes(dn,2/*REPLACE_ATTRIBUTE*/,attrs);
		  } catch (NamingException e) {
			  throw new RuntimeException("Failed writing update to " + attribute + " for " + dn + ": " + e.getMessage(),e);
		  }
	  }
	  
	  public boolean replaceNtSecurityDescriptor(String dn, long oldVersion, byte[] value) {
		  // Given a DN, a new value, and an old version number, update the ntSD value for the dn
		  // by checking for the duADSecDescVersion attribute in the schema and if it exists, transactionally
		  // deleting the oldVersion value, adding a newVersion value that's one greater, and then pushing
		  // in the ntSecurityDescriptor value.  If no duASecDescVersion value is available, just push in 
		  // the ntSecurityDescriptor value.  I the passed in oldVersion value is negative, assume there is no 
		  // duADSecDescVersion attribute available.  If the passed in oldVersion value is 0, assume this is a 
		  // new update, and avoid the delete.  The delete will fail if the value has changed, and the ad will fail
		  // if another process has already added a value -- in either event, the transaction should roll back.
		  //
		  // Return true if we make the change and false if we fail for some reason.
		  //
		  boolean retval = false;
		  boolean interlock = true;
		  if (oldVersion == -1) {
			  interlock = false;
		  }
		  if (interlock) {
			  //System.out.println("This update will interlock");
			  long newVersion = oldVersion + 1;			  
			  BasicAttribute attr = new BasicAttribute("duADSecDescVersion");
			  if (oldVersion != 0) {
				  //System.out.println("There is a version number already");
				  ModificationItem [] items = new ModificationItem[3];
				  attr.add(Long.toString(oldVersion));
				  //System.out.println("Old version is " + Long.toString(oldVersion));
				  items[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE,attr);
				  attr.remove(oldVersion);
				  attr = new BasicAttribute("duADSecDescVersion");
				  attr.add(Long.toString(newVersion));
				  //System.out.println("New version is " + Long.toString(newVersion));
				  items[1] = new ModificationItem(DirContext.ADD_ATTRIBUTE,attr);
				  attr = new BasicAttribute("ntSecurityDescriptor");
				  attr.add(value);
				  items[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,attr);
				  try {
					  context.newInstance(null).modifyAttributes(dn,items);
				  } catch (NamingException e) {
					  System.out.println("Failed with a naming exception: " + e.getMessage());
					  return retval;
				  }
				  return true;
			  } else {
				  ModificationItem [] items = new ModificationItem[2];
				  attr.add(Long.toString(newVersion));
				  //System.out.println("New version is " + newVersion);
				  items[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE,attr);
				  attr = new BasicAttribute("ntSecurityDescriptor");
				  attr.add(value);
				  items[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,attr);
				  try {
					  context.newInstance(null).modifyAttributes(dn,items);
				  } catch (NamingException e) {
					  System.out.println("Failed with a naming exception: " + e.getMessage());
					  return retval;
				  }
				  return true;
			  }
		  } else {
			  //System.out.println("No interlocking on this update");

			  replaceBinaryAttribute(dn,"ntSecurityDescriptor",value);
			  return true;
		  }
	  }
	  
	  
	  public byte[] getSIDfromDN(String dn) {
		  // Given the DN of an object in the AD we're attached to, retrieve a byte array containing its SID
		  if (dn == null) {
			  return null;
		  }
		  SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE,0,0,new String[] {"objectSID"},false,false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL, userDN, userPassword);
				  results = context.newInstance(null).search(dn,"(objectclass=*)", cons);
			  }
			  if (results.hasMoreElements()) {
				  byte [] decodedBytes = (byte []) (results.next().getAttributes().get("objectSID").get());
				  return decodedBytes;
			  } else {
				  return null;
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed querying AD for objectSID: " + e.getMessage(),e);
		  }
	  }
	  
	  public byte[] getRightsGUIDfromName(String name) {
		  //
		  // ADS (the Active Directory) holds a collection of objects whose function is to act as containers for 
		  // collections of attributes that can be secured together, or that act as the targets for access 
		  // controls for attributes that aren't actually in the schema but are instead calculated as needed
		  // by the AD controller itself (eg., "memberOf").
		  //
		  // These objects have rightsGUID values which are used as the ObjectType values for 
		  // ACE's that refer to them as their target objects.  This routine retrieves the rGUID of one of 
		  // those objects based on its name.
		  //
		  
		  if (name == null) {
			  return null;
		  }
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,new String[] {"rightsGUID"},false,false);
		  NamingEnumeration<SearchResult>  results = null;
		  try {
			  try {
				  results = context.newInstance(null).search("cn=Extended-Rights,"+configContainer,"(name="+name+")",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search("cn=Extended-Rights,"+configContainer,"(name="+name+")",cons);
			  }
			  if (results.hasMoreElements()) {
				  byte[] decodedBytes = (byte []) (results.next().getAttributes().get("rightsGUID").get());
				  return decodedBytes;
			  } else {
				  return null;
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed querying AD for attribute security GUID for " + name);
		  }
	  }
	  
	  public byte[] getGUIDfromDN(String dn) {
		  // Given the DN of an object in the AD we're attached to, retrieve a byte array containing its SID
		  SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE,0,0,new String[] {"objectGUID"},false,false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL, userDN, userPassword);
				  results = context.newInstance(null).search(dn,"(objectclass=*)", cons);
			  }
			  if (results.hasMoreElements()) {
				  byte [] decodedBytes = (byte []) (results.next().getAttributes().get("objectGUID").get());
				  return decodedBytes;
			  } else {
				  return null;
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed querying AD for objectGUID: " + e.getMessage(),e);
		  }
	  }
	  
	  public byte[] getAttributeGUIDfromName(String name) {
		  // Given the name of an attribute, return its schemaIDGUID value
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,new String[] {"schemaIDGUID"},false,false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(schemaContainer,"(|(name="+name+")(ldapdisplayname="+name+"))",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search(schemaContainer,"(|(name="+name+")(ldapdisplayname="+name+"))",cons);
			  }
			  if (results.hasMoreElements()) {
				  byte[] decodedBytes = (byte []) (results.next().getAttributes().get("schemaIDGUID").get());
				  //System.out.println("Returning " + decodedBytes.length + " bytes from getAttributeGUIDfromName");
				  return decodedBytes;
			  } else {
				  return null;
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed retrieving attribute GUID for " + name + ": " + e.getMessage(),e);
		  }
	  }
	  
	  public long getduADSecDescVersion(String dn) {
		  // Given the dn of an object, get its duADSecDescVersion value
		  // If no value exists, return 0 to indicate that
		  // If the AD schema doesn't allow it, return -1.
		  
		  byte [] attrguid = getAttributeGUIDfromName("duADSecDescVersion");
		  if (attrguid == null) {
			  return -1;  // We don't support versioning
		  } else {
			  SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE,0,0,new String [] {"duADSecDescVersion"},false,false);
			  NamingEnumeration<SearchResult> results = null;
			  try {
				  try {
					  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
				  } catch (NamingException e) {
					  reconnect(ldapURL,userDN,userPassword);
					  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
				  }
				  BasicAttribute dadsdv = null;
				  if (results.hasMoreElements() && (dadsdv = (BasicAttribute) results.next().getAttributes().get("duADSecDescVersion")) != null) {
					  return (long) ((new Long ((String) dadsdv.get())).longValue());
				  } else {
					  return 0;
				  }
			  } catch (Exception e) {
				  throw new RuntimeException("Failed retrieving duADSecDescVersion attribute for " + dn + ": " + e.getMessage(),e);
			  }
		  }
	  }
	  
	  public byte[] getNTSecurityDescriptor(String dn) {
		  // Given the DN of an object in the AD we're attached to, return the NTSecurityDescriptor bytes
		  // Start by searching for the object and getting its NTSecurityDescriptor value, then 
		  // convert the value from base64 to binary and stuff it into the return value.
		  
		  // System.out.println("Queryinf for ntSecurityDescriptor for " + dn + " using AD connection parms " + ldapURL + "," + userDN + "," + userPassword);
		  
		  SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE,0,0,new String[] {"ntSecurityDescriptor"},false,false);
		  NamingEnumeration<SearchResult> results = null;
		  try {
			  try {
				  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL, userDN, userPassword);
				  results = context.newInstance(null).search(dn,"(objectclass=*)",cons);
			  }
			  if (results.hasMoreElements()) {
				  byte [] decodedBytes = (byte []) (results.next().getAttributes().get("ntSecurityDescriptor").get());
				  return(decodedBytes);
			  } else {
				  return(null);
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed querying AD for ntSecurityDescriptor: " + e.getMessage(),e);
		  }
	  }
	  public String convertGUIDtoDN(MsGUID guid) {
		  // Slightly more complicated since we have to check different parts of the tree for different things
		  SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,new String[0],false,false);
		  NamingEnumeration<SearchResult> results = null;
		  String guidString = guid.toString();
		  String guidHuman = guid.toHuman();
		  // First, try the GUID against normal objects in the AD
		  try {
			  try {
				  results = context.newInstance(null).search(rootContainer,"(objectGUID="+ guidString + ")",cons);
			  } catch (NamingException e) {
				  // Try reconnecting in this case
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search(rootContainer,"(objectGUID="+ guidString + ")",cons);
			  }
			  if (results.hasMoreElements()) {
				  return results.next().getNameInNamespace();
			  } 
		  } catch(NamingException e) {
			  throw new RuntimeException("Failed while querying AD: " + e.getMessage(),e);
		  }
		  // if we didn't get something there, we'll try a schema object
		  try {
			  try {
				  // DEBUG System.out.println("Trying schemaidguid="+guidString);
				  cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,new String[0],false,false);
				  results = context.newInstance(null).search(schemaContainer,"(schemaidguid=" + guidString + ")",cons);
				  if (! results.hasMoreElements()) {
					  results = context.newInstance(null).search(schemaContainer,"(objectGUID="+guidString+")",cons);
				  }
			  } catch (NamingException e) {
				  // Reconnect
				  reconnect(ldapURL,userDN,userPassword);
				  results = context.newInstance(null).search(schemaContainer,"(schemaidguid=" + guidString + ")",cons);
				  if (! results.hasMoreElements()) {
					  results = context.newInstance(null).search(schemaContainer,"(objectGUID="+guidString+")",cons);
				  }
			  }
			  if (results.hasMoreElements()) {
				  return results.next().getNameInNamespace();
			  } 
		  } catch (Exception e) {
			  throw new RuntimeException("Failed while querying AD: " + e.getMessage(),e);
		  }
		  // if we got nothing there, we'll try a rights object before bailing out
		  try {
			  try {
				 // DEBUG System.out.println("Trying rightsguid="+guidHuman);
				  cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,new String[0],false,false);
				  results = context.newInstance(null).search("CN=Extended-Rights,"+configContainer,"(rightsGUID="+guidHuman+")",cons);
			  } catch (NamingException e) {
				  reconnect(ldapURL,userDN,userPassword);
				  //results = context.newInstance(null).search("CN=Extended-Rights,cn=configuration,dc=win,dc=duke,dc=edu","(rightsGUID="+guidHuman+")",cons);
				  results = context.newInstance(null).search("CN=Extended Rights,"+configContainer,"(rightsGUID="+guidHuman+")",cons);
			  }
			  if (results.hasMoreElements()) {
				  	return results.next().getNameInNamespace();
			  } else {
				 // DEBUG System.out.println("Returning null for guid="+guidHuman+" in "+schemaContainer+" and "+ rootContainer);
				  return(null);
			  }
		  } catch (Exception e) {
			  throw new RuntimeException("Failed while querying AD: " + e.getMessage(),e);
		  }
	  }
	  
	  /**
	   * Reconnect to ldap
	   */
	  private void reconnect(String ldapURL, String userDN, String userPassword) {
	    try {
	      context.close();
	    } catch (NamingException e) {
	      // this is okay
	    }

	    this.context = createConnection(ldapURL, userDN, userPassword);
	  }

}


class ADControl implements Control {

	  private static final long serialVersionUID = 119L;
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
