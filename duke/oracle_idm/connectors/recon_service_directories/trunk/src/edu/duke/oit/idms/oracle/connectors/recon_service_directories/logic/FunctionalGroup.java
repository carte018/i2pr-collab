package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.HashSet;
import java.util.Iterator;
import javax.naming.directory.SearchResult;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.NamingException;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.ldap.Control;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.Rdn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;

import java.io.*;
import java.sql.*;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.message.*;
import org.apache.axis.encoding.ser.*;
import javax.xml.namespace.QName;
import javax.xml.rpc.ParameterMode;
import org.apache.axis.encoding.DeserializerFactory;
import org.apache.axis.encoding.Deserializer;
import org.xml.sax.SAXException;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.mc_style.functions.soap.sap.document.sap_com.*;

import javax.activation.*;
import javax.mail.*;

import edu.duke.oit.idms.oracle.ssl.*;

import javax.naming.directory.DirContext;

import Thor.API.Operations.tcUserOperationsIntf;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.util.ConnectorConfig;

import netscape.ldap.LDAPModification;



public class FunctionalGroup extends LogicBase {

    // Wrapper for a main() routine to handle determination of duFunctioanlGroup values in OIM.  To be called
    // both by an initialization routine for setting up duFunctionalGroup values in OIM and an extension to the 
    // service directories reconciler for setting duFunctionalGroup values as user departments change or new users
    // are created.

    public static void main(String [] args) {
	String DU = args[0];
	System.out.println(getFunctionalGroup(DU,null));
    }
    
    public void doSomething(Map<String, String> oimAttributes, Map<String,PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
    		int modificationType, tcUserOperationsIntf moUserUtility, DirContext context, PersonRegistryHelper personRegistryHelper, String dn) {
    	// Do something here...
 
    	String dudukeid = getAttribute(context, "dudukeid", dn);
    	if (dudukeid == null) {
    		// odd -- how do they get into the directory without that?
    		return; // just return as if nothing happened
    	}
    	// Otherwise, we have a Unique ID -- get a functional group value
    	
    	String fg = getFunctionalGroup(dudukeid, (LdapContext) context);
    	
    	//and do the appropriate thing depending on the state we're in
    	
    	if (modificationType == LDAPModification.ADD || modificationType == LDAPModification.REPLACE) {
    		// On add or replace, we simply make sure that the USR_UDF_HAS_FUNCTIONALGROUP attribute is 1
    		// and set the USR_UDF_FUNCTIONALGROUP attributein OIM.
    		oimAttributes.put("USR_UDF_HAS_FUNCTIONALGROUP", "1");
    		oimAttributes.put("USR_UDF_FUNCTIONALGROUP",fg);
    		return;
    	} else {
    		// On delete, we do something like the inverse
    		oimAttributes.put("USR_UDF_HAS_FUNCTIONALGROUP","0");
    		oimAttributes.put("USR_UDF_FUNCTIONALGROUP", "");
    		return;
    	}
    }
			   

    public static String getFunctionalGroup (String DukeUnique, LdapContext context) {

	// instantiate an LDAP connections
    	LDAPUtility ldap = null;
    	if (context == null) {
    		ldap = LDAPUtility.getInstance();
    	} else {
    		ldap = LDAPUtility.getInstance(context);
    	}
		
	// Query LDAP for attributes about the specfied user
	HashSet<SearchResult> UserSet = ldap.runQuery("(dudukeid="+DukeUnique+")","dc=duke,dc=edu",new String[] {"dudukeid","uid","eduPersonPrimaryAffiliation","duSAPOrgUnit","duSAPPositionCode","duPSAcadCareerC1","duSponsor","duPSAcadProgC1"});
	if (UserSet == null) {
		throw new RuntimeException("Unexpectedly cannot find user with unique ID " + DukeUnique + " in dc=duke,dc=edu in service directories -- how is that possible?");
	}
	// Iterate over the results that come back...

	Iterator ui = UserSet.iterator();
	while (ui.hasNext()) {
	    SearchResult res = (SearchResult) ui.next();
	    Attributes attrs = res.getAttributes();
	    if (attrs != null) {
		Attribute affila = attrs.get("eduPersonPrimaryAffiliation");
		String affilv = null;
		try {
		    affilv = affila != null ? (String) affila.get() : null;
		} catch (Exception e) {
		    // ignore
		}
		// Check for special affiliations
		if (affilv != null && affilv.equalsIgnoreCase("student")) {
		    // If we're a student, check whether we're ugrad or grad and return that value
		    //
		    Attribute typea = attrs.get("duPSAcadCareerC1");
		    String typev = null;
		    try {
			typev = typea != null ? (String) typea.get() : null;
		    } catch (Exception e) {
			// ignore
		    }
		    if (typev != null && typev.equalsIgnoreCase("ugrd")) {
			return("Ugrad");
		    } else {
		    	// modified to put some grad students in sub-ous
		    	ProgramUtility pu = new ProgramUtility(System.getenv("OIM_CONNECTOR_HOME") + "/conf/gradprogs");
		    	try {
		    	Attribute pra = attrs.get("duPSAcadProgC1");
		    	if (pra != null && pra.get() != null) {
		    		if (pu.getProgram((String) pra.get()) != null) {
		    			return((String) pra.get() + ":Grad");
		    		} else {
		    			return("Grad");
		    		}
		    	} else {
		    		return("Grad");
		    	}
		    	} catch (Exception e) {
		    		throw new RuntimeException(e);
		    	}
		    }
		} else if (affilv != null && affilv.equalsIgnoreCase("affiliate")) {
		    // For affiliates, do the thing with their sponsor
		    Attribute asponsor = attrs.get("duSponsor");
		    String vsponsor = null;
		    try {
			vsponsor = asponsor != null ? (String) asponsor.get() : null;
		    } catch (Exception e) {
			// ignore
		    }
		    // If sponsor is set, get its orgunit and compute from that
		    if (vsponsor != null) {
			HashSet<SearchResult> SponsorSet = ldap.runQueryOneDeep(vsponsor,new String [] {"duSAPOrgUnit"});
			if (! SponsorSet.isEmpty()) {
			    Attributes sponsorattrs = SponsorSet.iterator().next().getAttributes();
			    if (sponsorattrs != null) {
				Attribute asponsorattr = sponsorattrs.get("duSAPOrgUnit");
				Attribute asponsorou = sponsorattrs.get("ou");
				// Add attribute if it exists
				if (asponsorattr != null) {
				    attrs.put(asponsorattr);
				}
			    }
			}
		    }
		} 
		// Regardless, if we get here, we've got duSAPOrgUnit set, at least (possibly duSAPPositionCode too)
		// Compute the result and send it back
		
		// Read in the data from the mapping file
		OUUtility U = new OUUtility(System.getenv("OIM_CONNECTOR_HOME") + "/conf/ounames");

		// Get the two attributes we need
		Attribute aposition = attrs.get("duSAPPositionCode");
		Attribute aorgunit = attrs.get("duSAPOrgUnit");

		String vposition = null;
		String vorgunit = null;
		
		try {
		    vposition = aposition != null ? (String) aposition.get() : null;
		    vorgunit = aorgunit != null ? (String) aorgunit.get() : null;
		} catch (Exception e) {
		    // ignore
		}

		String RetOU = null;
		String RetVal = null;

		// Compute the FunctionalGroup value
		if (vposition != null) {
		    // Check for position override
		    RetOU = U.getOU(vposition);
		}
		if (RetOU == null) {
		    // No override, so find the real thing
		    if (vorgunit != null) {
			RetOU = U.getOU(vorgunit);
			if (RetOU == null) {   // RetOU = null;
			    String [] parents = SAPWSUtility.getPath(vorgunit);
			    for (int i = 0; i < parents.length - 1 && RetOU == null; i ++) {
				if ((RetOU = U.getOU(parents[i])) != null) {
				    RetOU = RetOU.replace("ou=DukePeople,dc=ad,dc=duke,dc=edu","");
				    RetOU = RetOU.replace("ou=DukePeople,dc=win,dc=duke,dc=edu","");  // two cases -- test and prod -- both supported
				    RetVal = RetOU.replaceAll("ou=","");
				    RetVal = RetVal.replaceAll(",",":");
				    RetVal = RetVal.replaceAll(":$","");
				}
			    }
			} else {
			    RetOU = RetOU.replace("ou=DukePeople,dc=ad,dc=duke,dc=edu","");
			    RetOU = RetOU.replace("ou=DukePeople,dc=win,dc=duke,dc=edu",""); // two cases -- test and prod -- both supported
			    RetVal = RetOU.replaceAll("ou=","");
			    RetVal = RetVal.replaceAll(",",":");
			    RetVal = RetVal.replaceAll(":$","");
			}
		    } else {
			RetVal = "";
		    }
		} else {
		    RetOU = RetOU.replace("ou=DukePeople,dc=ad,dc=duke,dc=edu","");
		    RetOU = RetOU.replace("ou=DukePeople,dc=win,dc=duke,dc=edu","");  // two cases just in case
		    RetVal = RetOU.replaceAll("ou=","");
		    RetVal = RetVal.replaceAll(",",":");
		    RetVal = RetVal.replaceAll(":$","");
		}
		return(RetVal);
	    }
	}
	return(null);  // Just in case
    }
}



/**
 * @author rob
 */

class LDAPUtility {
    private static LDAPUtility instance = null;
    private LdapContext context = null;
    
    private LdapContext createConnection() {
      throw new RuntimeException("unexpected -- don't have an ldap connection object");
    }

    private LDAPUtility() {
	this.context = createConnection();
    }
    
    private LDAPUtility(DirContext context) {
    this.context = (LdapContext) context;
    }

    public static LDAPUtility getInstance() {
	if (instance == null) {
	    instance = new LDAPUtility();
	} 
	return instance;
    }
    
    public static LDAPUtility getInstance(DirContext context) {
    	if (instance == null) {
    		instance = new LDAPUtility(context);
    	}
    	return instance;
    }

    public HashSet runQuery(String filter, String base, String[] retattrs) {

	SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE,0,0,retattrs,false,false);
	NamingEnumeration<SearchResult> results = null;
	HashSet<SearchResult> res = new HashSet();
	try {
	    results = context.search(base,filter,cons);
	    while (results.hasMoreElements()) {
		res.add(results.next());
	    }
	    if (res.iterator().hasNext()) {
		return(res);
	    } else {
		return(null);
	    }
	} catch (NamingException e) {
	    throw new RuntimeException("Missed query: " + e.getMessage(),e);
	}
    }

    public HashSet runQueryOneDeep(String base, String[] retattrs) {

	SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE,0,0,retattrs,false,false);
	NamingEnumeration<SearchResult> results = null;
	HashSet<SearchResult> res = new HashSet();
	try {
	    results = context.search(base,"(objectClass=*)",cons);
	    while (results.hasMoreElements()) {
		res.add(results.next());
	    }
	    if (res.iterator().hasNext()) {
		return(res);
	    } else {
		return(null);
	    }
	} catch (NamingException e) {
	    throw new RuntimeException("Missed query: " + e.getMessage(),e);
	}
    }

}

/**
 * @author rob
 */

class OUUtility {
    
    private String inFileName = null;
    private File inFile = null;
    private FileInputStream inStream = null;
    
    private HashMap<String,String> ouValues = null;
    
    public OUUtility(String file) {
	try {
	inFileName = file;
	inStream = new FileInputStream(inFileName);

	DataInputStream input = new DataInputStream(inStream);
	BufferedReader br = new BufferedReader(new InputStreamReader(input));

	String s = null;
	ouValues = new HashMap<String,String>();

	while ((s = br.readLine()) != null) {
	    // for each line in the input file specified
	    String [] parts = s.split(":");
	    ouValues.put(parts[0],parts[1]);
	}
	br.close();   // Duh.
	} catch (Exception e) {
	    throw new RuntimeException("Exception: " + e.getMessage(),e);
	}

    }

    public String getOU(String orgUnit) {
	if (ouValues.containsKey(orgUnit)) {
	    return(ouValues.get(orgUnit));
	} else {
	    return(null);
	}
    }
}

class ProgramUtility {
	
		private String inFileName = null;
		private File inFile = null;
		private FileInputStream inStream = null;
		
		private HashMap<String,String> programValues = null;
		
		public ProgramUtility(String file) {
			try {
				inFileName = file;
				inStream = new FileInputStream(inFileName);
				
				DataInputStream input = new DataInputStream(inStream);
				BufferedReader br = new BufferedReader(new InputStreamReader(input));
				
				String s = null;
				programValues = new HashMap<String,String>();
				
				while ((s = br.readLine()) != null) {
					String [] parts = s.split(":");
					programValues.put(parts[0],parts[1]);
				}
				br.close();
			} catch (Exception e) {
				throw new RuntimeException("Exception: " + e.getMessage(),e);
			}
		}
		
		public String getProgram(String prog) {
			if (programValues.containsKey(prog)) {
				return(programValues.get(prog));
			} else {
				return(null);
			}
		}
}



/**
 * @author rob
 */

class SAPWSUtility {
    
    public static String [] getPath(String orgUnit) {

	try {
		
		// Acquire some configuration settings for later
		ConnectorConfig cfg = ConnectorConfig.getInstance();
		String endpoint = cfg.getProperty("sapws.endpoint");
		String username = cfg.getProperty("sapws.username");
		String password = cfg.getProperty("sapws.password");

	    // Attempt to avoid SSL failure
	    System.setProperty("org.apache.axis.components.net.SecureSocketFactory","org.apache.axis.components.net.SunFakeTrustSocketFactory");
	    
	    Service service = new Service();

	    Call call = (Call) service.createCall();  // Get a Call object to make calls on
	    
	    call.setProperty(Call.USERNAME_PROPERTY,cfg.getProperty("sapws.username"));
	    call.setProperty(Call.PASSWORD_PROPERTY,cfg.getProperty("sapws.password"));

	    call.setTargetEndpointAddress(new java.net.URL(endpoint));
	    call.setOperationName(new QName("urn:sap-com:document:sap:soap:functions:mc-style", ">RhphStructureRead"));

	    call.addParameter(new QName("Objid"),new QName("numeric8"),ParameterMode.IN);
	    call.setReturnType(new QName("urn:sap-com:document:sap:soap:functions:mc-style","RhphSTructureReadResponse"),RhphStructureReadResponse.class);

	    call.addParameter(new QName("Otype"),new QName("char2"),ParameterMode.IN);
	    call.addParameter(new QName("Plvar"),new QName("char2"),ParameterMode.IN);
	    call.addParameter(new QName("StruTab"),new QName("TableOfQcatStru"),ParameterMode.IN);
	    call.addParameter(new QName("Wegid"),new QName("char8"),ParameterMode.IN);
	    call.addParameter(new QName("WithStext"),new QName("char1"),ParameterMode.IN);
	    call.addParameter(new QName("Tdepth"),new QName("decimal5.0"),ParameterMode.IN);
	    call.addParameter(new QName("PupInfo"),new QName("char1"),ParameterMode.IN);
	    call.addParameter(new QName("Begda"),new QName("date"),ParameterMode.IN);
	    call.addParameter(new QName("Endda"),new QName("date"),ParameterMode.IN);

	    call.registerTypeMapping(RhphStructureReadResponse.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","RhphStructureReadResponse"),new org.apache.axis.encoding.ser.BeanSerializerFactory(RhphStructureReadResponse.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style",">RhphStructureReadResponse")),(DeserializerFactory) new RhphStructureReadResponseDeserializerFactory());

	    call.registerTypeMapping(TableOfQcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","TableOfQcatStru"),new org.apache.axis.encoding.ser.BeanSerializerFactory(TableOfQcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","TableOfQcatStru")),(DeserializerFactory) new TableOfQcatStruDeserializerFactory());

	    call.registerTypeMapping(QcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","QcatSTru"),new org.apache.axis.encoding.ser.BeanSerializerFactory(QcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","QcatSTru")),(DeserializerFactory) new QcatStruDeserializerFactory());

	    QName method = new QName("RhphStructureRead");
	    TableOfQcatStru ret = (TableOfQcatStru) call.invoke(new Object [] {orgUnit,"O","01","","O-O","x","0","x","today","today"});

	    String [] parStack = new String[ret.getItem().length];
	    int parnum = 0;
	    for (parnum = 0; parnum < ret.getItem().length; parnum++) {
		parStack[parnum] = ret.getItem()[parnum].getObjid();
	    }
	    return(parStack);

	} catch (Exception e) {
	    System.err.println("Exception:  "+e.getMessage());
	    String[] rv = new String [2];
	    rv[0] = orgUnit;
	    rv[1] = "50000000";  // fake parant as Duke University
	    return(rv);
	}
    }
}



class QcatStruDeserializerFactory implements DeserializerFactory {
public Deserializer createQcatStruDeserializer() {
        return getDeserializerAs("Axis SAX Mechanism");
}

public Deserializer getDeserializerAs(java.lang.String mechanismType) {
        QcatStru foo = new QcatStru();
//              return foo.getDeserializer(mechanismType,QcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","QCatStru"));
        return foo.getDeserializer(mechanismType, QcatStru.class, new QName("","QcatStru"));
}
public java.util.Iterator getSupportedMechanismTypes() {
        return null;
}
}

class RhphStructureReadResponseDeserializerFactory implements DeserializerFactory {
public Deserializer createRhphStructureReadResponseDeserializerFactory() {
        TableOfQcatStruDeserializerFactory T = new TableOfQcatStruDeserializerFactory();
        return T.getDeserializerAs("Axis SAX Mechanism");
}
public Deserializer getDeserializerAs(java.lang.String mechanismType) {
        TableOfQcatStruDeserializerFactory T = new TableOfQcatStruDeserializerFactory(); // RGC 
        return T.getDeserializerAs("Axis SAX Mechanism"); // RGC 
        
        //RhphStructureReadResponse foo = new RhphStructureReadResponse();
        //return foo.getDeserializer(mechanismType,RhphStructureReadResponse.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","RhphStructureReadResponse"));
}
public java.util.Iterator getSupportedMechanismTypes() {
        return null;
}
}


class TableOfQcatStruDeserializerFactory implements DeserializerFactory {
public Deserializer createTableOfQcatStruDeserializerFactory() {
        return getDeserializerAs("Axis SAX Mechanism");
}
public Deserializer getDeserializerAs(java.lang.String mechanismType) {
        TableOfQcatStru foo = new TableOfQcatStru();
        //return foo.getDeserializer(mechanismType,TableOfQcatStru.class,new QName("urn:sap-com:document:sap:soap:functions:mc-style","TableOfQcatStru"));
        return foo.getDeserializer(mechanismType, TableOfQcatStru.class, new QName("","TableOfQcatStru"));
        }
        public java.util.Iterator getSupportedMechanismTypes() {
                return null;
        }
}
