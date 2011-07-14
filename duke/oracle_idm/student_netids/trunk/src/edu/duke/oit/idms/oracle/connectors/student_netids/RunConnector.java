package edu.duke.oit.idms.oracle.connectors.student_netids;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.HashSet;
import java.io.*;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import java.net.*;
import javax.net.ssl.*;


public class RunConnector extends SchedulerBaseTask {

	  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	  
	  private String connectorName = "STUDENT_NETIDS";

	  private tcUserOperationsIntf moUserUtility;
	  
	  private tcITResourceInstanceOperationsIntf moITResourceUtility;
	  
	  private String wsURL=null,wsUser=null,wsPassword=null;
	  
	  private Map<String,String> parameters;
	  
	/**
	 * @param args
	 */
	protected void execute() {
		logger.info(connectorName + ": Starting Task");
		Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
		tcResultSet hasNetIDSet=null,allSet=null;
		HashSet<String> targets = new HashSet<String>();
		String [] getAttributes = {"USR_UDF_UID","Users.User ID","USR_UDF_IS_NETIDCREATEDISABLED"};

		// get a tcUserOperationsIntf object
		  try {
			  moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		  } catch (tcAPIException e) {
			  logger.error(connectorName + ": Unable to instantiate tcUserOperationsIntf");
			  return;
		  }
		  // Get a tcITResourceInstanceOperationsIntf
		  try {
			  moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
		  } catch (tcAPIException e) {
			  logger.error(connectorName + ": Unable to instantiate tcITResourceInstanceOperationsIntf");
			  return;
		  }
		  
		  try {
		  parameters = new HashMap<String,String>();
		  Map<String,String> resourceMap = new HashMap<String,String>();

	      resourceMap.put("IT Resources.Name","STUDENT_NETIDS");
	      tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
	      long resourceKey = moResultSet.getLongValue("IT Resources.Key");

	      moResultSet = null;
	      moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
	      for (int i=0; i < moResultSet.getRowCount();i++) {
	        moResultSet.goToRow(i);
	        String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
	        String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
	        parameters.put(name,value);
	      }
	      if (parameters.containsKey("webServiceURL")) {
	                wsURL = (String) parameters.get("webServiceURL");
	      }
	      if (parameters.containsKey("webServiceUser")) {
	                wsUser = (String) parameters.get("webServiceUser");
	      }
	      if (parameters.containsKey("webServicePassword")) {
	                wsPassword = (String) parameters.get("webServicePassword");
	      }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Unable to retrieve values from IT Resource");
			  return;
		  }

		  
		  // Perform a search through OIM to find users who meet the selection criteria.
		  // Start by identifying the selection criteria.
		  
		  mhSearchCriteria.clear();
		  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","people");
		  mhSearchCriteria.put("Users.Status", "Active");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
		  mhSearchCriteria.put("USR_UDF_UID","*");
		  try {
		  hasNetIDSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
		  } catch (Exception e) {
			  logger.error(connectorName + ": Error finding students with netids " + e.getMessage());
			  return; // fail out short if we can't find students
		  }
		  mhSearchCriteria.clear();
		  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","people");
		  mhSearchCriteria.put("Users.Status","Active");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
		  
		  try {
			  allSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
		  } catch (Exception e) {
			  logger.error(connectorName + ": Error finding all students " + e.getMessage());
			  return;
		  }
		  try {
		  for (int i = 0; i < allSet.getRowCount(); i++) {
			  allSet.goToRow(i);
			  if (allSet.getStringValue("USR_UDF_IS_NETIDCREATEDISABLED").equals("1")) {
				  logger.info(connectorName + ": Skipping " + allSet.getStringValue("Users.User ID") + " because NETIDCREATEDISABLED is set");
			  } else {
				  targets.add(allSet.getStringValue("Users.User ID"));
			  }
		  }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Exception getting Unique IDs from set of all students " + e.getMessage());
			  return;
		  }
		  try {
		  for (int i = 0; i < hasNetIDSet.getRowCount(); i++) {
			  hasNetIDSet.goToRow(i);
			  targets.remove(hasNetIDSet.getStringValue("Users.User ID"));
		  }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Exception removing Unique IDs from set of all student Unique IDs " + e.getMessage());
			  return;  // don't process if we don't have the right list of users 
		  }
		  Iterator<String> iter = targets.iterator();
		  while (iter.hasNext()) {
			  // DEBUG System.out.println(iter.next());
			  String cur = (String) iter.next();
			  if(createNetID(cur,wsURL,wsUser,wsPassword)) {
				  HashMap<String,String> setValues = new HashMap<String,String>();
				  setValues.put("USR_UDF_IS_STUDENTNETID", "1");
				  mhSearchCriteria.clear();
				  mhSearchCriteria.put("Users.User ID", cur);
				  try {
				  tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
				  if (moResultSet != null) {
					  	moUserUtility.updateUser(moResultSet, setValues);
				  }
				  } catch (Exception e) {
					  logger.error(connectorName + ": Failed to update studentnetid value for " + cur + " with " + e.getMessage() + " exception");
				  }
			  }
			  
		  }
		  logger.info(connectorName + ": Completed run");
	}
		  
	protected static boolean createNetID(String unique, String wsURL, String wsUser, String wsPassword) {
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[]{
				new X509TrustManager() {
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return null;
					}
			            public void checkClientTrusted(
			                java.security.cert.X509Certificate[] certs, String authType) {
			            }
			            public void checkServerTrusted(
			                java.security.cert.X509Certificate[] certs, String authType) {
			            }
			        }
			    };

			    String authstring = wsUser + ":" + wsPassword; // Update for production - RGC
			    String encoded = new String(org.apache.commons.codec.binary.Base64.encodeBase64(authstring.getBytes()));
			    // logger.info("STUDENT_NETIDS" + " Authorization property value set to: Basic "+encoded);


			    try { 
			        SSLContext sc = SSLContext.getInstance("SSL");
			        sc.init(null,trustAllCerts,new java.security.SecureRandom());
			        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			    } catch (Exception e) {
			        throw new RuntimeException("Failed installing SSL bypass: " + e.getMessage(),e);
			    }
			    URLConnection conn = null;
			    int responseCode=500;
			    try {
			        // Construct data
			        String data = "<WsNetIDCreate><WsNetIDToCreate><DukeID>"+unique+"</DukeID></WsNetIDToCreate></WsNetIDCreate>";
			        
			        // Send data
			        URL url = new URL(wsURL); // Update or production - RGC
			        conn = url.openConnection();
			        conn.setRequestProperty("Authorization","Basic " + encoded);
			        conn.setRequestProperty("Content-Type","text/xml");
			        
			        conn.setDoOutput(true);
			        conn.setDoInput(true);
			        
			        OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
			        
			        wr.write(data);
			        wr.flush();

			        // Get the response
			        responseCode = ((HttpURLConnection)(conn)).getResponseCode();

			        wr.close();

			    } catch (Exception e) {
	
			    }
			if (responseCode == 201) {
				logger.info("STUDENT_NETIDS" + ": Assigned netid for " + unique);
			    return(true);
			} else {
				logger.error("STUDENT_NETIDS" + ": Failed creating netid for " + unique + " with response code " + responseCode);
			    return(false);
			}
	}
			

}


