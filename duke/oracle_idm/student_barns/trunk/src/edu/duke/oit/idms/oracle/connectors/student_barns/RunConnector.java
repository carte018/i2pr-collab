package edu.duke.oit.idms.oracle.connectors.student_barns;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.HashSet;
import java.io.*;

import java.security.MessageDigest;
import java.util.Random;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import javax.naming.NamingEnumeration;
import javax.naming.Context;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;


public class RunConnector extends SchedulerBaseTask {
	
    private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);

	public final String connectorName = "STUDENT_BARNS";
	
	  private tcUserOperationsIntf moUserUtility;
	  
	  private tcITResourceInstanceOperationsIntf moITResourceUtility;
	  private Map<String,String> parameters;
	  private String fromAddress, hasAliasMessageFile, noAliasMessageFile, hasAliasSubjectLine, noAliasSubjectLine;
	  private static String shadowEmail;
	  private String applicantLDAPURL,applicantLDAPUser,applicantLDAPPassword;


	  private static String convertToHex(byte[] data) {
		    StringBuffer buf = new StringBuffer();
		    for (int i = 0; i < data.length; i++) {
		      int halfbyte = (data[i] >>> 4) & 0x0F;
		      int two_halfs = 0;
		      do {
		        if ((0 <= halfbyte) && (halfbyte <= 9))
		          buf.append((char) ('0' + halfbyte));
		        else
		          buf.append((char) ('a' + (halfbyte - 10)));
		        halfbyte = data[i] & 0x0F;
		      } while (two_halfs++ < 1);
		    }
		    return buf.toString();
		  }
	  
	  public static String createBARN(String id) {
		    byte[] randbytes = new byte[40];

		    new Random().nextBytes(randbytes);
		    String randstring = new String(randbytes);

		    MessageDigest md;
		    try {
		      md = MessageDigest.getInstance("SHA-1");
		      byte[] sha1hash = new byte[40];

		      id = id + randstring;

		      md.update(id.getBytes("iso-8859-1"), 0, id.length());
		      sha1hash = md.digest();
		      return convertToHex(sha1hash);
		    } catch (Exception e) {
		      throw new RuntimeException(e);
		    }
		  }
	  
	    public static void doEmail(String fromaddr, String toaddr, String subject, String body) {
	    	// Code goes here
	    	Properties props = new Properties();
	    	props.put("mail.host","smtp.duke.edu");
	    	Session session = Session.getInstance(props,null);
	    	Message message = new MimeMessage(session);

	    	try {
	    	    Address toAddress = new InternetAddress(toaddr);
	    	    Address fromAddress = new InternetAddress(fromaddr);
	        
	    	    message.setContent(body, "text/plain");
	    	    message.setFrom(fromAddress);
	    	    message.setRecipient(Message.RecipientType.TO, toAddress);
	    	    message.setSubject(subject);
	          
	    	    Transport.send(message);
	    	    
	    	    if (shadowEmail != null) { 
	    	    	// if config requests a shadow copy of the email...
	    	    	Address shadowAddress = new InternetAddress(shadowEmail);
	    	    	Message shadowMessage = new MimeMessage(session);
	    	    	shadowMessage.setContent(body,"text/plain");
	    	    	shadowMessage.setFrom(fromAddress);
	    	    	shadowMessage.setRecipient(Message.RecipientType.TO, shadowAddress);
	    	    	shadowMessage.setSubject(subject);
	    	    	Transport.send(shadowMessage);
	    	    }
	    	} catch (AddressException e) {
	    	    throw new RuntimeException(e);
	    	} catch (MessagingException e) {
	    	    throw new RuntimeException(e);
	    	} catch (Exception e) {
	    	    throw new RuntimeException(e);
	    	}
	        }

	protected void execute() {
		
		logger.info(connectorName + ": Starting Task");
		Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
		tcResultSet hasBarnExpirationSet=null,allSet=null;
		HashSet<String> targets = new HashSet<String>();
		String [] getAttributes = {"USR_UDF_UID","Users.User ID","USR_UDF_IS_NETIDCREATEDISABLED","USR_UDF_IS_STUDENTNETID","USR_UDF_BARNEXPIRATIONDATE","USR_UDF_NETIDSTATUSDATE","USR_UDF_PSEXTERNALEMAIL","USR_UDF_EMAILALIAS","USR_UDF_HAS_CHALRESP","USR_UDF_PSEMPLID"};
		String baseLink = "";
		
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
		  
		  // Retrieve IT Resource configuration information
		  
		  try {
		  parameters = new HashMap<String,String>();
		  Map<String,String> resourceMap = new HashMap<String,String>();

	      resourceMap.put("IT Resources.Name","STUDENT_BARNS");
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
	      if (parameters.containsKey("fromAddress")) {
	                fromAddress = (String) parameters.get("fromAddress");
	      }
	      if (parameters.containsKey("hasAliasMessageFile")) {
	                hasAliasMessageFile = (String) parameters.get("hasAliasMessageFile");
	      }
	      if (parameters.containsKey("noAliasMessageFile")) {
	                noAliasMessageFile = (String) parameters.get("noAliasMessageFile");
	      }
	      if (parameters.containsKey("hasAliasSubjectLine")) {
	    	  		hasAliasSubjectLine = (String) parameters.get("hasAliasSubjectLine");
	      }
	      if (parameters.containsKey("noAliasSubjectLine")) {
	    	  		noAliasSubjectLine = (String) parameters.get("noAliasSubjectLine");
	      }
	      if (parameters.containsKey("applicantLDAPURL")) {
	    	  		applicantLDAPURL = (String) parameters.get("applicantLDAPURL");
	      }
	      if (parameters.containsKey("applicantLDAPUser")) {
	    	  		applicantLDAPUser = (String) parameters.get("applicantLDAPUser");
	      }
	      if (parameters.containsKey("applicantLDAPPassword")) {
	    	  		applicantLDAPPassword = (String) parameters.get("applicantLDAPPassword");
	      }
	      if (parameters.containsKey("baseLink")) {
	    	  baseLink = (String) parameters.get("baseLink");
	      }
	      if (parameters.containsKey("shadowEmail")) {
	    	  shadowEmail = (String) parameters.get("shadowEmail");
	      } else {
	    	  shadowEmail = null;
	      }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Unable to retrieve values from IT Resource");
			  return;
		  }
		  
		  // Select users for whom BARNs need to be created
		  
		  mhSearchCriteria.clear();
		  mhSearchCriteria.put("USR_UDF_ENTRYTYPE", "people");
		  mhSearchCriteria.put("Users.Status", "Active");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
		  mhSearchCriteria.put("USR_UDF_NETIDSTATUS", "pending pw activation");
		  mhSearchCriteria.put("USR_UDF_PSEXTERNALEMAIL","*");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENTNETID","1");
		  mhSearchCriteria.put("USR_UDF_UID","*");
		  mhSearchCriteria.put("USR_UDF_BARNEXPIRATIONDATE","*");
		  try {
		  hasBarnExpirationSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
		  } catch (Exception e) {
			  logger.error(connectorName + ": Unable to find users with barn expiration set - " + e.getMessage());
			  return;
		  }
		  
		  mhSearchCriteria.clear();
		  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","people");
		  mhSearchCriteria.put("Users.Status","Active");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENT","1");
		  mhSearchCriteria.put("USR_UDF_NETIDSTATUS","pending pw activation");
		  mhSearchCriteria.put("USR_UDF_PSEXTERNALEMAIL","*");
		  mhSearchCriteria.put("USR_UDF_IS_STUDENTNETID","1");
		  mhSearchCriteria.put("USR_UDF_UID","*");
		  try {
			  allSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
		  } catch (Exception e) {
			  logger.error(connectorName + ": Unable to get list of all eligible users - " + e.getMessage());
			  return;
		  }
		  
		  // Report
		  try {
		  logger.info(connectorName + ": Eligible students with barn expiration set" + hasBarnExpirationSet.getRowCount());
		  logger.info(connectorName + ": Eligible sudents total" + allSet.getRowCount());
		  } catch (Exception e) {
			  // Ignore exceptions while logging
		  }
		  // Subtract
		  
		  try {
		  for (int i = 0; i < allSet.getRowCount(); i++) {
			  allSet.goToRow(i);
			  if ((allSet.getStringValue("USR_UDF_EMAILALIAS") != null && ! allSet.getStringValue("USR_UDF_EMAILALIAS").equals("")) || (allSet.getDate("USR_UDF_NETIDSTATUSDATE").getTime() <= (System.currentTimeMillis() - 1000*24*60*60))) {
				  targets.add(allSet.getStringValue("Users.User ID"));
			  }
		  }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Unable to subset target users - " + e.getMessage());
			  return;
		  }
		  try {
			  for (int i = 0; i < hasBarnExpirationSet.getRowCount(); i++) {
				  hasBarnExpirationSet.goToRow(i);
				  targets.remove(hasBarnExpirationSet.getStringValue("Users.User ID"));
			  }
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception removing Unique IDs from set of all student Unique IDs");
				  return;
			  }
		  // Log again
		  logger.info(connectorName + ": Subset of target users has " + targets.size() + " elements");
		  
		  // targets now contains the Uniques of the targets we want to barnitize.
		  Iterator<String> iter = targets.iterator();
		  while (iter.hasNext()) {
			  String cur = (String) iter.next();
			  // create a BARN for the user
			  String newBarn = createBARN(cur);
			  long newBarnExpirationDate = System.currentTimeMillis() + (14 * 24 * 60 * 60 * 1000);
			  // store the BARN value in OIM along with the expiration date
			  HashMap<String,String> setValues = new HashMap<String,String>();
			  setValues.put("USR_UDF_BARN", newBarn);
			  setValues.put("USR_UDF_BARNEXPIRATIONDATE",String.valueOf(newBarnExpirationDate));
			  mhSearchCriteria.clear();
			  mhSearchCriteria.put("Users.User ID", cur);
			  tcResultSet moResultSet = null;
			  try {
				  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
				  if (moResultSet != null) {
					  	moUserUtility.updateUser(moResultSet, setValues);
					  	logger.info(connectorName + ": Added barn for " + cur);
				  }
			  } catch (Exception e) {
				  logger.error(connectorName + ": Failed to update barn value for " + cur + " with " + e.getMessage() + " exception");
				  return;  // if barn can't be created, return so we'll try again on next run with this user
			  }
			  // DEACTIVATE APPLICANT ENTRY HERE
			  // ...
			  try {
			  if (moResultSet != null) {
				  if (! deactivateApplicant(moResultSet.getStringValue("USR_UDF_PSEMPLID"),applicantLDAPURL,applicantLDAPUser,applicantLDAPPassword)) {
					  logger.error(connectorName + ": Failed deactivation of applicant ID for " + cur);
					  // continue on deactivation error, since deactivation is non-mandatory
				  } else {
					  logger.info(connectorName + ": Deactivated applicant ID for " + cur + " with emplid " + moResultSet.getStringValue("USR_UDF_PSEMPLID"));					  
				  } 
			  }
			  } catch (Exception e) {
				  logger.error(connectorName + ": Exception deactivating applicant ID for " + cur + " - " + e.getMessage());
				  //continue, since deactivation is non-mandatory
			  }
			  //
			  try {
			  // COPY OVER CRV DATA IF PRESENT HERE...
			  //
			  } catch (Exception e) {
				  logger.error(connectorName + ": Failed writing new CRV data for user " + cur + " with exception " + e.getMessage());
				  // crv data copy will be non-mandatory
			  }
			  
			  // SEND EMAIL TO THE NEW STUDENT WITH TEXT SUBSTITUTIONS HERE
			  //
			  try {
			  // Retrieve target email address from user object
			  String targetEmailAddress = moResultSet.getStringValue("USR_UDF_PSEXTERNALEMAIL");
			
			  // Retrieve the message text as appropriate
			  File msgbodyfile = null;
			  String curSubject = "";
			  if (moResultSet.getStringValue("USR_UDF_EMAILALIAS") != null && ! moResultSet.getStringValue("USR_UDF_EMAILALIAS").equals("")) {
				  msgbodyfile = new File(hasAliasMessageFile);
				  curSubject = hasAliasSubjectLine;
			  } else {
				  msgbodyfile = new File(noAliasMessageFile);
				  curSubject = noAliasSubjectLine;
			  }
			  FileInputStream fstream = new FileInputStream(msgbodyfile);
			  DataInputStream input = new DataInputStream(fstream);
			  BufferedReader ireader = new BufferedReader(new InputStreamReader(input));
			  String msgbody = "", s = "";
			  while ((s=ireader.readLine()) != null) {
				  // Perform line by line substitutions on "s" here
				  if (s.contains("%LINK%")) {
					  s = s.replaceAll("%LINK%", baseLink + cur + "?barn=" + newBarn);
				  }
				  msgbody = msgbody + "\n" + s;
			  }
			  
			  // And actually send the thing
			  doEmail(fromAddress,targetEmailAddress,curSubject,msgbody); // For production
			  logger.info(connectorName + ": Sent barn email for " + cur + " to " + targetEmailAddress);
			  
			  } catch (Exception e) {
				  logger.error(connectorName + ": Failed sending email message user " + cur + " with exception " + e.getMessage());
				  // no need to return here, since fall-thru has the same effect.
			  }
			  
		  }
		  logger.info(connectorName + ": Completed run");
	}
	
	private boolean deactivateApplicant(String emplID,String ldapURL,String ldapUser,String ldapPassword) {
		  LdapContext ldap = null;
          Hashtable<String,String> environment = new Hashtable<String,String>(); 
          environment.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
          environment.put(Context.PROVIDER_URL,ldapURL);
          environment.put(Context.SECURITY_AUTHENTICATION, "simple");
          environment.put(Context.SECURITY_PRINCIPAL, ldapUser);
          environment.put(Context.SECURITY_CREDENTIALS,ldapPassword);
          environment.put(Context.SECURITY_PROTOCOL, "ssl");
          try {
          ldap = new InitialLdapContext(environment, null);
          String [] attrs = {"userPassword"};
          SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs, false, false);
          NamingEnumeration<SearchResult> result;
          result = ldap.search("ou=applicants,ou=external","dupsemplid="+emplID,cons);
          SearchResult answer = null;
          if (result.hasMoreElements()) {
        	  answer = result.next();
        	  String dn = answer.getNameInNamespace();
        	  BasicAttributes attributes = new BasicAttributes();
        	  attributes.put(new BasicAttribute("userPassword"));
        	  ldap.modifyAttributes(dn,DirContext.REMOVE_ATTRIBUTE,attributes);
        	  logger.info(connectorName + ": userPassword removed for " + dn);
        	  return true;
          } else {
        	  	logger.info(connectorName + ": No applicant id for emplid " + emplID);
        	  	return true;
          }
          } catch (Exception e) {
        	  logger.error(connectorName + ": Failed trying to deactivate applicant ID with exception " + e.getMessage());
        	  return false;
          }
	}
}
