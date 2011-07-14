package edu.duke.oit.idms.oracle.connectors.recon_applicants;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Hashtable;
import java.io.*;
import edu.duke.oit.idms.applicants.*;
import org.apache.commons.lang.RandomStringUtils;
import javax.naming.NamingEnumeration;
import javax.naming.Context;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

public class RunConnector extends SchedulerBaseTask {
	  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	  /*
	   * Some information about the input file we'll process
	   * This will all be overridden by atain the DCO_RECONCILIATION 
	   * object in OIM, but we need defaults just in case.
	   */
	  // Server where the input file comes to us from
	  private String sourceServer = "howes.oit.duke.edu";
	  // Source file name on sourceServer
	  private String sourceFile = "/servers/idms/work/applicants/applicant.csv";
	  // SSH key used to transfer files from sourceServer
	  private String transferKeyFile = "/export/home/oracle/howes-access";
	  // SSH user on sourceServer as whom to transfer file
	  private String transferUser = "root";
	  // Location on local machine of target file
	  private String destFile = "/export/home/oracle/applicants";

	  //Name of the current connector
	  
	  private String connectorName = "APPLICANT_RECONCILIATION";

	  private tcUserOperationsIntf moUserUtility;

	  // Some data stream objects
	  
	  File targetFile = null;
	  FileInputStream fstream = null;
	  Process proc = null;
	  
	  // and the central HashMap
	  
	  Map<String,Hashtable<String,String>> hashMap = null;
	  
      Map<String,String> parameters = new HashMap<String,String>();

	  
	  
	  /** 
	   * Initialize the file parameters from the IT Resource in OIM,
	   * acquire a copy of the source file, and then load it into a 
	   * Hashtable indexing User ID values against other attribute values
	   * and return the Hashtable.
	   */
	  
	  private Map<String,Hashtable<String,String>> getInFileData() throws Exception {
	            Map<String,Hashtable<String,String>> retval = new HashMap<String,Hashtable<String,String>>();

	            tcITResourceInstanceOperationsIntf moITResourceUtility = (tcITResourceInstanceOperationsIntf) 
	            super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");

	            Map<String,String> resourceMap = new HashMap<String,String>();
	            resourceMap.put("IT Resources.Name", "APPLICANT_RECONCILIATION");
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

	            if (parameters.containsKey("sourceServer")) {
	                sourceServer = (String)parameters.get("sourceServer");
	            } 
	            if (parameters.containsKey("sourceFile")) {
	                sourceFile = (String)parameters.get("sourceFile");
	            }
	            if (parameters.containsKey("transferKeyFile")) {
	                transferKeyFile = (String)parameters.get("transferKeyFile");
	            }
	            if (parameters.containsKey("transferUser")) {
	                transferUser = (String)parameters.get("transferUser");
	            }
	            if (parameters.containsKey("destFile")) {
	                destFile = (String)parameters.get("destFile");
	            }
	            logger.info(connectorName + "Starting scp...");
	            try {
	                //String cmds[] = {"/usr/bin/scp","-i",transferKeyFile,transferUser + "@" + sourceServer + ":" + sourceFile,destFile};
	                String cmds1[] = {"/usr/bin/ssh","-i",transferKeyFile,transferUser + "@" + sourceServer,"cat " + sourceFile + "> /tmp/finalized"};
	                String cmds2[] = {"/usr/bin/ssh","-i",transferKeyFile,transferUser + "@" + sourceServer,"mv " + sourceFile + " /srv/idms/loaders/peoplesoft/applicants/archive/"};
	                String cmds3[] = {"/usr/bin/scp","-i",transferKeyFile,transferUser + "@" + sourceServer + ":/tmp/finalized",destFile};
	                //proc = Runtime.getRuntime().exec("/usr/bin/scp -i " + transferKeyFile + " " + transferUser + "@" + sourceServer + ":"+ sourceFile + " " + destFile);
	                //proc = Runtime.getRuntime().exec(cmds);
	                proc = Runtime.getRuntime().exec(cmds1);
	                proc.waitFor();
	                proc = Runtime.getRuntime().exec(cmds2);
	                proc.waitFor();
	                proc = Runtime.getRuntime().exec(cmds3);
	                proc.waitFor();
	                } catch (Exception e) {
	                throw new RuntimeException("Exception in scp: " + e.getMessage(),e);
	                }
	                logger.info(connectorName + "Ended scp...");
	            targetFile = new File(destFile);

	            // At this point, we either successfully retrieved the file or we didn't, but we can begin the process
	            // of loading up data from the file anyway and see where we land.
	            //
	            int LinesInFile = 0;
	            try {
	                fstream = new FileInputStream(destFile);
	            } catch (Exception e) {
	                // This is OK, but we'll need to log the problem
	                // and bail out gracefully
	                logger.warn(connectorName + ": Input file not found -- no update for applicants this time");
	                return(retval);
	            }
	            DataInputStream input = new DataInputStream(fstream);
	            
	            BufferedReader file = new BufferedReader(new InputStreamReader(input));
	            String s = null;
	            
	            while ((s = file.readLine()) != null) {
	                LinesInFile = LinesInFile + 1;  // increment line count
	                if (! s.contains(",")) {
	                	continue; // only observe lines with commas in them
	                }
	                // Convert embedded commas in double-quoted strings to <comma>
	                // taking into account the possibility of multiple quoted commas
	                // then convert <comma> tags back to commas after the split
	                
	                String sx = s;
	                String sy = null;
	                while (! sx.equals(sy)) {
	                	sy = sx;
	                	sx = sx.replaceAll("\"([^,\"]+),([^\"]*)\"","\"$1<comma>$2\"");
	                }
	                String [] splitout = sx.split(",");
	                logger.info(connectorName + "Split data has " + splitout.length + "entries");
	                for (int jj=0; jj <=26; jj++) {
	                	splitout[jj] = splitout[jj].replaceAll("[\"]", "");
	                	splitout[jj] = splitout[jj].replaceAll("<comma>",",");
	                	if (jj == 2) {
	                		logger.info(connectorName + "Split data element 2 is " + splitout[jj]);
	                	}
	                }
	                logger.info(connectorName + ": Starting foo...");
	                try {
	                	Hashtable<String,String> foo = new Hashtable<String,String>();
	                	foo.put("EMPLID", splitout[0]);
	                	foo.put("OPERID", splitout[1]);
	                	foo.put("USERALIAS",splitout[2]);
	                	foo.put("CAMPUS_ID",splitout[3]);
	                	foo.put("EXTERNAL_ID",splitout[4]);
	                	foo.put("NAME_PREFIX",splitout[5]);
	                	foo.put("FIRST_NAME",splitout[6]);
	                	foo.put("MIDDLE_NAME",splitout[7]);
	                	foo.put("LAST_NAME",splitout[8]);
	                	foo.put("NAME_SUFFIX",splitout[9]);
	                	foo.put("DATEOFBIRTH",splitout[10]);
	                	foo.put("GENDER",splitout[11]);
	                	foo.put("EMAIL",splitout[12]);
	                	foo.put("COUNTRY",splitout[13]);
	                	foo.put("ADDR1",splitout[14]);
	                	foo.put("ADDR2",splitout[15]);
	                	foo.put("ADDR3",splitout[16]);
	                	foo.put("CITY",splitout[17]);
	                	foo.put("STATE",splitout[18]);
	                	foo.put("POSTAL",splitout[19]);
	                	foo.put("PHONE",splitout[20]);
	                	foo.put("NATIONALID",splitout[21]);
	                	foo.put("APPLICANT",splitout[22]);
	                	foo.put("CAREER",splitout[23]);
	                	foo.put("TERM",splitout[24]);
	                	foo.put("STATUS",splitout[25]);
	                	foo.put("CREATEDT",splitout[26]);
	                	if (splitout.length > 27) {
	                		foo.put("UPDATEDT",splitout[27]);
	                	} else {
	                		foo.put("UPDATEDT","");
	                	}
	                	logger.info(connectorName + ": Finishing foo");
	                	retval.put(splitout[2],foo);
	                	logger.info(connectorName + ": Finished foo");
	                } catch (Exception e) {
	                	throw new RuntimeException(e);
	                }
	            }
	            return retval;
	  }
	  
	  

	  protected void execute() {
		  
		  logger.info(connectorName + ": Starting task.");
		  
		  boolean needToSendNewBARN = false;
		  try {
			  if ((hashMap = getInFileData()) == null || hashMap.size() == 0) {
				  return;  // Nothing to do, go back to your homes;
			  }
		  } catch (Exception e) {
			  logger.error(connectorName + ": Failed retrieving applicant input file -- bailing out on " + e.getMessage());
			  return;
		  }
		  	
		  // get a tcUserOperationsIntf object
		  try {
			  moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
		  } catch (tcAPIException e) {
			  logger.error(connectorName + ": Unable to instantiate tcUserOperationsIntf");
			  return;
		  }
		  
		  // At this point, we have the information from the file in hand.  Process it one
		  // line at a time.
		  Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
		  tcResultSet moResultSet;
		  String[] getAttributes= {"USR_UDF_PSEMPLID","USR_UDF_PSOPERID","Users.User ID","USR_UDF_PSCAMPUSID","USR_UDF_PSEXTERNALID","Users.First Name","Users.Middle Name","Users.Last Name","USR_UDF_DATEOFBIRTH","USR_UDF_GENDERCODE","Users.Email","USR_UDF_HOMEADRCOUNTRY","USR_UDF_HOMEADRLINE1","USR_UDF_HOMEADRLINE2","USR_UDF_HOMEADRLINE3","USR_UDF_HOMEADRCITY","USR_UDF_HOMEADRSTATE","USR_UDF_HOMEADRZIP","USR_UDF_HOMEPHONE","USR_UDF_PSNATIONALID","USR_UDF_PSACADCAREERC1","USR_UDF_PSADMITTERMC1"};
		  
		  
		  Iterator<String> it = hashMap.keySet().iterator();
		  while (it.hasNext()) {
			  needToSendNewBARN = false;  // Assume the best and prepare for the worst
			  String key = it.next();
			  logger.info(connectorName + ": Loading info for user " + key);
			  Hashtable<String,String> val = hashMap.get(key);
			  mhSearchCriteria.clear();
			  mhSearchCriteria.put("USR_UDF_PSUSERALIAS", key);
			  mhSearchCriteria.put("Users.Status", "Active");
			  mhSearchCriteria.put("USR_UDF_ENTRYTYPE","applicants");
			  try{
			  moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, getAttributes);
			  } catch (Exception e) {
				  logger.info(connectorName + ": Throwing exception finding the user: " + e.getMessage());
				  throw new RuntimeException(e);
			  }
			  logger.info(connectorName + ": Completed search for user");
			
			  try {
				  
			  if (moResultSet.getRowCount() > 1) {
				  logger.error(connectorName + ": Found too many users for ID : " + key);
				  throw new RuntimeException("Multipel users with ID: " + key);
			  }
			  if (moResultSet.isEmpty()) {
				  // We have a new user -- verify that the new user flag is on and act 
				  // accordingly.
				  //
				  logger.info(connectorName + ": No user " + key + " found - adding");
				  if (val.get("STATUS").equals("U")) {
					  // Error
					  logger.error(connectorName + ": Update record should have been a new record -- adding anyway for key " + key);
					  val.put("STATUS","N");  // fake it
				  }
				  if (val.get("STATUS").equals("N")) {
					  // OK -- this is a new entry -- create the object in OIM
					  logger.info(connectorName + ": SISS input status is new");
					  Map<String,String> newUserAttrs = new HashMap<String,String>();
					  
					  // Some fixed values to add for all creations so that OIM is happy
					  
					  newUserAttrs.put("Users.Password",RandomStringUtils.randomAlphanumeric(60));
					  
					  newUserAttrs.put("Users.User ID",val.get("EMAIL")); // new user gets a user id
					  newUserAttrs.put("USR_UDF_ENTRYTYPE","applicants");
					  newUserAttrs.put("Users.Role","Full-Time");
					  newUserAttrs.put("Organizations.Organization Name","Xellerate Users");
					  newUserAttrs.put("Users.Xellerate Type","End-User");
					  newUserAttrs.put("USR_UDF_IS_STAFF","0");
					  newUserAttrs.put("USR_UDF_IS_STUDENT", "0");
					  newUserAttrs.put("USR_UDF_IS_FACULTY", "0");
					  newUserAttrs.put("USR_UDF_IS_EMERITUS", "0");
					  newUserAttrs.put("USR_UDF_IS_ALUMNI", "0");
					  newUserAttrs.put("USR_UDF_IS_AFFILIATE","0");
					  newUserAttrs.put("Organizations.Key","1");
					  
					  newUserAttrs.put("USR_UDF_PSEMPLID", val.get("EMPLID"));
					  newUserAttrs.put("USR_UDF_PSOPERID", val.get("OPERID"));
					  newUserAttrs.put("USR_UDF_PSUSERALIAS", val.get("USERALIAS"));
					  newUserAttrs.put("USR_UDF_PSCAMPUSID", val.get("CAMPUS_ID"));
					  newUserAttrs.put("USR_UDF_PSEXTERNALID", val.get("External_ID"));
					  newUserAttrs.put("Users.First Name", val.get("FIRST_NAME"));
					  newUserAttrs.put("Users.Middle Name", val.get("MIDDLE_NAME"));
					  newUserAttrs.put("Users.Last Name", val.get("LAST_NAME"));
					  logger.info(connectorName + "Before DOB");
					  String [] dtparts = val.get("DATEOFBIRTH").split("/");
					  logger.info(connectorName + "During DOB - " + dtparts[0] + " " + dtparts[1] + " " + dtparts[2]);
					  int dobm = Integer.parseInt(dtparts[0]);
					  int dobd = Integer.parseInt(dtparts[1]);
					  int doby;
					  if (Integer.parseInt(dtparts[2]) < 100) {
						  doby = 1900+Integer.parseInt(dtparts[2]);
					  } else {
						  doby = Integer.parseInt(dtparts[2]);
					  }
					  if (doby < 1930) {
						  doby += 100;
					  }
					  newUserAttrs.put("USR_UDF_DATEOFBIRTH",String.format("%d",doby)+String.format("%02d",dobm)+String.format("%02d",dobd));
					  //newUserAttrs.put("USR_UDF_DATEOFBIRTH",String.format("19%1$02d%2$02%3$02d", Integer.parseInt(dtparts[2]),Integer.parseInt(dtparts[0]),Integer.parseInt(dtparts[1])));
					  logger.info(connectorName + "After DOB");
					  newUserAttrs.put("USR_UDF_GENDERCODE",val.get("GENDER"));
					  newUserAttrs.put("Users.Email",val.get("EMAIL"));
					  newUserAttrs.put("USR_UDF_HOMEADRCOUNTRY",val.get("COUNTRY"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE1", val.get("ADDR1"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE2", val.get("ADDR2"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE3", val.get("ADDR3"));
					  newUserAttrs.put("USR_UDF_HOMEADRCITY",val.get("CITY"));
					  newUserAttrs.put("USR_UDF_HOMEADRSTATE",val.get("STATE"));
					  newUserAttrs.put("USR_UDF_HOMEADRZIP",val.get("POSTAL"));
					  newUserAttrs.put("USR_UDF_HOMEPHONE",val.get("PHONE"));
					  newUserAttrs.put("USR_UDF_PSNATIONALID",val.get("NATIONALID"));
					  newUserAttrs.put("Users.Status",val.get("APPLICANT").equals("Y")?"Active":"Disabled");
					  newUserAttrs.put("USR_UDF_PSACADCAREER",val.get("CAREER"));
					  newUserAttrs.put("USR_UDF_PSADMITTERMC1",val.get("TERM"));
					  logger.info(connectorName + "Calling createUser");
					  moUserUtility.createUser(newUserAttrs); // create 
					  logger.info(connectorName + "user created");
					  // And add the BARN + send email about it
					  User u = new User(val.get("EMAIL"));
					  u.sendBARN();  // create and send a BARN for the user
				  } else {
					  // This should be impossible now, so throw the exception
					  logger.error(connectorName + ": Failed to find entry for update user in applicant input: " + key);
					  throw new RuntimeException("Update user record for user not found in OIM with ID " + key);
				  }
			  } else {
				  if (val.get("STATUS").equals("N")) {
					  logger.error(connectorName + ": Found extant entry for new user in applicant input : " + key);
					  // We log the error but do not throw an exception in the case where an N comes for 
					  // an extant entry -- we just write the error (and the email in production) and 
					  // move on to the next entry
					  // We do *not* overwrite the existing entry, since this may be an erorr in processing.
					  // If SISS believes the entry is new, we need to process it manually.
					  //throw new RuntimeException("User record found in OIM for new applicant " + key);
				  } else {
					  // This is an update case -- just update the whole set of attributes to match the input record
					  Map<String,String> newUserAttrs = new HashMap<String,String>();
					  if (moResultSet.getStringValue("Users.Email").equals(val.get("EMAIL"))) {
						  // Do nothing -- there is no email change happening
						  logger.info(connectorName + ": No email change for user " + key);
					  } else {
						  // We have a new email address for this user.  Depending on the state of the user
						  // in the LDAP directory, we either update both the email address and the user id
						  // and arrange to have a new BARN sent to the user (if the user is not yet activated)
						  // or we update only the email address and send no barn to the user.
						  // We check for the existence of a userPassword value in the LDAP entry for the
						  // user, which requires that we make an LDAP connection at this point.
						  LdapContext ldap = null;
                          Hashtable<String,String> environment = new Hashtable<String,String>(); 
                          environment.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
                          environment.put(Context.PROVIDER_URL,parameters.get("ldapURL"));
                          environment.put(Context.SECURITY_AUTHENTICATION, "simple");
                          environment.put(Context.SECURITY_PRINCIPAL, parameters.get("ldapUser"));
                          environment.put(Context.SECURITY_CREDENTIALS,parameters.get("ldapPassword"));
                          environment.put(Context.SECURITY_PROTOCOL, "ssl");
                          ldap = new InitialLdapContext(environment, null);
                          String [] attrs = {"userPassword"};
                          SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, attrs, false, false);
                          NamingEnumeration<SearchResult> result;
                          result = ldap.search("ou=applicants,ou=external","mail="+val.get("EMAIL"),cons);
                    	  SearchResult answer = null;
                    	  if (! result.hasMoreElements()) {
                    		  // We didn't find the user, but that may not be surprising, since the email address may be changing
                    		  // Check instead with the more stable but slower duPSEmplID value
                    		  result = ldap.search("ou=applicants,ou=external","dupsemplid="+val.get("EMPLID"),cons);
                    	  }
                          if (result.hasMoreElements()) {
                        	  // only if there's someone there
                        	  answer = result.next();
                        	  if (answer != null && answer.getAttributes() != null && answer.getAttributes().get("userPassword") != null && answer.getAttributes().get("userPassword").get() != null && ! answer.getAttributes().get("userPassword").get().toString().equals("")) {
                        		  // user has already registered, so we do nothing here but close the connection
                        		  logger.info(connectorName + ": No need to update User ID for activated user " + key);
                        		  ldap.close();
                        	  } else {
                        		  // change the user's ID as well as his email address, and agree to send a new Barn
                        		  logger.info(connectorName + ": Staging change of User ID for " + key + " from " + moResultSet.getStringValue("Users.User ID") + " to " + val.get("EMAIL"));
                        		  newUserAttrs.put("Users.User ID", val.get("EMAIL"));
                        		  needToSendNewBARN = true;
            					  ldap.close();
                        	  }
                          } else {
                        	  // Change the user's ID and email address, since he's not been 
                        	  // completely registered yet
                        	  logger.info(connectorName + ": Early update of email address -- changing BARN and staging change of User ID for " + key + " from " + moResultSet.getStringValue("Users.User ID") + " to " + val.get("EMAIL"));
                        	  newUserAttrs.put("Users.User ID", val.get("EMAIL"));
                        	  needToSendNewBARN = true;
                        	  ldap.close();
                          }
					  }
					  // Already done the right thing with email changes and the BARN
					  // User ID value is in the newUserAttrs hash if it needs to be
					  newUserAttrs.put("USR_UDF_PSEMPLID", val.get("EMPLID"));
					  newUserAttrs.put("USR_UDF_PSOPERID", val.get("OPERID"));
					  newUserAttrs.put("USR_UDF_PSUSERALIAS", val.get("USERALIAS"));
					  newUserAttrs.put("USR_UDF_PSCAMPUSID", val.get("CAMPUS_ID"));
					  newUserAttrs.put("USR_UDF_PSEXTERNALID", val.get("External_ID"));
					  newUserAttrs.put("Users.First Name", val.get("FIRST_NAME"));
					  newUserAttrs.put("Users.Middle Name", val.get("MIDDLE_NAME"));
					  newUserAttrs.put("Users.Last Name", val.get("LAST_NAME"));
					  String [] dtparts = val.get("DATEOFBIRTH").split("/");
					  int dobm = Integer.parseInt(dtparts[0]);
					  int dobd = Integer.parseInt(dtparts[1]);
					  int doby;
					  if (Integer.parseInt(dtparts[2]) < 100) {
						  doby = 1900+Integer.parseInt(dtparts[2]);
					  } else {
						  doby = Integer.parseInt(dtparts[2]);
					  }
					  if (doby < 1930) {
						  doby += 100;
					  }
					  newUserAttrs.put("USR_UDF_DATEOFBIRTH",String.format("%d",doby)+String.format("%02d",dobm)+String.format("%02d",dobd));
					  newUserAttrs.put("USR_UDF_GENDERCODE",val.get("GENDER"));
					  newUserAttrs.put("Users.Email",val.get("EMAIL"));
					  newUserAttrs.put("USR_UDF_HOMEADRCOUNTRY",val.get("COUNTRY"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE1", val.get("ADDR1"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE2", val.get("ADDR2"));
					  newUserAttrs.put("USR_UDF_HOMEADRLINE3", val.get("ADDR3"));
					  newUserAttrs.put("USR_UDF_HOMEADRCITY",val.get("CITY"));
					  newUserAttrs.put("USR_UDF_HOMEADRSTATE",val.get("STATE"));
					  newUserAttrs.put("USR_UDF_HOMEADRZIP",val.get("POSTAL"));
					  newUserAttrs.put("USR_UDF_HOMEPHONE",val.get("PHONE"));
					  newUserAttrs.put("USR_UDF_PSNATIONALID",val.get("NATIONALID"));
					  newUserAttrs.put("Users.Status",val.get("APPLICANT").equals("Y")?"Active":"Disabled");
					  newUserAttrs.put("USR_UDF_PSACADCAREER",val.get("CAREER"));
					  newUserAttrs.put("USR_UDF_PSADMITTERMC1",val.get("TERM"));
					  moUserUtility.updateUser(moResultSet,newUserAttrs);
					  if (needToSendNewBARN) {
						  logger.info(connectorName + ": Sending a new BARN on unactivated email change");
				     	  User u = new User(val.get("EMAIL"));
                    	  u.sendBARN(); // create and send a BARN for the user
					  }
				  }
			  }
			  
		  } catch (Exception e) {
			  logger.info(connectorName + "Throwing exception during update or creation: " + e.getMessage() + " due to " + e.toString() + " for user " + key);
			  // If we get an exception here, we should just continue with an error
			  // throw new RuntimeException(e);
		  }
		  
		  }
		  cleanfile();
		  logger.info(connectorName + ": Ending task.");
	  }
	  
	  private void cleanfile() {
          try {
                targetFile.renameTo(new File("/tmp/old.Applicants"));
            } catch (Exception e) {
                logger.warn(connectorName + ": Failed to rename to /tmp/old.Applicants -- continuing");
            }
  }

}
