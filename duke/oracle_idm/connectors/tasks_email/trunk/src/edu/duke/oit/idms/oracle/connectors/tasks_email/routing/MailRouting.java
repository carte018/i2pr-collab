package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;
import edu.duke.oit.idms.oracle.connectors.tasks_email.routing.DepartmentRestrictions;

/**
 * Generates flat routing files for Sunmail as well as Exchange based on OIM attributes
 * @author Michael Meadows (mm310)
 *
 */
public class MailRouting extends SchedulerBaseTask {

	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private static RoutingFileLastUpdate lastUpdate;
	public final static String connectorName = "MAIL_ROUTING";

	
/**
 * Sets up and triggers routing file generation for Sunmail and Exchange mail systems
 */
	protected void execute() {

		tcProvisioningOperationsIntf moProvUtility = null;
		tcUserOperationsIntf moUserUtility = null;
		tcObjectOperationsIntf moObjectUtility = null;
		tcITResourceInstanceOperationsIntf moITResourceUtility = null;
		tcGroupOperationsIntf moGroupUtility = null;
		
		try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
      lastUpdate = new RoutingFileLastUpdate(moITResourceUtility);
      if (lastUpdate == null) {
        return;
      }
      
      moProvUtility = (tcProvisioningOperationsIntf) super.getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");
			moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			moObjectUtility = (tcObjectOperationsIntf) super.getUtility("Thor.API.Operations.tcObjectOperationsIntf");
			moGroupUtility = (tcGroupOperationsIntf) super.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
			String[] oimResultsAttributes = {"USR_UDF_IS_STUDENT", "USR_UDF_IS_STAFF", "USR_UDF_IS_FACULTY", "USR_UDF_IS_EMERITUS","USR_UDF_IS_AFFILIATE", "USR_UDF_IS_ALUMNI",
					"USR_UDF_PSACADPROGC1", "USR_UDF_PSACADPROGC2", "USR_UDF_PSACADPROGC3", "USR_UDF_PSACADPROGC4",
					"USR_UDF_PSACADCAREERC1", "USR_UDF_PSACADCAREERC2", "USR_UDF_PSACADCAREERC3", "USR_UDF_PSACADCAREERC4"
					};
			EmailUsers users = new EmailUsers(connectorName, moProvUtility, moUserUtility, moObjectUtility, moITResourceUtility, oimResultsAttributes);


			
			HashMap<String, RoutingData> routingDataMap = new HashMap<String, RoutingData>();
			Iterator<String> resourceIterator = users.getResourceIterator();
			while (resourceIterator.hasNext()) {
				String resource = resourceIterator.next();
				logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Instantiating RoutingData for resource: " + resource));
				RoutingData data = new RoutingData(connectorName, resource, moGroupUtility);
				routingDataMap.put(resource, data);
			}
			
			logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Begin routing files generation for resources."));
			
			if (!generateRoutingFiles(users, routingDataMap)) {
				logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Could not generate routing file for resources."));
				if (moGroupUtility != null) moGroupUtility.close();
				if (moITResourceUtility != null) moITResourceUtility.close();
				if (moObjectUtility != null) moObjectUtility.close();
				if (moUserUtility != null) moUserUtility.close();
				if (moProvUtility != null) moProvUtility.close();
				return;
			}
		} catch (tcAPIException e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Count not initialize tcAPI Interfaces needed to instantiate EmailUsers instance."));
			if (moGroupUtility != null) moGroupUtility.close();
			if (moITResourceUtility != null) moITResourceUtility.close();
			if (moObjectUtility != null) moObjectUtility.close();
			if (moUserUtility != null) moUserUtility.close();
			if (moProvUtility != null) moProvUtility.close();
			return;
		} catch (Exception e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", e.getMessage()), e);
			if (moITResourceUtility != null) moITResourceUtility.close();
			if (moObjectUtility != null) moObjectUtility.close();
			if (moUserUtility != null) moUserUtility.close();
			if (moProvUtility != null) moProvUtility.close();
			return;
		}
		
    try {
      lastUpdate.updateTaskEnd(moITResourceUtility);
    } catch (Exception e) {
      logger.error(EmailUsers.addLogEntry(connectorName, "ERROR", "Could not update TASK_END in OIM_TASK_LOG"), e);
    } 
		logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "Completed mail routing file entries."));
		moITResourceUtility.close();
		moObjectUtility.close();
		moUserUtility.close();
		moProvUtility.close();
	}

	/**
	 * Validates and updates mail routing entries
	 * @param target
	 * @param validator
	 */
	private boolean generateRoutingFiles(EmailUsers users, HashMap<String, RoutingData> routingDatum) {

		Iterator<String> resourceIterator = users.getResourceIterator();
		while(resourceIterator.hasNext()) {
			String resource = resourceIterator.next();
			logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Generating file for resource: " + resource));
			String resourceStatus = resource + "_STATUS";
			String resourceDate = resourceStatus + "_DATE";
			RoutingData target = routingDatum.get(resource);
			String localPath = target.getTasksData().getProperty("local.path");
			String fileName = target.getTasksData().getProperty("file");

			File outFile = new File(localPath + fileName);
			File inFile = new File(outFile.toString() + ".bak");
			
			// remove any local copies
			if (outFile.exists())
				outFile.delete();
			if (inFile.exists())
				inFile.delete();
			try {
				if (!copyFile("get", target, localPath, fileName))
					return false;
				outFile.renameTo(inFile); // make current outFile into new inFile
				
				PrintWriter routingWriter = null;
				routingWriter = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));

				// process current users already in the routing file
				Set<String> fileNetIDs = processCurrentFile(users, resource, routingDatum, inFile, routingWriter);
				
				// process the hashmap of all OIM users that are provisioned the resource
				Set<String> netids = users.getUsers().keySet();
				Iterator<String> netidsIterator = netids.iterator();
				while (netidsIterator.hasNext()) {
					HashMap<String, String> user = users.getUsers().get(netidsIterator.next());
					if (user != null) {
						if (user.containsKey("USR_UDF_UID")) {
							String netid = user.get("USR_UDF_UID");
							if (user.containsKey(resourceStatus)) {
								String status = user.get(resourceStatus);
								// continue if user status is Provisioned
								if (status.equals("Provisioned"))								
									if (fileNetIDs.contains(netid)) // already in routing file, continue
										continue;
									else if (resource.equals("COMMSDIR_PROVISIONING2")) { // sunmail file always gets updated
										appendRoutingFile(target, routingWriter, netid);
									} else if (resource.equals("EXCHANGE_PROVISIONING")) {
										long userKey = Long.parseLong(users.getUsers().get(netid).get("Users.Key"));
										if (!(existsBothSunmailFirst(user) || routingDatum.get("COMMSDIR_PROVISIONING2").getGroupMembers().contains(userKey) ||
												lessThanLastFileChange (resourceDate, user)) || (existsBothSunmailFirst(user) && (clearedDeptRestrictions(target, user) ||
														routingDatum.get("EXCHANGE_PROVISIONING").getGroupMembers().contains(userKey))))
											appendRoutingFile(target, routingWriter, netid);
									} else {
										logger.info(EmailUsers.addLogEntry(connectorName, "WARN", "Routing is not managed for this resource: " + target.getTasksData().getTaskName()));
									}
							}
						}
					}
				}
				logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Completed generating routing file for resource: " + resource));
				routingWriter.close();

				if (fileSizeDiffPasses(target, inFile.length(), outFile.length())) {
					if(!copyFile("put", target, localPath, fileName))
						return false;
				} else {
					String msg = EmailUsers.addLogEntry(connectorName, "ERROR", "File size variance is out of bounds. Routing file update cannot continue.");
					logger.error(msg);
				}
			} catch (FileNotFoundException e) {
				String msg = EmailUsers.addLogEntry(connectorName, "FAILED","File, " + inFile + " not found. Exiting . . .");
				logger.error(msg);
			} catch (IOException e) {
				String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Could not create file, " + outFile.toString() + ", for writing.");
				logger.error(msg);
			} catch (Exception e) {
				StackTraceElement[] stk = e.getStackTrace();
				StringBuffer stackBuffer = new StringBuffer(e.getMessage() + ": ");
				for (int i = 0; i < stk.length; ++i)
					stackBuffer.append("\t" + stk[i].toString() + "\n");
				logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Unhandled exception caught:" + stackBuffer));
			}
		}
		return true;
	}
	
	/**
	 * Manages of copying routing files between remote systems
	 * @param operation
	 * @param target
	 * @param localPath
	 * @param file
	 * @return
	 * @throws Exception
	 */
	private boolean copyFile(String operation, RoutingData target, String localPath, String file) throws Exception {
		String remoteHost = target.getTasksData().getProperty("remote.host");
		String remotePath = target.getTasksData().getProperty("remote.path");
		String user = target.getTasksData().getProperty("remote.user");
		String password = target.getTasksData().getProperty("remote.password");
		int port = Integer.parseInt(target.getTasksData().getProperty("remote.port"));
		
		if (port == 0) // if no value in properties.conf, set default
			port = 22;
		try {
			Connection scpConn = new Connection(remoteHost, port);
			scpConn.connect();
			
	        if (!scpConn.authenticateWithPassword(user, password)) {
	        	String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Authentication failed to " + remoteHost + " while " + operation + "ting file.");
	        	logger.error(msg);
		        scpConn.close();
	        	return false;
	        }
	        SCPClient scp = new SCPClient(scpConn);

        	File localFile = new File(localPath + file);
        	File remoteFile = new File (remotePath + file);
	        if (operation.equals("get")) {
	        	// remote to local copy
	        	logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Retrieving current routing file, " + file + " from host, " + remoteHost + " for processing."));
	        	scp.get(remoteFile.toString(), localPath);
	        	int localLength = (int) localFile.length();
	        	if (fileSizeLocalEqualsRemote(scpConn, localLength, remoteFile)) {
	        		Session session = scpConn.openSession();
		        	String updateDate = lastUpdate.getLastTaskSuccessDate();
		        	File backup = new File(remoteFile + "." + updateDate);
		        	logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Running remote command:" + "cp " + remoteFile + " " + backup));
		        	session.execCommand("cp " + remoteFile + " " + backup);
		        	session.close();
			        scpConn.close();
		        	return true;
	        	} else {
	        		logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "File Copy Error - Remote file, " + remoteFile + " is not the same size as local file, " + localFile));
	    	        scpConn.close();
	    	        return false;
	        	}
	    	// local to remote copy
			} else if (operation.equals("put")) {
				logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Replacing previous routing file, " + file + " from host, " + remoteHost + " with the current version."));
				File remoteFileTmp = new File(remoteFile + ".tmp");
	        	scp.put(localFile.toString(), file + ".tmp", remotePath, "0644");
	        	int localLength = (int) localFile.length();
	        	if (fileSizeLocalEqualsRemote(scpConn, localLength, remoteFileTmp)) {
	        		Session session = scpConn.openSession();
	        		logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Running remote command:" + "mv " + remoteFileTmp + " " + remoteFile));
	        		session.execCommand("mv " + remoteFileTmp + " " + remoteFile);
		        	session.close();
			        scpConn.close();
			        return true;
	        	} else {
	        		String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "File Copy Error - Remote file, " + remoteFile + " is not the same size as local file, " + localFile);
	        		logger.error(msg);
	    	        scpConn.close();
	    	        return false;
	        	}
	        } else {
	        	logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", "Unknown remote file operation called: " + operation));
	        }
	        scpConn.close();
		} catch (IOException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Error copying file, " + file + " remoteHost: " + remoteHost + " :" + e.getMessage());
			logger.error(msg, e);
			return false;
		}
		return false;
	}
	
	/**
	 * Reads the current routing file for a system. Current users in the file are copied into the generated file. Revoked and
	 *	unprovisioned users are left out of the generated file
	 * @param target
	 * @param inFile
	 * @param routingWriter
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private Set<String> processCurrentFile(EmailUsers target, String resource, HashMap<String, RoutingData> routingDatum, File inFile, PrintWriter routingWriter)
			throws NumberFormatException, IOException {
		logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Processing current routing file for resource: " + resource));
		BufferedReader routingReader = new BufferedReader(new FileReader(inFile));
		String routingLine = null;
		Set<String> netids = new HashSet<String>();

		// begin reading current routing file
		String lastNetID = "";
		while((routingLine = routingReader.readLine()) != null) {
			if (routingLine.trim().length() == 0)
				continue;
			String netid = null;
			if (resource.equals("EXCHANGE_PROVISIONING")) {
				routingLine = routingLine.toLowerCase();
				int position = routingLine.replace('\t', ' ').indexOf(' ') + 1;
				try {
					netid = routingLine.substring(position, routingLine.indexOf("@", position)).trim();
				} catch (Exception e) {
					logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Could not parse netid from routing file line: " + routingLine));
					continue;
				}
			} else
				netid = routingLine.substring(0, routingLine.indexOf('@'));
			// if new line has previous netid, write to output and continue;
			if (lastNetID.equals(netid)) {
					routingWriter.println(routingLine);
					netids.add(netid);
					continue;
			}
			// lookup netid in target, skip entry if status is Revoked, or if entry does not exist
			if (target.getUsers().containsKey(netid)) {
				HashMap<String, String> userMap = target.getUsers().get(netid);
				String resourceStatus = resource + "_STATUS";
				if (userMap.containsKey(resourceStatus)) {
					String status = userMap.get(resourceStatus);
					if (status.equals("Provisioned")) {
						if (resource.equals("COMMSDIR_PROVISIONING2")) { // sunmail file always gets updated
							routingWriter.println(routingLine);
							lastNetID = netid;
							netids.add(netid);						
						} else if (resource.equals("EXCHANGE_PROVISIONING")) {
							long userKey = Long.parseLong(target.getUsers().get(netid).get("Users.Key"));
							if (routingDatum.get("COMMSDIR_PROVISIONING2").getGroupMembers().contains(userKey)) // user is in forceSunmail
								continue;	// do not put sunmail user in exchange routing file
							routingWriter.println(routingLine);
							lastNetID = netid;
							netids.add(netid);
						} else {
							logger.info(EmailUsers.addLogEntry(connectorName, "WARN", "Routing is not managed for this resource: " + resource));
						}

					}
				}
			}		
		}
		routingReader.close();
		logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Completed processing current routing file for resource: " + resource));
		return netids;
	}
	
	/**
	 * Generates needed routing lines for new users being added to a generated routing file
	 * @param target
	 * @param routingWriter
	 * @param netid
	 */
	private void appendRoutingFile(RoutingData target, PrintWriter routingWriter, String netid) {
		HashSet<String> entries = target.getTasksData().getRoutingEntries();
		Iterator<String> entriesIterator = entries.iterator();
		while (entriesIterator.hasNext()) {
			String line = entriesIterator.next();
			String newline = line.replaceAll("netid", netid);
			routingWriter.println(newline);
		} 
	}
	
	/**
	 * Determine if there is a department class defined, if so, call it's method to determine if routing passes
	 *   department restrictions. Department class is determined by the first defined
	 * @param target
	 * @param userMap
	 * @return
	 */
	private boolean clearedDeptRestrictions (RoutingData target, HashMap<String, String> userMap) {
		SortedMap<Integer, String> depts = new TreeMap<Integer, String>();
		String dept = null;
		int priority;
		Set<String> keys = userMap.keySet();
		Iterator<String> keysIterator = keys.iterator();
		while (keysIterator.hasNext()) {
			String key = keysIterator.next();
			if (key.contains("USR_UDF_PSACAD")) {
				dept = userMap.get(key);
				priority = target.getRestrictionsData().getDeptPriority(dept);
				if (depts.containsKey(priority))	// if same priority, existing routing wins, so no routing update is needed
					return false;					// as there are only 2 routing choices, return false at this point
				depts.put(priority,dept);
			}			
		}
		
		if (depts.isEmpty()) // default is no routing update for 2 accounts, routing already in place remains
			return false;
		else
			dept = depts.get(depts.firstKey()); // lowest integer is highest priority
		
		if (!target.getRestrictionsData().hasClassDefined(dept)) // if no department-specific class has been defined, then use default policy
			return true;
		
		DepartmentRestrictions deptRest;
		try {
			deptRest = target.getDeptRestrictionsClass(dept.toLowerCase());
		} catch (Exception e) {
			String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "DepartmentRestrictions class defined for " + dept + " but could not be instantiated.");
			logger.error(msg);
			return false;
		}
		Properties deptProperties = target.getRestrictionsData().getDeptProperties(dept.toLowerCase());
		return (deptRest.updateRoutingForUser(connectorName, deptProperties, userMap));		

	}
	
	/**
	 * Determine is the user provision status is more recent than the initiation time of the last successful file update
	 * @param userMap
	 * @return
	 */
	private static boolean lessThanLastFileChange (String resourceDate, HashMap<String, String> userMap) {
		if (!userMap.containsKey(resourceDate))
			return false;
		String userTaskStatusChange = userMap.get(resourceDate);
		double userTaskStatusChangeDbl = 0;
		userTaskStatusChangeDbl = Double.parseDouble(userTaskStatusChange);
				
		return (userTaskStatusChangeDbl < Double.parseDouble(lastUpdate.getLastTaskSuccessDate()));
	}
	
	/**
	 * Determine if the user has an account in both Sunmail and Exchange, with Sunmail existing first. 
	 * @param target
	 * @param validator
	 * @param netid
	 * @return
	 */
	private boolean existsBothSunmailFirst (HashMap<String, String> user) {
		/*logger.info(EmailUsers.addLogEntry("DEBUG", "netid:" + netid + ": targetTaskName:" + target.getTasksData().getTaskName() +
				": validatorTaskName:" + validator.getTasksData().getTaskName() + ": validatorContainsNetID:" + validator.getUsers().containsKey(netid)));*/
		
		return (user.containsKey("EXCHANGE_PROVISIONING_STATUS_DATE") && 
				user.containsKey("COMMSDIR_PROVISIONING2_STATUS_DATE") &&
				(Long.parseLong(user.get("COMMSDIR_PROVISIONING2_STATUS_DATE")) > Long.parseLong(user.get("EXCHANGE_PROVISIONING_STATUS_DATE")))
				);
	}
	
	/**
	 * Check difference in size between current and generated routing file is within variance specified in properties
	 * @param target
	 * @param inLength
	 * @param outLength
	 * @return
	 */
	private boolean fileSizeDiffPasses(RoutingData target, double inLength, double outLength) {
		String pctChangeString = target.getTasksData().getProperty("percentchange");

		if (outLength == 0) {
			return false;
		}
		if (pctChangeString == null || pctChangeString.equals(""))
			return true;
		double percentChange = Double.parseDouble(pctChangeString);
		double diffFileSize = 0;
		if (outLength <= inLength)
			diffFileSize = 100 - (outLength / inLength * 100);
		else
			diffFileSize = 100 - (inLength / outLength * 100);
		logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "fileSizeDiffPasses: outLength:" + outLength + ": inLength:" + inLength + ": percentChange:" + percentChange + ": diffFileSize:" + diffFileSize));
		return (diffFileSize <= percentChange);
	}
	
	/**
	 * Ensures that the copied file was duplicated successfully by comparing the file sizes of the remote and local copies
	 * @param conn
	 * @param localLength
	 * @param remoteFile
	 * @return
	 * @throws Exception
	 */
	private boolean fileSizeLocalEqualsRemote(Connection conn, int localLength, File remoteFile) throws Exception {
    	Session session;
    	int remoteLength = 0;
		try {
			session = conn.openSession();
	    	session.execCommand("du -b " + remoteFile);
	    	InputStream stdout = new StreamGobbler(session.getStdout());
	    	BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
	    	StringBuffer sb = new StringBuffer();
	    	char c = 0;
	    	while ((c = (char)stdoutReader.read()) != -1 && c != '\t')
	    		sb.append(c);
	    	
	    	if (sb.length() > 0)
	    		remoteLength = Integer.parseInt(sb.toString());
	    	logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "remoteLength: " + remoteLength + " localLength: " + localLength));
	    	session.close();
		} catch (IOException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Error opening session to " + conn.getHostname() + ", unable to determine success of remote copy operation.");
			logger.error(msg);
			return false;
		}

    	return (localLength == remoteLength);
	}
}
