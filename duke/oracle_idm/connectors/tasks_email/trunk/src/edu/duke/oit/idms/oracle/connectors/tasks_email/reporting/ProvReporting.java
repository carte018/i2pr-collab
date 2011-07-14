package edu.duke.oit.idms.oracle.connectors.tasks_email.reporting;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchResult;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcITResourceNotFoundException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;
import edu.duke.oit.idms.oracle.connectors.tasks_email.TasksData;

/**
 * Generates and emails User Provisioning reporting based on departmental requirements
 * @author mm310
 *
 */
public class ProvReporting extends SchedulerBaseTask {

	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	public final static String connectorName = "PROV_REPORTING";
	private TasksData tasksData;
	private Date reportStart, reportEnd;
	 
	@Override
	protected void execute() {
			
		tcProvisioningOperationsIntf moProvUtility = null;
		tcUserOperationsIntf moUserUtility = null;
		tcObjectOperationsIntf moObjectUtility = null;
		tcITResourceInstanceOperationsIntf moITResourceUtility = null;
		try {
			moProvUtility = (tcProvisioningOperationsIntf) super.getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");
			moUserUtility = (tcUserOperationsIntf) super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			moObjectUtility = (tcObjectOperationsIntf) super.getUtility("Thor.API.Operations.tcObjectOperationsIntf");
			moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
			String[] oimResultsAttributes = {"USR_LAST_NAME", "USR_FIRST_NAME", "USR_UDF_SAPLASTDAYWORKED", "USR_UDF_NETIDSTATUS", "USR_UDF_NETIDSTATUSDATE", "USR_UDF_FUNCTIONALGROUP",
					"USR_UDF_LDAPKEY", "USR_UDF_IS_STUDENT", "USR_UDF_IS_STAFF", "USR_UDF_IS_FACULTY", "USR_UDF_IS_EMERITUS","USR_UDF_IS_AFFILIATE", "USR_UDF_IS_ALUMNI",
					"USR_UDF_PSACADPROGC1", "USR_UDF_PSACADPROGC2", "USR_UDF_PSACADPROGC3", "USR_UDF_PSACADPROGC4",
					"USR_UDF_PSACADCAREERC1", "USR_UDF_PSACADCAREERC2", "USR_UDF_PSACADCAREERC3", "USR_UDF_PSACADCAREERC4"};
			EmailUsers emailUsers = new EmailUsers(connectorName, moProvUtility, moUserUtility, moObjectUtility, moITResourceUtility, oimResultsAttributes);
			tasksData = TasksData.getInstance(connectorName);
			int offset = 0;
			String range = tasksData.getProperty("date.range");
			if (range != null && !range.equals(""))
				offset = Integer.parseInt(range);
			setDateParameters(offset);
			
			HashMap<HashMap<String, String>, SortedSet<String> > deptReports = generateReports(moITResourceUtility, emailUsers);
			dumpReportBodies (deptReports);

			try {
				sendReport(deptReports); // email it
			} catch (javax.mail.MessagingException e) {
				logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Could not sent report."), e);
				return;
			}

		} catch (tcAPIException e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Count not initialize tcAPI Interfaces needed to instantiate EmailUsers instance."));
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
		
		logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "Reports generation completed."));
		moITResourceUtility.close();
		moObjectUtility.close();
		moUserUtility.close();
		moProvUtility.close();
	}
	
	/**
	 * Creates the report
	 * @param department
	 * @param reports
	 * @param usersByResource
	 * @throws Exception
	 */
	private HashMap<HashMap<String, String>, SortedSet<String> > generateReports(tcITResourceInstanceOperationsIntf moITResourceUtility, EmailUsers users) {
		HashMap<HashMap<String, String>, SortedSet<String> > reports = new HashMap<HashMap<String, String>, SortedSet<String> >();
		initializeReports(reports);
		
		Iterator<HashMap<String, String> > userIterator = users.getUsers().values().iterator();
		while (userIterator.hasNext()) {
			HashMap<String, String> user = userIterator.next();
			//logger.info(EmailUsers.addLogEntry("DEBUG", "user: " + user.toString()));
			Iterator<String> resourceIterator = users.getResourceIterator();
			while (resourceIterator.hasNext()) {
				String resource = resourceIterator.next();
				String resourceStatus = resource + "_STATUS";
				String resourceDate = resourceStatus + "_DATE";

				if (!user.containsKey(resourceStatus)) //if user never had resource, continue
					continue;
				
				String category = setUserCategory(resourceStatus, user);
				if (category == null)
					continue; // user is not Provisioned or Deprovisioned
				
				if (user.containsKey(resourceDate) && fallsInReportRange(user.get(resourceDate))) {
					Iterator<HashMap<String, String> > reportKeyIterator = reports.keySet().iterator();
					while (reportKeyIterator.hasNext()) {
						HashMap<String, String> reportKey = reportKeyIterator.next();
						String reportType = reportKey.get("type");
						String reportResource = reportKey.get("resource");
						String department = reportKey.get("department");
						SortedMap<String, String> columns = tasksData.getColumnMappingsForDept(resource, department);
						if (category.equals("Provisioned")) {
							String userType;
							if (user.containsKey("USR_UDF_IS_STUDENT") && user.get("USR_UDF_IS_STUDENT").equals("1"))
								userType = "Student";
							else
								userType = "Nonstudent";
							if (reportType.equals(userType) && reportResource.equals(resource)) {
								department = department.toUpperCase();
								if (department.equals("ALL") ||
										(user.containsKey("USR_UDF_PSACADCAREERC1") && user.get("USR_UDF_PSACADCAREERC1").equals(department))
										|| (user.containsKey("USR_UDF_PSACADCAREERC1") && user.get("USR_UDF_PSACADCAREERC1").equals(department))
										|| (user.containsKey("USR_UDF_PSACADCAREERC2") && user.get("USR_UDF_PSACADCAREERC2").equals(department))
										|| (user.containsKey("USR_UDF_PSACADCAREERC3") && user.get("USR_UDF_PSACADCAREERC3").equals(department))
										|| (user.containsKey("USR_UDF_PSACADCAREERC4") && user.get("USR_UDF_PSACADCAREERC4").equals(department))
										|| (user.containsKey("USR_UDF_PSACADPROGC1") && user.get("USR_UDF_PSACADPROGC1").equals(department))
										|| (user.containsKey("USR_UDF_PSACADPROGC2") && user.get("USR_UDF_PSACADPROGC2").equals(department))
										|| (user.containsKey("USR_UDF_PSACADPROGC3") && user.get("USR_UDF_PSACADPROGC3").equals(department))
										|| (user.containsKey("USR_UDF_PSACADPROGC4") && user.get("USR_UDF_PSACADPROGC4").equals(department))
										|| (user.containsKey("USR_UDF_FUNCTIONALGROUP") && user.get("USR_UDF_FUNCTIONALGROUP").toUpperCase().contains(department))) {
									SortedSet<String> reportBody = reports.get(reportKey);
									String line = reportAddLine(columns, user, resource);
									reportBody.add(line);
								}
							}
						} else { // Deprovisioned or Deactivated
						  
							if (user.containsKey("USR_UDF_FUNCTIONALGROUP") && (user.get("USR_UDF_FUNCTIONALGROUP").toUpperCase().contains(department)
									|| department.equals("ALL"))) {
								SortedSet<String> reportBody = reports.get(reportKey);
								reportBody.add(reportAddLine(columns, user, resource));
							}
							if (user.containsKey("USR_UDF_LDAPKEY")) {
								String career = mostRecentStudentCareer(moITResourceUtility, user.get("USR_UDF_LDAPKEY"));
								if (career != null && (career.equals(department) || department.equals("ALL"))) {
									//logger.info(TasksData.addLogEntry("DEBUG", "career: " + career + "duLDAPKey: " + user.get("USR_UDF_LDAPKEY")));
									SortedSet<String> reportBody = reports.get(reportKey);
									reportBody.add(reportAddLine(columns, user, resource));
								}
							}
						}
					}					
				}
			}
		}
		logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "generateReports completed successfully."));
		return reports;
	}
	
	/**
	 * Initializes the report structure
	 * @param department
	 * @param reports
	 * @param usersByResource
	 * @throws Exception
	 */
	private void initializeReports (HashMap<HashMap<String, String>, SortedSet<String> > reports) {

		HashSet<String> departments = tasksData.getDepartments();
		Iterator<String> departmentsIterator = departments.iterator();
		while (departmentsIterator.hasNext()) {  // determine which reports are requested

			String department = departmentsIterator.next();
			HashSet<String> reportResources = tasksData.getReportResourcesForDept(department);
			Iterator<String> reportResourcesIterator = reportResources.iterator();
			while (reportResourcesIterator.hasNext()) {
			
				String resource = reportResourcesIterator.next();
				HashSet<String> reportTypes = tasksData.getReportTypeForDept(department);
				Iterator<String> typesIterator = reportTypes.iterator();
				while (typesIterator.hasNext()) {

					String type = typesIterator.next();
					String isEnabled = tasksData.getProperty(department + "." + type.toLowerCase() + ".mail.enabled");
					if (isEnabled == null || isEnabled.equals("true")) {
						HashMap<String, String> reportHeaders =  new HashMap<String, String>();
						reportHeaders.put("department", department);
						reportHeaders.put("resource", resource);
						reportHeaders.put("type", type);
						SortedSet<String> sortedReport = new TreeSet<String>();
						// populate headers for all reports
						reports.put(reportHeaders, sortedReport);
						//logger.info(EmailUsers.addLogEntry("initializeReports: DEBUG", "reportHeaders" + reportHeaders.toString() + " sortedReport: " + sortedReport));
					}
				}
			}
		}
	}

	/**
	 * Returns the status of the resource for the user - Provisioned or Deprovisioned
	 * @param user
	 * @return
	 */
	private String setUserCategory (String resourceStatus, HashMap<String, String> user) {
		String category = null;
		if (user.containsKey(resourceStatus)) {
			if (user.get(resourceStatus).equals("Provisioned")) {
				if (user.containsKey("USR_UDF_NETIDSTATUS")) {
					if (user.get("USR_UDF_NETIDSTATUS").toLowerCase().equals("inactive")) { // status is Deactivated
						category = "Deactivated";
					} else {
						category = "Provisioned";
					}
				} // status is Provisioned
			}
			if (user.get(resourceStatus).equals("Revoked")) { // status is Deprovisioned
				category = "Deprovisioned";
			}
		}
		user.put(resourceStatus, category);
		return category;
	}
	
	/**
	 * Verifies the date range for the report
	 * @param date
	 * @return
	 */
	private boolean fallsInReportRange(String dateString) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = null;
		try {
			date = sdf.parse(dateString);
		} catch (ParseException e) {
			return false;
		}
		return (date.equals(reportStart) || date.equals(reportEnd) || (date.after(reportStart) && date.before(reportEnd)));
	}
	
	/**
	 * Sets the start and end of the report
	 * @param offset
	 */
	private void setDateParameters(int offset) {
		Calendar start = Calendar.getInstance();
		start.set(Calendar.HOUR_OF_DAY, 0);
		start.set(Calendar.MINUTE, 0);
		start.set(Calendar.SECOND, 0);
		start.set(Calendar.MILLISECOND, 0);
		start.add(Calendar.DATE, -offset);
		Calendar end = Calendar.getInstance();
		end.setTimeInMillis(start.getTimeInMillis());
		end.add(Calendar.DATE, offset);
		end.add(Calendar.MILLISECOND, -1);
		reportStart = start.getTime();
		reportEnd = end.getTime();
		if (reportEnd.before(reportStart))
			reportEnd = new Date();
		//logger.info(EmailUsers.addLogEntry("DEBUG", "reportStart: " + reportStart + " :reportEnd: " + reportEnd));
	}
	/**
	 * If the account is deprovisioned and the user is a student, there is no way to discern departmental info from OIM. Therefore, a query of duStudentCareerHistory in ldap
	 *   is required
	 * @param duLDAPKey
	 * @return
	 * @throws tcAPIException
	 * @throws tcColumnNotFoundException
	 * @throws tcITResourceNotFoundException
	 * @throws NamingException
	 */
	@SuppressWarnings("unchecked")
	private String mostRecentStudentCareer (tcITResourceInstanceOperationsIntf moITResourceUtility, String duLDAPKey) {
		String career = null;
		//read ldap for duStudentCareerHistory to determine department in the case of a student who is inactive
		Attributes attributes = null;
		try {
			LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(connectorName, moITResourceUtility);
			SearchResult result = ldapConnectionWrapper.findEntryByLDAPKey(connectorName, duLDAPKey);
			//logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Retrieved ldap for: " + duLDAPKey));
			if (result != null) {
				//logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "SearchResult: " + result.getAttributes().toString()));
				attributes = result.getAttributes();
			}
			if (attributes != null && attributes.size() > 0) {
				//logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Start processing studentCareerHistory"));
				Attribute duStudentCareerHistory = attributes.get("duStudentCareerHistory");
				NamingEnumeration<String> duStudentCareerHistoryValues = (NamingEnumeration<String>) duStudentCareerHistory.getAll();
				String careerLine = null;
				int mostRecent = 0;
				while (duStudentCareerHistoryValues.hasMoreElements()) {
					careerLine = duStudentCareerHistoryValues.next();
					String [] fields = careerLine.split("\\|", 5);
					if (fields.length != 5 || fields[0].equals("") || fields[3].equals("")) {
						String msg;
						if (duLDAPKey == null)
							msg = "Null value for duLDAPKey. Skipping entry . . .";
						else
							msg = "Invalid duStudentCareerHistory format for duLDAPKey:" + duLDAPKey + " in LDAP. Skipping entry . . .";
						logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", msg));
						continue;
					}
					int date = Integer.parseInt(fields[3]);
					if (date > mostRecent) {
						mostRecent = date;
						career = fields[0];
					}
				}		
			}
			//logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Finished processing studentCareerHistory:" + career));
		} catch (Exception e) {
			logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", "Could not retrieve duStudentCareerHistory from LDAP for duLDAPKey: " + duLDAPKey), e);
		}
		return career;
	}
	
	/**
	 * Sends the report via email
	 * @param department
	 * @param reports
	 * @throws MessagingException
	 */
	private void sendReport(HashMap<HashMap<String, String>, SortedSet<String> > reports) throws MessagingException {
		dumpReportBodies(reports);
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
		Iterator<HashMap<String, String> > reportKeyIterator = reports.keySet().iterator();
		while (reportKeyIterator.hasNext()) {
			HashMap<String, String> reportKey = reportKeyIterator.next();
			String department = reportKey.get("department");
			String resource = reportKey.get("resource");
			String type = reportKey.get("type");
			SortedSet<String> report = reports.get(reportKey);
			StringBuffer reportBody = new StringBuffer();
			SortedMap<String, String> columns = tasksData.getColumnMappingsForDept(resource, department);
			Iterator<String> reportIterator = report.iterator();
			if (reportIterator.hasNext())
				reportBody.append(reportAddLine(columns, null, resource));
			while (reportIterator.hasNext())
				reportBody.append(reportIterator.next());
			String reportTitle = department.toUpperCase() + " " + type + " Report for " + resource + " from " + sdf.format(reportStart)
				+ " to " + sdf.format(reportEnd);
			logger.info(EmailUsers.addLogEntry(connectorName, "BEGIN", "Generating " + reportTitle));
			String sender = tasksData.getProperty(department + "." + type.toLowerCase() + ".mail.sender");
			String recipient = tasksData.getProperty(department + "." + type.toLowerCase() + ".mail.recipient");
			String mailHost =  tasksData.getProperty(department + "." + type.toLowerCase() + ".mail.host");
			
			if (sender == null || recipient == null || mailHost == null || reportBody.lastIndexOf("\n") ==  reportBody.indexOf("\n")) {
				logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Report: " + reportTitle + " contains zero entries, Skipping . . ."));
				continue;
			}
		    Properties properties = new Properties();
		    properties.put("mail.host", mailHost);
		     
		    Session session = Session.getInstance(properties, null);
		    Message message = new MimeMessage(session);		   
		    Address toAddress;
		    toAddress = new InternetAddress(recipient);
		    Address fromAddress = new InternetAddress(sender);
		    message.setContent(reportBody.toString(), "text/plain");
		    message.setFrom(fromAddress);
		    message.setRecipient(Message.RecipientType.TO, toAddress);
		    message.setSubject(reportTitle);
		    Transport.send(message);
		    logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "Report sent: " + reportTitle));
	
		}
	}
	
	/**
	 * Formats and appends a report line to the generated report
	 * @param queryCols
	 * @param cols
	 * @param user
	 * @return
	 */
	private String reportAddLine(SortedMap<String, String> cols, HashMap<String, String> user, String resource) {
		Set<String> queryCols = cols.keySet();
		String line = new String();
		Iterator<String> queryColsIterator = queryCols.iterator();
		while (queryColsIterator.hasNext()) {
			String key = queryColsIterator.next();
			String column = key.substring(key.indexOf('.') + 1);
			//logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "reportAddLine:column: " + column));
			if (user == null) // only populate report column names
				line += cols.get(key);
			else if (user.containsKey(column)) // there is a value for the column
				line += user.get(column);
			if (queryColsIterator.hasNext())
				line += ",";
			else
				line += "\n";
		}
		//logger.info(EmailUsers.addLogEntry("DEBUG", "report line: " + line));
		return line;
	}
	
	private void dumpReportBodies(HashMap<HashMap<String, String>, SortedSet<String> > reports) {
		Set<HashMap<String, String> > reportHeaders = reports.keySet();
		Iterator<HashMap<String, String> > reportHeaderIterator = reportHeaders.iterator();
		while (reportHeaderIterator.hasNext()) {
			HashMap<String, String> reportHeader = reportHeaderIterator.next();
			SortedSet<String> report = reports.get(reportHeader);
			String department = reportHeader.get("department");
			String resource = reportHeader.get("resource");
			String type = reportHeader.get("type");
			Iterator<String> reportIterator = report.iterator();
			while (reportIterator.hasNext()) {
				String line = reportIterator.next();
				logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", department + ":" + resource + ":" + type + ":" + line));
			}
		}
	}
		
}
	
