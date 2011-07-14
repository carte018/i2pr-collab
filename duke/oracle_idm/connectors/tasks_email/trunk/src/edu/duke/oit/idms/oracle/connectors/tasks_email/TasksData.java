package edu.duke.oit.idms.oracle.connectors.tasks_email;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

/**
 * Read and store all properties related to the instantiating task.
 * @author Michael Meadows (mm310)
 *
 */

public class TasksData {
	
	private static TasksData cfg = null;
	private Properties properties = new Properties();
	private String task;
	private HashSet<String> routingEntries = new HashSet<String>();
	private HashSet<String> departments = new HashSet<String>();
	private String connectorName;
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);

	public static TasksData getInstance(String connectorName, String fileName) {
		if (cfg == null)
			return new TasksData(connectorName, fileName);
		return cfg;
	}
	
	public static TasksData getInstance(String connectorName) {
		if (cfg == null)
			return new TasksData(connectorName);
		return cfg;
	}
	
	/**
	 * Instantiate the properties data for the specific connector in use
	 * @param connectorName
	 * @param fileName
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	@SuppressWarnings("unchecked")	
	public TasksData(String connector, String fileName) {
		connectorName = connector;
		if (connectorName.equals("COMMSDIR_PROVISIONING2"))
			task = "sunmail.routing";
		else if (connectorName.equals("EXCHANGE_PROVISIONING"))
			task = "exchange.routing";
		else if (connectorName.equals("REVOKE_PROVISIONING"))
			task = "revoke";
		else if (connectorName.equals("PROV_REPORTING"))
			task = "notify";
		
		// default file location
		Properties allProperties = new Properties();
	    try {
			allProperties.load(new FileInputStream(System.getProperty("OIM_COMMON_HOME", fileName)));
		} catch (Exception e) {
			String exceptionName = e.getClass().getSimpleName();
			if (exceptionName.contains("FileNotFoundException") || exceptionName.contains("IOException")) {
				throw new RuntimeException(EmailUsers.addLogEntry(connectorName, "ERROR", "Could not retrieve properties from file: " + fileName), e);
			} else {
				logger.info(EmailUsers.addLogEntry(connectorName, "DEBUG", "Unhandled exception caught: " + e.getClass().getCanonicalName() + ":" + e.getMessage()));
			}
		}

	    Enumeration<String> propertyNames = (Enumeration<String>)allProperties.propertyNames();
		while (propertyNames.hasMoreElements()) {
		      String key = propertyNames.nextElement();
		      String value = allProperties.getProperty(key);

		      if (key.startsWith(task)) {
		    	  if (Pattern.matches(".*line\\.\\d.*", key))
		    		  routingEntries.add(value);
		    	  else if (Pattern.matches(".*dept\\.\\d.*", key))
		    		  departments.add(value);
		    	  else
		    		  properties.setProperty(key, value);
		      }
		}
	}
	
	/**
	 * Instantiate using the default properties.conf location
	 * @param taskName
	 */
	public TasksData (String taskName) {
		this(taskName, "/opt/idms/oracle_idm/common/conf/provisioning_connectors/EMAIL_TASKS/properties.conf");
	}
	
	/**
	 * Return String representation of the class. Useful for debugging.
	 */
	@SuppressWarnings("unchecked")
	public String toString() {
		String indent = "  ";
		StringBuffer string = new StringBuffer();
		string.append("Properties:\n");
	    Enumeration<String> propertyNames = (Enumeration<String>)properties.propertyNames();
	    
		while (propertyNames.hasMoreElements()) {
		      String key = propertyNames.nextElement();
		      String value = properties.getProperty(key);
		      string.append(indent + "key=" + key + ", value=" + value + "\n");
		}
		
		if (!routingEntries.isEmpty()) {
			Iterator<String> i = routingEntries.iterator();
			string.append("Routing Entries:\n");
			while (i.hasNext())
				string.append(indent + i.next() + "\n");
		}
		
		return string.toString();	
	}
	
	public String getProperty (String key) {
		return properties.getProperty(task + "." + key);
	}
	
	/**
	 * Return routing entry list
	 */
	public HashSet<String> getRoutingEntries() {
		return routingEntries;
	}
	
	/**
	 * Return departments list
	 */
	public HashSet<String> getDepartments() {
		return departments;
	}
	
	/**
	 * Return reports types for specified department
	 * @param dept
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public HashSet<String> getReportTypeForDept(String dept) {
		dept = dept.toLowerCase();
		HashSet<String> reportTypes = new HashSet<String>();
		if (departments.contains(dept)) {
			Enumeration<String> propertyNames = (Enumeration<String>)properties.propertyNames();
			while (propertyNames.hasMoreElements()) {
			      String key = propertyNames.nextElement();
			      if (Pattern.matches(task + "\\." + dept + "\\.report\\.type\\.\\d.*", key)) {
				      String value = properties.getProperty(key);
				      value = value.substring(0,1).toUpperCase() + value.substring(1).toLowerCase();
				      reportTypes.add(value);
			      }
			}
		}
		return reportTypes;
	}
	
	/**
	 * Return reports types for specified department
	 * @param dept
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public HashSet<String> getReportResourcesForDept(String dept) {
		dept = dept.toLowerCase();
		HashSet<String> reportResources = new HashSet<String>();
		if (departments.contains(dept)) {
			Enumeration<String> propertyNames = (Enumeration<String>)properties.propertyNames();
			while (propertyNames.hasMoreElements()) {
			      String key = propertyNames.nextElement();
			      if (Pattern.matches(task + "\\." + dept + "\\.report\\.resource\\.\\d.*", key)) {
				      String value = properties.getProperty(key).toUpperCase();
				      reportResources.add(value);
			      }
			}
		}
		return reportResources;
	}
	
	/**
	 * Returns the report column names mapped to their actual table column names
	 * @param dept
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public SortedMap<String, String> getColumnMappingsForDept(String resource, String dept) {
		dept = dept.toLowerCase();
		SortedMap<String, String> mappings = new TreeMap<String, String>();
		if (departments.contains(dept)) {
			Enumeration<String> propertyNames = (Enumeration<String>)properties.propertyNames();
			while (propertyNames.hasMoreElements()) {
				String key = propertyNames.nextElement();
			      if (Pattern.matches(task + "\\." + dept + "\\.report\\.header\\..*", key)) {
				      String value = properties.getProperty(key);
			    	  key = key.substring(key.lastIndexOf(".header.") + ".header.".length() );
						if (key.contains("%resource%"))
							key = key.replace("%resource%", resource);
			    	  mappings.put(key, value);
			      }
			}
		}
		return mappings;
	}
	
	public String getTaskName() {
		return task;
	}

}
