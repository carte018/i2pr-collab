package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

@SuppressWarnings("unchecked")
public class DepartmentRestrictionsData {
	
	private static DepartmentRestrictionsData cfg = null;
	private Properties allProperties = new Properties();
	private HashMap<String, Class<DepartmentRestrictions>> deptRestrictions = new HashMap<String, Class<DepartmentRestrictions>>();
	private final String DEPT_RESTRICTIONS_PREFIX = "restrictions.class.";


	/**
	 * 
	 * Get instance of class.
	 * @return DepartmentRestrictionsConfig
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static DepartmentRestrictionsData getInstance(String fileName) throws IOException, ClassNotFoundException {
		if (cfg == null)
			return new DepartmentRestrictionsData(fileName);
		return cfg;
	}
	
	public static DepartmentRestrictionsData getInstance() throws IOException, ClassNotFoundException {
		if (cfg == null)
			return new DepartmentRestrictionsData();
		return cfg;
	}
	
	public DepartmentRestrictionsData(String fileName) throws IOException, ClassNotFoundException {
		// default file location

	    allProperties.load(new FileInputStream(System.getProperty("OIM_COMMON_HOME", fileName)));
	    
	    Enumeration<String> propertyNames = (Enumeration<String>)allProperties.propertyNames();
		while (propertyNames.hasMoreElements()) {
			String key = propertyNames.nextElement().toLowerCase();
			String value = allProperties.getProperty(key);

			if (key.startsWith(DEPT_RESTRICTIONS_PREFIX)) {
				int keyStart = DEPT_RESTRICTIONS_PREFIX.length();
				String hashKey = key.substring(keyStart).toLowerCase();
				Class<DepartmentRestrictions> deptClass;
				deptClass = (Class<DepartmentRestrictions>) Class.forName(value);
				if (deptClass != null)
					deptRestrictions.put(hashKey, deptClass);
			}
		}
	}
	
	public DepartmentRestrictionsData() throws IOException, ClassNotFoundException {
		this("/opt/idms/oracle_idm/common/conf/provisioning_connectors/EMAIL_TASKS/DepartmentRestrictions.conf");
	}
	
	public boolean hasClassDefined(String dept) {
		return (deptRestrictions.containsKey(dept.toLowerCase()));
	}

	public DepartmentRestrictions getDeptRestrictionsClass(String dept) throws InstantiationException, IllegalAccessException {
			return (deptRestrictions.get(dept).newInstance());
	}
	
	public Properties getAllProperties() {
		return allProperties;
	}
	
	public int getDeptPriority(String dept) {
		String priority = allProperties.getProperty(dept + ".priority", "9999");
		return (Integer.parseInt(priority));
	}
	
	public Properties getDeptProperties(String dept) {
		Properties properties = new Properties();
		Enumeration<String> propertyNames = (Enumeration<String>)allProperties.propertyNames();
		while (propertyNames.hasMoreElements()) {
			String key = propertyNames.nextElement();
			String value = allProperties.getProperty(key);

			if (key.startsWith(dept)) {
				key = key.substring(dept.length() + 1);
				properties.put(key, value);
			}
		}
		return properties;
	}
}
