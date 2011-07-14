package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.util.HashMap;
import java.util.Properties;

public interface DepartmentRestrictions {
	
	public boolean updateRoutingForUser(String connectorName, Properties deptProperties, HashMap<String, String> userMap);

}
