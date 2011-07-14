package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;

public class FuquaRestrictions implements DepartmentRestrictions {

	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);

	public boolean updateRoutingForUser(String connectorName, Properties deptProperties, HashMap<String, String> userMap) {
		String startStr = deptProperties.getProperty("blackout.start.date");
		String endStr = deptProperties.getProperty("blackout.end.date");
		SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
		String currentYear = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
		startStr += currentYear;
		endStr += currentYear;
		Date startDate, endDate;
		Date now = new Date();

		try {
			startDate = sdf.parse(startStr);
			endDate = sdf.parse(endStr);
			if (now.after(startDate) && now.before(endDate))
				return false;	// does not pass blackout dates restriction
		} catch (ParseException e) {
			String msg = EmailUsers.addLogEntry(connectorName, "FAILED", "Error parsing date: " + startStr + " or " + endStr + ": from properties for class: " + this.getClass().getName());
			logger.error(msg);
			return false;
		}

		if (!(isStudentAlumniOnly(userMap) || isStudentFUQOnly(userMap)))
			return false;	// does not pass affiliation requirement or does not pass academic career requirement

		return true;
	}

	/**
	 * Determine is the user being provisioned is currently or previously an undergrad
	 * @param userMap
	 * @return
	 */
	private boolean isStudentAlumniOnly(HashMap<String, String> userMap) {
		boolean staff = userMap.containsKey("USR_UDF_IS_STAFF") && userMap.get("USR_UDF_IS_STAFF").equals("1");
		boolean faculty = userMap.containsKey("USR_UDF_IS_FACULTY") && userMap.get("USR_UDF_IS_FACULTY").equals("1");
		boolean emeritus = userMap.containsKey("USR_UDF_IS_EMERITUS") && userMap.get("USR_UDF_IS_EMERITUS").equals("1");
		boolean affiliate = userMap.containsKey("USR_UDF_IS_AFFILIATE") && userMap.get("USR_UDF_IS_AFFILIATE").equals("1");
		boolean student = userMap.containsKey("USR_UDF_IS_STUDENT") && userMap.get("USR_UDF_IS_STUDENT").equals("1");
		
		return (!(staff || faculty || emeritus || affiliate) && student);
	}
	
	private boolean isStudentFUQOnly(HashMap<String, String> userMap) {
		Set<String> keys = userMap.keySet();
		Iterator<String> keysIterator = keys.iterator();
		ArrayList<String> careers = new ArrayList<String>();
		while (keysIterator.hasNext()) {
			String key = keysIterator.next();
			if (key.contains("USR_UDF_PSACAD"))
				careers.add(userMap.get(key));
		}
		
		if (careers.size() == 1)	// only career is Fuqua
			return true;
		
		return false;
	}

}
