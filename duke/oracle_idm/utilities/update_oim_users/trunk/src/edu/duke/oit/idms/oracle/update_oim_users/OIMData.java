package edu.duke.oit.idms.oracle.update_oim_users;

public abstract class OIMData {

	public static String getResourceName(String name) {
		String resourceName = null;
		if (name.equals("sunmail")) {
			resourceName = "COMMSDIR_PROVISIONING2";
		} else if (name.equals("exchange")) {
			resourceName = "EXCHANGE_PROVISIONING";
		}
		return resourceName;
	}
	
	public static String getGroupName(String name) {
		String groupName = null;
		if (name.equals("sunmail")) {
			groupName = "OIM_FORCE_SUNMAIL_ROUTING";
		} else if (name.equals("exchange")) {
			groupName = "OIM_FORCE_EXCHANGE_ROUTING";
		}
		return groupName;
	}
	
	public static String getCompetingGroupName(String name) {
		if (name.equals("sunmail")) {
			return (getGroupName("exchange"));
		} else {
			return (getGroupName("sunmail"));
		}
	}
	
	public static boolean isResourceValid(String name) {
		return(getResourceName(name) != null);
	}
	
	public static boolean isGroupValid(String name) {
		return(getGroupName(name) != null);
	}
}
