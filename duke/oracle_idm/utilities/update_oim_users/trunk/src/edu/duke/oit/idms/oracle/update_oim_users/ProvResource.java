package edu.duke.oit.idms.oracle.update_oim_users;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ProvResource {

	protected org.apache.log4j.Logger LOG = Logger.getLogger(ProvResource.class);
	protected static final String OPTION_PREFIX = "--";
	protected static Set<String> resources = new HashSet<String>();
	protected static Set<String> groups = new HashSet<String>();	
	protected static boolean isDeprovision = false;
	protected static OIMUsers oimUsers = new OIMUsers();

	public static void main(String[] args) {
		PropertyConfigurator.configure(System.getenv("OIM_APP_HOME") + "/conf/log4j.properties");
		
		processArguments(args);
		if (oimUsers.getNetIDs().size() == 0) {
			argsErrorMessage("No NetIDs to process.");
		} else {
			if (resources.isEmpty() && groups.isEmpty()) {
				argsErrorMessage("No action to perform on NetIDs.");
			} else {
				processUsers();
			}
		}
		System.out.println(summaryReport(oimUsers));
		System.exit(0);
	}
	
	private static void processArguments(String [] args) {
		
		if (args.length == 0) {
			argsErrorMessage("Missing arguments.");
		}
		
		int i = 0;
		while (i < args.length) {
			String option = args[i];
			if (!option.startsWith(OPTION_PREFIX)) {
				argsErrorMessage("Invalid option received: " + option);
			} else {
				option = option.substring(OPTION_PREFIX.length());
				++i;
				if (option.equals("deprovision")) {
					isDeprovision = true;
				} else {
					if (i == args.length || args[i].startsWith(OPTION_PREFIX)) {
						argsErrorMessage("Missing arguments for option: " + option);
					} else {
						while(i < args.length && !args[i].startsWith(OPTION_PREFIX)) {
							String argument = args[i];
							if (option.equals("netids")) {
								oimUsers.addNetID(argument);
							} else if (option.equals("resources")) {
								if (OIMData.isResourceValid(argument)) {
									resources.add(argument);
								} else {
									argsErrorMessage("Resource name: " + argument + " is not valid.");
								}
							} else if (option.equals("routing")) {
								if (i < args.length - 1 && !args[i +1].startsWith(OPTION_PREFIX)) {
									argsErrorMessage("Too many routing arguments received. Routing can be forced to exchange or sunmail, but not both.");
								} else {
									if (OIMData.isGroupValid(argument)) {
										groups.add(argument);
									} else {
										argsErrorMessage("Resource name: " + argument + " is not valid.");
									}
								}
							} else if (option.equals("file")) {
								if (i < args.length - 1 && !args[i +1].startsWith(OPTION_PREFIX)) {
									argsErrorMessage("Too many file arguments received.");
								} else {
									oimUsers.getNetIDsFromFile(argument);
								}
							} else {
								argsErrorMessage("Unknown option: " + option);
							}
							++i;
						}
					}
				}			
			}
		}
	}
	
	private static void processUsers() {
		UpdateOIMUsers updateUser = new UpdateOIMUsers();
		Set<String> netids = oimUsers.getNetIDs();
		Iterator<String> netidIterator = netids.iterator();
		while (netidIterator.hasNext()) {
			String netid = netidIterator.next();
			long userKey = updateUser.getUserKey(netid);
			oimUsers.setKeyForUser(netid, userKey);
			if (userKey <= 0) {
				continue;
			}
			Iterator<String> groupIterator = groups.iterator();
			while (groupIterator.hasNext()) {
				String name = groupIterator.next();
				long targetKey = updateUser.getGroupKey(OIMData.getGroupName(name));
					if (isDeprovision) {
						updateUser.removeUserFromGroup(targetKey, userKey);
					} else {
						updateUser.addUserToGroup(targetKey, userKey);
						long competingKey = updateUser.getGroupKey(OIMData.getCompetingGroupName(name));
						updateUser.removeUserFromGroup(competingKey, userKey);
					}
			}
			Iterator<String> resourceIterator = resources.iterator();
			while (resourceIterator.hasNext()) {
				String name = resourceIterator.next();
				String resourceName = OIMData.getResourceName(name);
				if (isDeprovision) {
					long newKey = updateUser.deprovisionResourceFromUser(userKey, resourceName);
					if (userKey != newKey) { // means error returned, update oimuser hash
						oimUsers.setKeyForUser(netid, newKey);
					}
					long groupKey = updateUser.getGroupKey(OIMData.getGroupName(name));
					updateUser.removeUserFromGroup(groupKey, userKey);
				} else {
					long newKey = updateUser.provisionResourceToUser(userKey, resourceName);
					if (userKey != newKey) { // means error returned, update oimuser hash
						oimUsers.setKeyForUser(netid, newKey);
					}
				}
			}
		}
		updateUser.close();
	}
	
	private static void argsErrorMessage(String msg) {
		System.out.println(msg);
		printHelp();
		System.exit(1);
	}
	
	public static void printHelp() {
		System.out.println("usage: java -jar update_oim_users.jar <OPTIONS>");
		System.out.println("\t--netids\tspace separated list of NetID(s)");
		System.out.println("\t--file\t\tfile containing new line separated list of NetIDs");
		System.out.println("\t--deprovision\tdeprovision the listed resource(s) from the list of NetID(s), default action is provisioning.");
		System.out.println("\t--resources [sunmail exchange]\tresource(s) to manage");		
		System.out.println("\t--routing [sunmail exchange]\tforce mail routing for users with both sunmail and exchange to either sunmail or exchange");
		System.out.println();
	}
	
	public static String summaryReport (OIMUsers oimUsers) {
		final String LINE_SEPARATOR = "----------------------------------------------------------------------------\n";
		final String INDENT = "   ";
		Set<String> totalNetids = oimUsers.getNetIDs();
		Set<String> alreadyProvisioned = new HashSet<String>(); // userKey == -2
		Set<String> needsConsolidation = new HashSet<String>(); // userKey == -1
		Set<String> notFound = new HashSet<String>();  // userKey == 0
		Iterator<String> totalNetidIterator = totalNetids.iterator();
		while (totalNetidIterator.hasNext()) {
			String netid = totalNetidIterator.next();
			Long userKey = oimUsers.getUsers().get(netid);
			if (userKey.equals(0L)) {
				notFound.add(netid);
			} else if (userKey.equals(-1L)) {
				needsConsolidation.add(netid);
			} else if (userKey.equals(-2L)) {
				alreadyProvisioned.add(netid);
			}
		}
		StringBuffer report = new StringBuffer();
		report.append("\n" + LINE_SEPARATOR);
		int failedCount = totalNetids.size() - (notFound.size() + needsConsolidation.size() + alreadyProvisioned.size());
		report.append(new Date() + " - Summary of OIM changes: " + failedCount + " of " + totalNetids.size() + " successfully updated.\n");
		if (totalNetids.size() != failedCount) {
			report.append("\nPlease note the following exceptions:\n");
			if (needsConsolidation.size() > 0) {
				report.append(INDENT + "Duplicate NetIDs located in OIM:\n");
				Iterator<String> needsConsolidationIterator = needsConsolidation.iterator();
				while (needsConsolidationIterator.hasNext()) {
					report.append(INDENT + INDENT + needsConsolidationIterator.next() + "\n");
				}
			}
			if (notFound.size() > 0) {
				report.append(INDENT + "NetIDs missing or deleted status in OIM:\n");
				Iterator<String> notFoundIterator = notFound.iterator();
				while (notFoundIterator.hasNext()) {
					report.append(INDENT + INDENT + notFoundIterator.next() + "\n");
				}
			}
			if (alreadyProvisioned.size() > 0) {
				report.append(INDENT + "NetIDs already provisioned OIM:\n");
				Iterator<String> alreadyProvisionedIterator = alreadyProvisioned.iterator();
				while (alreadyProvisionedIterator.hasNext()) {
					report.append(INDENT + INDENT + alreadyProvisionedIterator.next() + "\n");
				}
			}
		}
		report.append(LINE_SEPARATOR);
		return report.toString();
	}	
}
