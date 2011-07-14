package edu.duke.oit.idms.oracle.update_oim_users;

import java.util.HashMap;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcGroupNotFoundException;
import Thor.API.Exceptions.tcObjectNotFoundException;
import Thor.API.Exceptions.tcProvisioningNotAllowedException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.tcGroupOperationsIntf;
import Thor.API.Operations.tcObjectOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

public class UpdateOIMUsers {
	private static final String oimuser = "SUPPRESSED";
	private static final String oimpwd = "SUPPRESSED";
	private tcUtilityFactory moFactory = null;
	private tcUserOperationsIntf moUserUtility = null;
	private tcObjectOperationsIntf moObjectUtility = null;
	private tcGroupOperationsIntf moGroupUtility = null;
	
	public UpdateOIMUsers() {
		ConfigurationClient.ComplexSetting config = ConfigurationClient.getComplexSettingByPath("Discovery.CoreServer");
		try {
			moFactory = new tcUtilityFactory(config.getAllSettings(), oimuser, oimpwd);
		} catch (Exception e) {
			System.out.println("Could not get instance of tcUtilityFactory from OIM.");
			e.printStackTrace();
			close();
			System.exit(1);
		}
	}
	
	public long getUserKey(String netid) {
		tcResultSet userKeySet = null;
		long userKey = 0;
		try {
			if (moUserUtility == null)
				moUserUtility = (tcUserOperationsIntf) moFactory.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			HashMap<String, String> queryAttributes = new HashMap<String, String>();
			if(!(netid.equals("") || netid == null)) {
				queryAttributes.put("USR_UDF_UID", netid);
				queryAttributes.put("Users.Status", "active");
			}			
			String [] resultsAttributes = new String [] {"USR_UDF_UID", "Users.Key"};
			userKeySet = moUserUtility.findUsersFiltered(queryAttributes, resultsAttributes);
			if (userKeySet.getRowCount() == 1) {
				userKeySet.goToRow(0);
				userKey = userKeySet.getLongValue("Users.Key");
			} else if (userKeySet.getRowCount() > 1) {
				userKey = -1; // use -1 to signify consolidation needed, 0 to signify netid not found
			}
		} catch (tcAPIException e) {
			System.out.println("Message: " + e.getMessage() + ": ErrorCode: " + e.getErrorCode());
			e.printStackTrace();
			close();
			System.exit(1);
		} catch (tcColumnNotFoundException e) {
			System.out.println("ERROR - Invalid column name(s): " + "USR_UDF_UID" + ", " + "Users.Key" + " used to retrieve User Keys from OIM. Exiting . . .");
			e.printStackTrace();
			close();
			System.exit(1);
		}
		return userKey;
	}
	
	public long getGroupKey(String oimName) {
		HashMap<String, String> groupAttributes = new HashMap<String, String>();
		String columnName = "Groups.Key";
		groupAttributes.put("Groups.Group Name", oimName);
		tcResultSet groupResults = null;
		try {
			if (moGroupUtility == null)
				moGroupUtility = (tcGroupOperationsIntf) moFactory.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
				
			groupResults = moGroupUtility.findGroups(groupAttributes);
			if (groupResults.getRowCount() == 0) {
				System.out.println("Group does not exist: " + oimName);
				close();
				System.exit(1);
			} else if (groupResults.getRowCount() > 1) {
				System.out.println("Multiple results returned: " + oimName);
				close();
				System.exit(1);
			}
			groupResults.goToRow(0);			
			return (groupResults.getLongValue(columnName));
		} catch (tcAPIException e) {
			System.out.println("Could not get Group Key for Group: " + oimName);
			e.printStackTrace();
			close();
			System.exit(1);
		} catch (tcColumnNotFoundException e) {
			System.out.println("Invalid Column Name used to retrieve Group: " + columnName);
			e.printStackTrace();
			close();
			System.exit(1);
		}
		return 0;
	}
	
	public long getResourceKey(String oimName) {
		HashMap<String, String> resourceAttributes = new HashMap<String, String>();
		String columnName = "Objects.Key";
		resourceAttributes.put("Objects.Name", oimName);
		try {
			if (moObjectUtility == null)
				moObjectUtility = (tcObjectOperationsIntf) moFactory.getUtility("Thor.API.Operations.tcObjectOperationsIntf");
			tcResultSet resourceResults = moObjectUtility.findObjects(resourceAttributes);
			if (resourceResults.getRowCount() == 0) {
				System.out.println("Resource does not exist: " + oimName);
				close();
				System.exit(1);
			} else if (resourceResults.getRowCount() > 1) {
				System.out.println("Multiple results returned: " + oimName);
				close();
				System.exit(1);
			}
			resourceResults.goToRow(0);			
			return (resourceResults.getLongValue(columnName));
		} catch (tcAPIException e) {
			System.out.println("Could not get Group Key for Group: " + oimName);
			e.printStackTrace();
			close();
			System.exit(1);
		} catch (tcColumnNotFoundException e) {
			System.out.println("Invalid Column Name used to retrieve Group: " + columnName);
			e.printStackTrace();
			close();
			System.exit(1);
		}
		return 0;
	}
	
	public void manageGroupMembership(boolean addToGroup, long groupKey, long userKey) {
		try {
			if (moGroupUtility == null)
				moGroupUtility = (tcGroupOperationsIntf) moFactory.getUtility("Thor.API.Operations.tcGroupOperationsIntf");
			if (addToGroup && !moGroupUtility.isUserGroupMember(groupKey, userKey))
				moGroupUtility.addMemberUser(groupKey, userKey);
			else if (!addToGroup && moGroupUtility.isUserGroupMember(groupKey, userKey))
				moGroupUtility.removeMemberUser(groupKey, userKey);
		} catch (tcAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (tcGroupNotFoundException e) {
			System.out.println("Invalid Group Key: "+ groupKey + " used in UpdatOIMUsers.manageGroupMembership().");
		} catch (tcUserNotFoundException e) {
			System.out.println("Invalid User Key: "+ userKey + " used in UpdatOIMUsers.manageGroupMembership().");
		}
	}

	public void removeUserFromGroup(long groupKey, long userKey) {
		manageGroupMembership(false, groupKey, userKey);
	}

	public void addUserToGroup(long groupKey, long userKey) {
		manageGroupMembership(true, groupKey, userKey);
	}
	
	public long manageResourceProvisioning(boolean provisionObject, long userKey, String resourceName) {
		long objectKey = 0;
		try {
			if (moUserUtility == null)
				moUserUtility = (tcUserOperationsIntf) moFactory.getUtility("Thor.API.Operations.tcUserOperationsIntf");
			if (provisionObject) {
				objectKey = getResourceKey(resourceName);
				if (moUserUtility.canResourceBeProvisioned(userKey, objectKey)) {
					moUserUtility.provisionObject(userKey, objectKey);
				}
			} else {
				revokeResource(moUserUtility, userKey, resourceName);
			}
		} catch (tcAPIException e) {
			if (e.getMessage().contains("Multiple Instances Not Allowed")) {
				return -2L;
			} else {
				e.printStackTrace();
			}
		} catch (tcObjectNotFoundException e) {
			System.out.println("Invalid Object Key: "+ objectKey + " used in UpdatOIMUsers.manageResourceProvisioning().");
		} catch (tcProvisioningNotAllowedException e) {
			System.out.println("User: "+ userKey + " cannot be provisioned resource: " + resourceName);
		} catch (tcUserNotFoundException e) {
			System.out.println("Invalid User Key: "+ userKey + " used in UpdatOIMUsers.manageResourceProvisioning.");
		}
		return userKey;
	}
	
	public long deprovisionResourceFromUser(long userKey, String resourceName) {
		return (manageResourceProvisioning(false, userKey, resourceName));
	}

	public long provisionResourceToUser(long userKey, String resourceName) {
		return (manageResourceProvisioning(true, userKey, resourceName));
	}
	
	private boolean revokeResource(tcUserOperationsIntf moUserUtility, long userKey, String resourceName) {
		tcResultSet results;
		try {
			results = moUserUtility.getObjects(userKey);
			for (int rowIndex = 0; rowIndex < results.getRowCount(); ++rowIndex) {
				results.goToRow(rowIndex);
				if (results.getStringValue("Objects.Name").equals(resourceName) &&
						!results.getStringValue("Objects.Object Status.Status").equals("Revoked")) {
					moUserUtility.revokeObject(userKey, results.getLongValue("Users-Object Instance For User.Key"));
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public void close() {
		if (moFactory != null)
			moFactory.close();
		if (moUserUtility != null)
			moUserUtility.close();
		if (moObjectUtility != null)
			moObjectUtility.close();
		if (moGroupUtility != null)
			moGroupUtility.close();
	}
}
