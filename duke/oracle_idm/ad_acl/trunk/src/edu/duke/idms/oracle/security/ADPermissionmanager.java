package edu.duke.idms.oracle.security;

import java.util.Set;


import java.util.Iterator;

public class ADPermissionmanager implements PermissionManager {

	public void addPermission(Object context, SecuredResource SR,
			Set<Permission> perms) {
		MsAce [] aces = new MsAce[SR.sd.Dacl.AceCount];
		Iterator<Permission> iter = perms.iterator();
		int ai = 0;
		while (iter.hasNext()) {
			aces[ai++] = convertPermissionToAce(context,(ADPermission) iter.next());
		}
		for (int i = 0; i < ai; i++) {
			// Perform addition
			SR.addAceDacl(aces[i]);
		}
		if (! SR.updateSecurityDescriptor()) {
			throw new RuntimeException("Failed to write updated SR to repository");
		}
	}
	
	public void addPermission(Object context,SecuredResource SR, Permission perm) {
		MsAce ace = convertPermissionToAce(context,(ADPermission) perm);
		SR.addAceDacl(ace);
		if (! SR.updateSecurityDescriptor()) {
			throw new RuntimeException("Failed to write updated SR to repository");
		}
	}

	public boolean hasPermission(Object context, SecuredResource SR,
			Permission P) {

		// Start by constructing an MsAce from the Permission 
		MsAce ace = convertPermissionToAce(context,P);
		if (! SR.sd.Dacl.containsAce(ace)) {
			// Not there -- return false
			return false;
		} 
		return true;
	}

	public void removePermission(Object context, SecuredResource SR,
			Set<Permission> perms) {
		// Atomic remove for a set of permissions that not only removes the permissions
		// but also writes the SR back to the repository with changes before returning.
		//
		MsAce [] aces = new MsAce[SR.sd.Dacl.AceCount];
		Iterator<Permission> iter = perms.iterator();
		int ai = 0;
		while (iter.hasNext()) {
			aces[ai++] = convertPermissionToAce(context,(ADPermission) iter.next());
		}
		for (int i = 0; i < ai; i++) {
			// Perform removals 
			SR.removeAceDacl(aces[i]);
		}
		if (! SR.updateSecurityDescriptor()) {
			throw new RuntimeException("Failed to write updates SR to repository");
		}
	}
	
	public void removePermission(Object context, SecuredResource SR, Permission perm) {
		MsAce ace = convertPermissionToAce(context, (ADPermission) perm);
		SR.removeAceDacl(ace);
		if (! SR.updateSecurityDescriptor()) {
			throw new RuntimeException("Failed to write updated SR to repository");
		}
	}
	
	private MsAce convertPermissionToAce(Object context, Permission P) {
		// Here, we take a Permission object (probably an ADPermission, but it doesn't matter)
		// and return an MsAce object that reflects that permission.
		// This is the bridge between the Permission interface and the AD ACL interface.
		// The context passed in in this case is an ADConnectionWrapper
		//
		// Make sure it is...
		if (! context.getClass().getName().contains("ADConnectionWrapper")) {
			throw new RuntimeException("context argument to convertPermissionToAce must be of class ADConnectionWrapper");
		}
		
		// Convert the subject DN into an SID for use in the MsAce
		
		ADConnectionWrapper AD = (ADConnectionWrapper) context;
		String subject = P.SubjectDN;
		byte [] sidbytes = AD.getSIDfromDN(subject);
		MsSID sid = new MsSID(sidbytes);
		
		// Convert the permission qualifiers
		
		byte ACLType = 0;
		if (P.Mode.equals(PermissionMode.PERM_ALLOW)) {
			if (P.AttributeName != null || P.InheritorName != null) {
				// Extended
				ACLType = MsAce.ACETYPE_ACCESS_ALLOWED_OBJECT;
			} else {
				ACLType = MsAce.ACETYPE_ACCESS_ALLOWED;
			}
		} else {
			if (P.AttributeName != null || P.InheritorName != null) {
				// Extended
				ACLType = MsAce.ACETYPE_ACCESS_DENIED_OBJECT;
			} else {
				ACLType = MsAce.ACETYPE_ACCESS_DENIED;
			}
		}
		
		byte Inherit = 0;
		if (P.Inherit.equals(PermissionInheritance.PERM_SELF)) {
			Inherit = MsAce.INHERIT_NONE;
		} else if (P.Inherit.equals(PermissionInheritance.PERM_INHERIT)) {
			Inherit = MsAce.INHERIT_CHILDREN_ONLY;
		} else if (P.Inherit.equals(PermissionInheritance.PERM_SELF_AND_INHERIT)) {
			Inherit = MsAce.INHERIT_THIS_AND_CHILDREN;
		}
		
		byte [] Mask = null;
		if (P.Operation.equals(PermissionOperation.PERM_READ)) {
			if (P.AttributeName != null) {
				// attribute specific
				Mask = MsAce.EXTENDED_READ_PROP;
			} else {
				Mask = MsAce.READ_OBJECT;
			}
		}
		if (P.Operation.equals(PermissionOperation.PERM_WRITE)) {
			if (P.AttributeName != null) {
				// attribute specific
				Mask = MsAce.EXTENDED_WRITE_PROP;
			} else {
				Mask = MsAce.WRITE_OBJECT;
			}
		}
		if (P.Operation.equals(PermissionOperation.PERM_READ_AND_WRITE)) {
			if (P.AttributeName != null) {
				// attribute specific
				Mask = MsAce.EXTENDED_READ_WRITE_PROP;
			} else {
				Mask = MsAce.READ_WRITE_OBJECT;
			}
		}
		if (P.Operation.equals(PermissionOperation.PERM_CREATE)) {
			// no sense to create for attributes only
			Mask = MsAce.READ_WRITE_CREATE;
		}
		if (P.Operation.equals(PermissionOperation.PERM_DELETE)) {
			// no sense to delete for attributes only
			Mask = MsAce.DELETE_CHILDREN;
		}
		if (P.Operation.equals(PermissionOperation.PERM_FULL_CONTROL)) {
			Mask = MsAce.FULL_CONTROL;
		}
		
		if (P.AttributeName == null && P.InheritorName == null) {
			// No extended operations, so we have what we need for a creation now
			return new MsAce(ACLType,Inherit,Mask,sid);
		} else {
			// We need to construct extended attribute(s)
			
			MsGUID attrguid = null;
			if (P.AttributeName != null) {
				attrguid = new MsGUID(AD.getAttributeGUIDfromName(P.AttributeName));
			}
			MsGUID InheritClass = null;
			if (P.InheritorName != null) {
				InheritClass = new MsGUID(AD.getAttributeGUIDfromName(P.InheritorName));
			}
			return new MsAce(ACLType,Inherit,Mask,attrguid,InheritClass,sid);
		}
	}
	
	// Test harness main routine
	
	public static void main(String [] args) {
		// Assume arguments of the form:
		// [command] <add|remove> <allow|deny> <targetDN> <read|write|readwrite|full> <object|attrname> <sAMAccountName> <yes|no>
		//
		
		if (args.length != 7) {
			System.err.println("Syntax:  [command] <add|remove> <allow|deny> <targetDN> <read|write|readwrite|full> <object|attrname> <sAMAccountName> <yes|no>");
			return;
		}
		// And perform the change to the AD (where yes/no controls inheritance or no inheritance
		//
		boolean isAdd = args[0].equalsIgnoreCase("add");
		boolean isAllow = args[1].equalsIgnoreCase("allow");
		String targetDN = args[2];
		String operation = args[3];
		boolean isObject = args[4].equalsIgnoreCase("object");
		String attr = args[4];
		String sAMAccountName = args[5];
		boolean isInherit = args[6].equalsIgnoreCase("yes");
		
		String LDAPURL = "ldap://SUPPRESSED:636";
		String LDAPUSER = "SUPPRESSED@SUPPRESSED";
		String LDAPPASSWORD = "SUPPRESSED";
		
		// Get an AD connection
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		
		// Get the SR for the passed in DN
		SecuredResource SR = new SecuredResource(targetDN);
		
		// Parse out an ADPermission object
		ADPermission adp = null;
		
		String subject= null;
		PermissionMode mode = null;
		PermissionInheritance inherit = null;
		PermissionOperation op = null;
		
		if (isAllow) {
			mode = PermissionMode.PERM_ALLOW;
		} else {
			mode = PermissionMode.PERM_DENY;
		}
		
		if (operation.equals("read")) {
			op = PermissionOperation.PERM_READ;
		} else if (operation.equals("write")) {
			op = PermissionOperation.PERM_WRITE;
		} else if (operation.equals("readwrite")) {
			op = PermissionOperation.PERM_READ_AND_WRITE;
		} else if (operation.equals("full")) {
			op = PermissionOperation.PERM_FULL_CONTROL;
		}
		
		subject = AD.convertSANtoDN(sAMAccountName);
		if (isInherit) {
			inherit = PermissionInheritance.PERM_SELF_AND_INHERIT;
		} else {
			inherit = PermissionInheritance.PERM_SELF;
		}
		if (! isObject) {
			adp = new ADPermission(subject,mode,inherit,op,attr,null);
		} else {
			adp = new ADPermission(subject,mode,inherit,op,null,null);
		}
		
		ADPermissionmanager adpm = new ADPermissionmanager();
		if (isAdd) {
			if (adpm.hasPermission(AD,SR,adp)) {
				System.err.println("Error -- addition of permission already in ACL");
			}
			adpm.addPermission(AD,SR,adp);
		} else {
			if (! adpm.hasPermission(AD,SR,adp)) {
				System.err.println("Error -- removal of permission not found in ACL");
			}
			adpm.removePermission(AD,SR,adp);
		}
	}

}
