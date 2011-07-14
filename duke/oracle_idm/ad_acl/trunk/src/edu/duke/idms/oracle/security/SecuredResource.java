package edu.duke.idms.oracle.security;

import java.io.File;
import java.io.FileInputStream;

public class SecuredResource {
	public SecurityDescriptor sd;
	public String objectName;
	public long version;
	
	private static final String LDAPURL = "ldap://SUPPRESSED:636";
	private static final String LDAPUSER = "SUPPRESSED@SUPPRESSED";
	private static final String LDAPPASSWORD = "SUPPRESSED";
	
	public SecuredResource() {
		// Create a new null SecuredResource instance
		sd = new SecurityDescriptor();
		objectName = null;
		version = -1;  // nulls don't have versions
	}
	
	public SecuredResource(String name) {
		// Given a DN or a sAMAccountName, construct a SecuredResource from that DN by finding and loading its 
		// associated ntSecurityDescriptor into the sd (or by making it a null SR if the 
		// given dn doesn't exist in the directory.
		
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		byte[] sdbytes = null;
		// try looking for the DN first
		if (name.contains(",")) {
			// might be a DN...
			sdbytes = AD.getNTSecurityDescriptor(name);
		}
		if (sdbytes == null) {
			// name value was not a valid DN -- either it's a sAMAccountName or it's a failure.
			//
			String dn = AD.convertSANtoDN(name);
			if (dn != null) {
				sdbytes = AD.getNTSecurityDescriptor(dn);
				name = dn;
			} else {
				sd = new SecurityDescriptor();
				objectName = null;
				return;
			}
		}
		if (sdbytes != null) {
			sd = new SecurityDescriptor(sdbytes);
			objectName = name;
		} else {
			sd = new SecurityDescriptor();
			objectName = null;
		}
		version = AD.getduADSecDescVersion(name);
		
	}
	
	@Override public String toString() {
		String retval = "";
		retval += "ResourceName:  " + objectName;
		retval += "Security Descriptor:\n" + sd.toString();
		return retval;
	}
	
	public void addAceDacl(MsAce ace) {
		sd.addAceDacl(ace);
	}
	
	public void removeAceDacl(MsAce ace) {
		sd.removeAceDacl(ace);
	}
	
	public byte[] getSID() {
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		return AD.getSIDfromDN(objectName);
	}
	
	public byte[] getGUID() {
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		return AD.getGUIDfromDN(objectName);
	}
	
	public boolean updateSecurityDescriptor() {
		// Write the current value of the ntSecurityDescriptor in the object back to the AD entry for this object.
		// Return true if successful, false if not.  Presumably, on failure, the caller will re-try whatever it 
		// was changing, but we don't actually care here.
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		if (AD.replaceNtSecurityDescriptor(objectName,version,sd.Serialize())) {
			// Successful update
			if (version >= 0) {
				version += 1;  // increment version number after successful update
			}
			return true; // successful
		}
		return false; // unsuccessful
	}
	
	//This is a bit of an abomination, but I don't really want to spawn a whole new class for this right now...
	public byte[] getAttributeGUID(String attr) {
		// Get the GUID of an attribute we want to use for an ACE
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		return AD.getAttributeGUIDfromName(attr);
	}
	
	public static void main(String args[]) {
		String filename = "";
		byte[] bytes = null;
		
		
		//if there's only one argument, load up a file by that name
		if (args.length == 1) {
			filename = args[0];
			File inputfile = new File(filename);
			FileInputStream instream;
				
			try {
				instream = new FileInputStream(inputfile);
			} catch (Exception e) {
				throw new RuntimeException("Failed opening file with " + e.getMessage(),e);
			}
			try {
				long length = inputfile.length();
				bytes = new byte[(int) length];
				int offset = 0;
				int numRead = 0;
				while (offset < bytes.length && (numRead = instream.read(bytes,offset,(int) bytes.length-offset)) >= 0) {
					offset += numRead;
				}
				instream.close();
			} catch (Exception e) {
				throw new RuntimeException("Reading input failed: " + e.getMessage(),e);
			}
		} else {
			
			// assume we need to retrieve a value from the AD and decode it directly
			System.out.println("Getting new secured resource");
			SecuredResource SR = new SecuredResource(args[1]);  // DN or sAMAccountName
			System.out.println("New resource is " + SR);
			// We should now have an SD for this resource, so we're good to go...
			if (SR != null && SR.objectName != null) {
				// the resource is real

			  
				System.out.println("Testing removal of ACEs");
				SR.removeAceDacl(new MsAce(MsAce.ACETYPE_ACCESS_ALLOWED_OBJECT,MsAce.INHERIT_CHILDREN_ONLY,MsAce.EXTENDED_READ_PROP,new MsGUID(SR.getAttributeGUID("eduPersonPrimaryAffiliation")),null,new MsSID(new SecuredResource("gr-it_admins_systemOfRecord-Geo-A&S-TrinityCollege-orgs").getSID())));
				SR.removeAceDacl(new MsAce(MsAce.ACETYPE_ACCESS_ALLOWED,MsAce.INHERIT_CHILDREN_ONLY,MsAce.FULL_CONTROL,new MsSID(new SecuredResource("shilen").getSID())));
				System.out.println("Testing write of modified ACL again");
				if (SR.updateSecurityDescriptor()) {
					System.out.println("Successful");
				} else {
					System.out.println("Unsuccessful");
				} 
			}
		}

	}
}
