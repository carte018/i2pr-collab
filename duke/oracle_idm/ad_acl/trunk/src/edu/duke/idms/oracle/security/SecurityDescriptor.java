package edu.duke.idms.oracle.security;

// import java.util.ArrayList;
// import java.util.Arrays;
import java.io.*;


/* 
 * @author: rob
 * 
 * Class for processing of security descriptors.  Primarily 
 * used for parsing and manipulating ntSecurityDescriptor values
 * from AD objects, but can also be used for other MS_Security_Descriptor
 * instances (such as those for Exchange and those for file system permmissions 
 * within SMB network shares).
 */

public class SecurityDescriptor {
	
	byte Revision;			// SD Object Revision (one-byte integer)
	byte Sbz1;				// SD special data (only significant if a resource manager is involved and the RM bit in Control is set)
	byte[] Control;			// 2-byte Control bitfield
	int OwnerOffset;		// 32-bit integer offset of the owner SID
	byte [] OwnerOffsetBytes;	// OwnerOffset in lsb byte-order (4 bytes)
	int GroupOffset;		// 32-bit integer offset of the group SID
	byte [] GroupOffsetBytes;	// GroupOffset in lsb byte-order (4 bytes)
	int SaclOffset;			// 32-bit integer offset of the SACL start
	byte [] SaclOffsetBytes;	// SaclOffset in lsb byte-order (4 bytes)
	int DaclOffset;			// 32-bit integer offset of the DACL start
	byte [] DaclOffsetBytes;	// DaclOffset in lsb byte-order (4 bytes)
	MsAcl Sacl;					// System acl, stored at the SaclOffset
	MsAcl Dacl;					// System acl, stored at the DaclOffset 
	MsSID OwnerSID;				// SID of the owner of the object
	MsSID GroupSID;				// SID of the group of the object
	int Size;					// SD size including the owner and group values
	
	private static String LDAPURL = "ldap://SUPPRESSED:636";
	private static String LDAPUSER = "SUPPRESSED@SUPPRESSED";
	private static String LDAPPASSWORD = "SUPPRESSED";
	
	protected SecurityDescriptor() {
		// null build
		Revision = (byte) 1;
		Sbz1 = (byte) 0;
		Control = new byte[2];
		OwnerOffset = 0;
		OwnerOffsetBytes = new byte[4];
		GroupOffset = 0;
		GroupOffsetBytes = new byte[4];
		SaclOffset = 0;
		SaclOffsetBytes = new byte[4];
		DaclOffset = 0;
		DaclOffsetBytes = new byte[4];
		Sacl = new MsAcl();
		Dacl = new MsAcl();
		OwnerSID = new MsSID();
		GroupSID = new MsSID();
		Size=0;
	}
	
	protected SecurityDescriptor(byte[] bytes) {
		Revision = bytes[0];
		Sbz1 = bytes[1];
		Control = new byte[] {bytes[2],bytes[3]};
		OwnerOffsetBytes = new byte[] {bytes[4],bytes[5],bytes[6],bytes[7]};
		OwnerOffset = (int) ((((bytes[7] < 0)?bytes[7]+256:bytes[7])<<24) + (((bytes[6]<0)?bytes[6]+256:bytes[6]) << 16) + (((bytes[5]<0)?bytes[5]+256:bytes[5]) << 8) + ((bytes[4]<0)?bytes[4]+256:bytes[4]));
		GroupOffsetBytes = new byte[] {bytes[8],bytes[9],bytes[10],bytes[11]};
		GroupOffset = (int) ((((bytes[11] < 0)?bytes[11]+256:bytes[11])<<24) + (((bytes[10]<0)?bytes[10]+256:bytes[10])<<16) + (((bytes[9]<0)?bytes[9]+256:bytes[9])<<8) + ((bytes[8]<0)?bytes[8]+256:bytes[8]));
		SaclOffsetBytes = new byte[] {bytes[12],bytes[13],bytes[14],bytes[15]};
		SaclOffset = (int) ((((bytes[15]<0)?bytes[15]+256:bytes[15])<<24) + (((bytes[14]<0)?bytes[14]+256:bytes[14])<<16) + (((bytes[13]<0)?bytes[13]+256:bytes[13])<<8) + ((bytes[12]<0)?bytes[12]+256:bytes[12]));
		DaclOffsetBytes = new byte[] {bytes[16],bytes[17],bytes[18],bytes[19]};
		DaclOffset = (int) ((((bytes[19]<0)?bytes[19]+256:bytes[19])<<24) + (((bytes[18]<0)?bytes[18]+256:bytes[18])<<16) + (((bytes[17]<0)?bytes[17]+256:bytes[17])<<8) + ((bytes[16]<0)?bytes[16]+256:bytes[16]));
		byte[] newSacl = new byte[bytes.length - SaclOffset];
		for (int foo = 0; foo < bytes.length - SaclOffset; foo++) {
			newSacl[foo] = bytes[foo+SaclOffset];
		}
		Sacl = new MsAcl(newSacl);
		byte[] newDacl = new byte[bytes.length - DaclOffset];
		for (int foo = 0; foo < bytes.length - DaclOffset; foo++) {
			newDacl[foo] = bytes[foo+DaclOffset];
		}
		Dacl = new MsAcl(newDacl);
		byte [] sidblock = new byte[bytes.length - OwnerOffset];
		for (int foo=0; foo < bytes.length - OwnerOffset; foo ++) {
			sidblock[foo] = bytes[foo+OwnerOffset];
		}
		OwnerSID = new MsSID(sidblock);
		byte [] gsidblock = new byte[bytes.length - GroupOffset];
		for (int foo = 0; foo < bytes.length - GroupOffset; foo ++) {
			gsidblock[foo] = bytes[foo + GroupOffset];
		}
		GroupSID = new MsSID(gsidblock);
		
	}
	
	@Override public String toString() {
		String retval = "";
		ADConnectionWrapper ad = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		retval += "Revision: " + (int) Revision + ", Sbz1: " + Sbz1 + ", OwnerOffset: " + OwnerOffset + ", GroupOffset: " + GroupOffset + ", SACL Offset: " + SaclOffset + " DACL Offset: " + DaclOffset;
		retval += "\nOwner: " + OwnerSID.toString() + "(" +ad.convertSIDtoDN(OwnerSID) + ")";
		retval += "\nGroup: " + GroupSID.toString() + "(" + ad.convertSIDtoDN(GroupSID) + ")";
		retval += "\nSACL:  " + Sacl.toString();
		retval += Dacl.toString();
		return(retval);
	}
	
	private static byte[] convertIntLSB(int input,int size) {
		// Given an integer on input, convert it to an array of bytes (size) bytes long
		// If size is 1, we return a single byte.  If it's 2 we return a 2-byte value.
		// If size is 4, we return a 4-byte value.  All output in more than one byte is in LSB order.
		
		byte[] retval = new byte[size];  // get a new byte array to work with
		if (size == 1) {
			retval[0] = (byte) ((input > 128)?input-256:input);
			return(retval);
		} else if (size == 2) {
			int leftbyte = (int) (input / 256);
			retval[1] = (byte) ((leftbyte > 128)?leftbyte - 256:leftbyte);
			int rightbyte = (int) (input - (leftbyte * 256));
			retval[0] = (byte) ((rightbyte > 128)?rightbyte - 256:rightbyte);
			return(retval);
		} else if (size == 4) {
			int leftbyte = (int) (input / (256*256*256));
			retval[3] = (byte) ((leftbyte > 128)?leftbyte - 256:leftbyte);
			int leftmid = (int) ((input - (leftbyte * 256*256*256)) / (256*256));
			retval[2] = (byte) ((leftmid > 128)?leftmid - 256:leftmid);
			int rightmid = (int) ((input - (leftbyte * 256*256*256) - (leftmid * 256*256))/ (256));
			retval[1] = (byte) ((rightmid > 128)?rightmid - 256:rightmid);
			int rightbyte = (int) ((input - (leftbyte * 256*256*256) - (leftmid * 256*256) - (rightmid * 256)));
			retval[0] = (byte) ((rightbyte > 128)?rightbyte - 256:rightbyte);
			return(retval);
		} else {
			return null;
		}
	}
	
	public byte[] Serialize() {
		// Return a byte array containing the serialized version of a decoded SecurityDescriptor
		// This should reverse the process of loading the SD in the first place.
		// We serialize our own information and then call on the Serialize() routines of our components to 
		// emit the serialized versions of their own data.  Since padding is irrelevant beyond framing numeric
		// values, we can simply append to the array of bytes as we go.

		int totalsize = 20 + Sacl.AclSize + Dacl.AclSize + OwnerSID.SidSize + GroupSID.SidSize;
		byte [] retval = new byte[totalsize];  // Create an empty shell for returning
		int curoff = 0;
		
		// Start with the header
		retval[curoff++] = Revision;  
		retval[curoff++] = Sbz1;
		for (int i = 0; i < 2; i++) {
			retval[curoff++] = Control[i];
		}
		byte[] oo = convertIntLSB(OwnerOffset,4);
		if (oo != null) {
			for (int i = 0; i < 4; i ++) {
				retval[curoff++] = oo[i];
			}
		} 
		byte[] go = convertIntLSB(GroupOffset,4);
		if (go != null) {
			for (int i = 0; i < 4; i ++) {
				retval[curoff++] = go[i];
			}
		}
		byte[] so = convertIntLSB(SaclOffset,4);
		if (so != null) {
			for (int i=0; i< 4; i++) {
				retval[curoff++] = so[i];				
			}
		}
		byte[] daclo = convertIntLSB(DaclOffset,4);
		if (daclo != null) {
			for (int i = 0; i < 4; i++) {
				retval[curoff++] = daclo[i];
			}
		}
		byte[] SaclSer = Sacl.Serialize();
		if (SaclSer != null) {
			for (int i = 0; i < SaclSer.length; i++) {
				retval[curoff++] = SaclSer[i];
			}
		}
		byte[] DaclSer = Dacl.Serialize();
		if (DaclSer != null) {
			for (int i = 0; i < DaclSer.length; i++) {
				retval[curoff++] = DaclSer[i];
			}
		}
		byte[] OwnerSer = OwnerSID.Serialize();
		if (OwnerSer != null) {
			for (int i = 0; i < OwnerSer.length; i++) {
				retval[curoff++] = OwnerSer[i];
			}
		}
		byte[] GroupSer = GroupSID.Serialize();
		if (GroupSer != null) {
			for (int i=0; i< GroupSer.length; i++) {
				retval[curoff++] = GroupSer[i];
			}
		}
		return(retval);  // placeholder
	}
	
	public static void main(String args[]) {
		String filename = "";
		byte[] bytes = null;
		
	/* DEBUG
	 * 	byte[] test = convertIntLSB(409600,4);
	 *
	 *  System.out.println("Byte 0 is " + test[0] + " and Byte 1 is " + test[1] + " and Byte 2 is " + test[2] + " and Byte 3 is " + test[3]);
	 *	if (true) {
	 *		System.exit(0);
	 *	}
	*/
		
		
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
			// We should now have an SD for this resource, so we're good to go...
			if (SR != null && SR.objectName != null) {
				// the resource is real
				System.out.println(SR.toString());
				System.out.println("Testing add of ACE to descriptor");
				SR.addAceDacl(new MsAce(MsAce.ACETYPE_ACCESS_ALLOWED,MsAce.INHERIT_NONE,MsAce.READ_WRITE_CREATE_DELETE,new MsSID(new SecuredResource("shilen").getSID())));
				System.out.println("New SR is:\n" + SR.toString());
				System.out.println("Testing add of ACE with Object");
				SR.addAceDacl(new MsAce(MsAce.ACETYPE_ACCESS_ALLOWED_OBJECT,MsAce.INHERIT_NONE,MsAce.EXTENDED_READ_PROP,new MsGUID(SR.getAttributeGUID("eduPersonPrimaryAffiliation")),null,new MsSID(new SecuredResource("gr-it_admins_systemOfRecord-Geo-A&S-TrinityCollege-orgs").getSID())));
				System.out.println("new SR is:\n" + SR.toString());
			}
		}

	}
		
	
	@Override public int hashCode() {
		int retval = 17;
		retval += Revision;
		retval += Sbz1;
		retval += Control[0] + Control[1]*256;
		retval += OwnerOffset;
		retval += GroupOffset;
		retval += SaclOffset;
		retval += DaclOffset;
		retval += Sacl.hashCode();
		retval += Dacl.hashCode();
		retval += OwnerSID.hashCode();
		retval += GroupSID.hashCode();
		retval += Size;
		return retval;
		
	}
	
	@Override public boolean equals(Object other) {
		if (other == null) {
			return false;
		}
		if (! other.getClass().getName().equals(this.getClass().getName())) {
			return false;
		}
		SecurityDescriptor compare = (SecurityDescriptor) other;
		if (Revision != compare.Revision || Sbz1 != compare.Sbz1 || OwnerOffset != compare.OwnerOffset || GroupOffset != compare.GroupOffset || SaclOffset != compare.SaclOffset || DaclOffset != compare.DaclOffset) {
			return false;
		}
		if (Control[0] != compare.Control[0] || Control[1] != compare.Control[1]) {
			return false;
		}
		if (!Sacl.equals(compare.Sacl) || !Dacl.equals(compare.Dacl) || !OwnerSID.equals(compare.OwnerSID) || !GroupSID.equals(compare.GroupSID)) {
			return false;
		}
		if (Size != compare.Size) {
			return false;
		}
		return true;
	}
	
	public boolean containsAceDacl(MsAce target) {
		if (Dacl.containsAce(target)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public void addAceDacl(MsAce toAdd) {
		// Updates OwnerOffset, GroupOffset, and Size according to the new size of the DACL
		int oldSize = Dacl.AclSize;  // save the old size
		int newSize = Dacl.addAce(toAdd);  // Add the ACE to the Dacl
		if (newSize < oldSize) {
			throw new RuntimeException("Error -- added ACE and got a SMALLER ACL -- that dog won't hunt!");
		}
		if (newSize > oldSize) {
			OwnerOffset += (newSize - oldSize);  // push owner down by N
			GroupOffset += (newSize - oldSize);  // push group down by N
			Size += (newSize - oldSize); // add N to the total size of the beast
		}
	}
	
	public void removeAceDacl(MsAce toRemove) {
		// Updates OwnerOffset, GroupOffset, and Size according to the new size of the DACL
		int oldSize = Dacl.AclSize; // save old size
		int newSize = Dacl.removeAce(toRemove);  // remove the ACE
		if (newSize > oldSize) {
			throw new RuntimeException("Error -- removed ACE and got a LARGER ACL -- that dog still won't hunt!");
		}
		if (newSize < oldSize) {
			OwnerOffset -= (oldSize - newSize);  // pull owner up by N
			GroupOffset -= (oldSize - newSize);  // pull group up by N
			Size -= (oldSize - newSize);  // decrease total size of the beast by N
		}
	}
}

