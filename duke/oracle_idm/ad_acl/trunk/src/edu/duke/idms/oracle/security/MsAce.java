package edu.duke.idms.oracle.security;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Arrays;

/*
 * @author: rob
 * 
 * Class encapsulating the Microsoft ACE object -- the most primitive element in a Security Descriptor.
 * 
 */
public class MsAce {
	byte ACEType;		//1-byte type of the ACE
	byte Inheritance;	//1-byte inheritance flags bitfield
	short Size;			// 16-bit integer size value
	byte[] SizeBytes;	// 2-byte size in lsb byte-order
	byte[] AccessMask;	// 4-byte AccessMask bitfield
	byte[] ObjectFlags;	// 4-byte object flags (may be empty or null if this is not an extended Object ACE
	MsGUID ObjectType;	// 16-byte object type GUID for object-specific ACE (if present or needed)
	MsGUID InheritedObjectType;  // 16-byte inherited object type GUID for object-specific ACE that inherits only to specific classes (if present or needed)
	MsSID SID;	// Bytes of the SID of the referent (who's granted/denied access via this ACE).
	
	// ACE Type values can be one of the following
	public static final byte ACETYPE_ACCESS_ALLOWED = 0x00;
	public static final byte ACETYPE_ACCESS_DENIED = 0x01;
	public static final byte ACETYPE_SYSTEM_AUDIT = 0x02;
	public static final byte ACETYPE_ACCESS_ALLOWED_OBJECT = 0x05;
	public static final byte ACETYPE_ACCESS_DENIED_OBJECT = 0x06;
	public static final byte ACETYPE_SYSTEM_AUDIT_OBJECT = 0x07;
	public static final byte ACETYPE_SYSTEM_ALARM_OBJECT = 0x08;
	
	// ACE Inheritance flags
	public static final byte INHERIT_NONE = 0x0;
	public static final byte INHERIT_THIS_AND_CHILDREN = 0x02;
	public static final byte INHERIT_CHILDREN_ONLY = 0x0a;
	public static final byte INHERIT_ONE_LEVEL = 0x04;
	public static final byte INHERITED_ACE = 0x10;
	
	// Tourist information about our Active Directory
	private static final String LDAPURL = "ldap://SUPPRESSED:636";
	private static final String LDAPUSER = SUPPRESSED@SUPPRESSED";
	private static final String LDAPPASSWORD = "SUPPRESSED";
	
	// ACEMask values for common collections of privileges
	//public static final byte[] FULL_CONTROL = {(byte)0xff,0x01,0x0f,0x10};
	public static final byte[] FULL_CONTROL = {(byte)0xff,0x01,0x0f,0x00};
	//public static final byte[] READ_OBJECT = {0x14,0x00,0x02,(byte)0x80};
	public static final byte[] READ_OBJECT = {0x14,0x00,0x02,0x00};
	//public static final byte[] WRITE_OBJECT = {0x28,0x00,0x02,0x40};
	public static final byte[] WRITE_OBJECT = {0x28,0x00,0x02,0x00};
	public static final byte[] CREATE_CHILDREN = {0x01,0x00,0x02,0x00};
	public static final byte[] DELETE_CHILDREN = {0x02,0x00,0x02,0x00};
	//public static final byte[] READ_WRITE_OBJECT = {0x3c,0x00,0x02,(byte)0xc0};
	public static final byte[] READ_WRITE_OBJECT = {0x3c,0x00,0x02,0x00};
	//public static final byte[] READ_WRITE_CREATE_DELETE = {0x3f,0x00,0x02,(byte)0xc0};
	public static final byte[] READ_WRITE_CREATE_DELETE = {0x3f,0x00,0x02,0x00};
	//public static final byte[] READ_WRITE_CREATE = {0x3d,0x00,0x02,(byte)0xc0};
	public static final byte[] READ_WRITE_CREATE = {0x3d,0x00,0x02,0x00};
	
	// ACEMask values for extended privileges
	public static final byte[] EXTENDED_CONTROL_ACCESS = {0x00,0x01,0x00,0x00};
	public static final byte[] EXTENDED_READ_PROP = {0x10,0x00,0x00,0x00};
	public static final byte[] EXTENDED_WRITE_PROP = {0x20,0x00,0x00,0x00};
	public static final byte[] EXTENDED_READ_WRITE_PROP = {0x30,0x00,0x00,0x00};

	// ACEMask values for special privileges
	public static final byte[] CHANGE_PERMISSIONS = {0x00,0x00,0x04,0x00};
	public static final byte[] CHANGE_OWNER = {0x00,0x00,0x08,0x00};
	
	// Constants for names of extended rights 
	public static final String RIGHT_AUTHN = "Allowed-to-Authenticate";
	public static final String RIGHT_CHGPW = "User-Change-Password";
	public static final String RIGHT_RCVAS = "Receive-As";
	public static final String RIGHT_RSTPW = "User-Force-Change-Password";
	public static final String RIGHT_SENDAS = "Sent-As";
	public static final String RIGHT_MAIL = "Email-Information";
	public static final String RIGHT_GENERAL = "General-Information";
	public static final String RIGHT_MEMBERSHIP = "Membership";
	public static final String RIGHT_PERSONAL = "Personal-Information";
	public static final String RIGHT_PUBLIC = "Public-Information";
	public static final String RIGHT_RAS = "RAS-Information";
	public static final String RIGHT_RESTR = "User-Account-Restrictions";
	public static final String RIGHT_LOGON = "User-Logon";
	public static final String RIGHT_WEB = "Web-Information";
	
	// Constant
 	
	
	/* Constructor method - create an empty MsAce object
	 * @param none
	 * 
	 */
	protected MsAce() {
		ACEType = 0;
		Inheritance = 0;
		Size = 0;
		SizeBytes = new byte[]{0,0};
		AccessMask = new byte[]{0,0,0,0};
		ObjectFlags = new byte[]{0,0,0,0};
		ObjectType = new MsGUID(new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		InheritedObjectType = new MsGUID(new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		SID = null;
	}
	
	protected MsAce(byte Type, byte Inherit, byte[] Mask, MsSID sid) {
		/* Need code here */
		// Here we create an ACE based on a type, an inheritance setting, a mask, and a SID
		// This is necessarily a general object ACE -- it applies to the whole object rather than just some attr.
		// There is a separate constructor interface for handling creation of attr-specific ACEs.
		// We must calculate the size and set sizeBytes properly.  Presumes the caller has set the appropriate 
		// values for the bytes involved (which may rely upon our constants).
		short size = 0;
		if (Type > 2) {
			// Types greater than 2 are extended ACE types (with object qualifiers) and cannot be 
			// constructed with this interface, so we throw an exception for bad input.
			throw new RuntimeException("Object qualified ACE type specified without providing object qualifiers");
		}
		ACEType = Type;
		size += 1;
		Inheritance = Inherit;
		size += 1;
		if (Mask.length != 4) {
			throw new RuntimeException("Incorrect length of AccessMask " + Mask.length + " should be 4");
		}
		AccessMask = new byte[4];
		for (int i = 0; i < 4; i++) {
			AccessMask[i] = Mask[i];
		}
		size += 4;
		ObjectFlags = null;
		ObjectType = null;
		InheritedObjectType = null;
		SID = sid;
		size += SID.SidSize;
		size += 2;  // 2 bytes of size data
		Size = size;
		SizeBytes = new byte[2];
		SizeBytes[0] = (byte) (size & (byte) 0xff);
		SizeBytes[1] = (byte) (((size & 0xff00) >> 8) & 0xff);
	}
	
	protected MsAce(byte Type, byte Inherit, byte[] Mask, MsGUID ObjectT, MsGUID InheritedObjectT, MsSID sid) {
		/* Need code here */
		// This method creates an ACE that has target object qualifiers (usually a target attribute or some other 
		// odd thing). We must calculate Size and SizeBytes properly.  Presumes the caller has set the appropriate
		// values for the bytes involved and for the GUID(s) and SID(s) involved.  
		//
		// Note that in this case, it is possible for an object qualified ACE to contain either or both of ObjectType
		// and InheritedObjectType, based on the value of ObjectFlags, which we compute based on the existence or not
		// of the two types.  If we set an ObjectType, we add 1 to Flags value.  If we set an InheritedObjectType we 
		// add 2 to the Flags value, and if we have neither, the Flags value remains 0 (but in that case, we throw
		// an exception, since an object ACE with no object is nonsensical.
		//
		// Note that Flags is a byte[4], even though the value is always either 1, 2, or 3.  Go figure.
		//
		short size = 0;
		if (Type <= 2) {
			// Types less than 2 are standard ACE types (with no object qualifiers) and cannot be 
			// constructed with this interface, so we throw an exception for bad input.
			throw new RuntimeException("Unqualified ACE type specified with object qualifiers");
		}
		ACEType = Type;
		size += 1;
		Inheritance = Inherit;
		size += 1;
		if (Mask.length != 4) {
			throw new RuntimeException("Incorrect length of AccessMask " + Mask.length + " should be 4");
		}
		AccessMask = new byte[4];
		for (int i = 0; i < 4; i++) {
			AccessMask[i] = Mask[i];
		}
		size += 4;
		//
		// Here we diverge and handle the object types required by this class of ACE
		byte flag = 0;
		if (ObjectT != null) {
			flag += 1;
		}
		if (InheritedObjectT != null) {
			flag += 2;
		}
		if (flag == 0) {
			// we have no values to work with, so throw an exception
			throw new RuntimeException("Object restricted ACE type specified with no qualifiers present");
		}
		ObjectFlags = new byte[4];
		ObjectFlags[0] = (byte) flag;
		ObjectFlags[1] = 0;
		ObjectFlags[2] = 0;
		ObjectFlags[3] = 0;
		size += 4;  // Add four bytes for the new flag value
		
		if ((flag & 0x01) == 0x01) {
			// We need to insert the ObjectType value
			ObjectType = ObjectT;
			size += ObjectT.ByteCount;
		}
		
		if ((flag & 0x02) == 0x02) {
			// We need to insert the InheritedObjectType value
			InheritedObjectType = InheritedObjectT;
			size += InheritedObjectT.ByteCount;
		}

		SID = sid;
		size += SID.SidSize;
		size += 2;  // 2 bytes of size data
		Size = size;
		SizeBytes = new byte[2];
		SizeBytes[0] = (byte) (size & (byte) 0xff);
		SizeBytes[1] = (byte) (((size & 0xff00) >> 8) & 0xff);
		
	}
	/* Constructor method - create an MsAce object based on a passed in byte array representation
	 * @param data Serialized representation from, eg., ntSecurityDescriptor value
	 */
	
	protected MsAce(byte [] data) {
		ACEType = data[0];
		Inheritance = data[1];
		SizeBytes = new byte[] {data[2],data[3]};
		Size = (short) ((short) (((SizeBytes[1] < 0)?SizeBytes[1]+256:SizeBytes[1]) << 8) + ((SizeBytes[0]<0)?SizeBytes[0]+256:SizeBytes[0]));
		AccessMask = new byte[] {data[4],data[5],data[6],data[7]};
		ObjectFlags = new byte[4];
		//
		// Depending on the value of ObjectFlags, we either null out or fill in the 
		// ObjectType and InheritedObjectType values.
		// Perhaps these should be a separate "GUID" class?
		//
		int i = 8;  // start the counter...
		if (ACEType == ACETYPE_ACCESS_ALLOWED_OBJECT || ACEType==ACETYPE_ACCESS_DENIED_OBJECT || ACEType == ACETYPE_SYSTEM_AUDIT_OBJECT || ACEType == ACETYPE_SYSTEM_ALARM_OBJECT ) {
			// We need to specify the object values
			int j = 0;
			while (i < 12) {
				ObjectFlags[j++] = data[i++];
			}
			// DEBUG System.out.println("Object flags values are " + (int)ObjectFlags[0] + " , " + (int) ObjectFlags[1] + " , " + ObjectFlags[2] + " , " + ObjectFlags[3]);
			byte[] data2;
			byte[] data3;
			if ((ObjectFlags[0] & 0x01) == 0x01) { // originally objectlags[0]
				// Object values include an ObjectType value
				// DEBUG System.out.println("Object flags indicate object type present");
				data2 = new byte[data.length - 12];
				for (int foo = 0 ; foo < data.length - 12; foo ++) {
						data2[foo] = data[foo + 12];
				}
				ObjectType = new MsGUID(data2);
				//DEBUG System.out.println("ObjectType is " + ObjectType.toString());
			} else {
				ObjectType = null;
				// DEBUG System.out.println("No object type present");
			}
			if ((ObjectFlags[0] & 0x02) == 0x02) { // originally objectflags[0]
				// Object values include an InheritedObjectType value
				// DEBUG System.out.println("Object flags indicate inherited object type present");
				if (ObjectType != null) {
					// data3 = java.util.Arrays.copyOfRange(data,12+ObjectType.ByteCount,data.length);
					data3 = new byte[data.length - (12 + ObjectType.ByteCount)];
					for (int foo = 0; foo < data.length - (12 + ObjectType.ByteCount); foo ++) {
						data3[foo] = data[foo + (12 + ObjectType.ByteCount)];
					}
				} else {
					// data3 = java.util.Arrays.copyOfRange(data,12,data.length);
					data3 = new byte[data.length - 12];
					for (int foo = 0; foo < data.length - 12; foo++) {
						data3[foo] = data[foo+12];
					}
				}
				InheritedObjectType = new MsGUID(data3);
			} else {
				InheritedObjectType = null;
				// DEBUG System.out.println("No inherited object type present");
			}
		} else {
			ObjectFlags = null;
			ObjectType=null;
			InheritedObjectType=null;
			// DEBUG System.out.println("Neither object type nor inherited object type present");
		}
		if (ObjectType != null) {
			i += ObjectType.ByteCount;
		}
		if (InheritedObjectType != null) {
			i += InheritedObjectType.ByteCount;
		}
		byte[] sidbytes = new byte[(Size-i)]; // no add one here ;-(
		// DEBUG System.out.println("length is " + Size + " and i is " + i);
		int j = 0;
		while (i < Size) {
			sidbytes[j++] = data[i++];  // accumulate bytes of the SID into a byte array
		}
		SID = new MsSID(sidbytes);
	}
	
	public boolean isAllow() {
		if ((ACEType & ACETYPE_ACCESS_ALLOWED) == ACETYPE_ACCESS_ALLOWED || (ACEType & ACETYPE_ACCESS_ALLOWED_OBJECT) == ACETYPE_ACCESS_ALLOWED_OBJECT) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isDeny() {
		if ((ACEType & ACETYPE_ACCESS_DENIED) == ACETYPE_ACCESS_DENIED || (ACEType & ACETYPE_ACCESS_DENIED_OBJECT) == ACETYPE_ACCESS_DENIED_OBJECT) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isInherited() {
		// true only if this is an inherited ACE we cannot delete
		if ((Inheritance & INHERITED_ACE) == INHERITED_ACE) {
			return true;
		} else {
			return false;
		}
	}
	
	/*
	 * toString - dump a string representation of the object
	 * @param none
	 */
	@Override public String toString() {
		String returnVal = "";
		ADConnectionWrapper AD = ADConnectionWrapper.getInstance(LDAPURL,LDAPUSER,LDAPPASSWORD);
		if (ACEType == ACETYPE_ACCESS_ALLOWED) {
			returnVal += "Type: Allow(General),";
		} else if (ACEType == ACETYPE_ACCESS_DENIED) {
			returnVal += "Type: Deny(General),";
		} else if (ACEType == ACETYPE_ACCESS_ALLOWED_OBJECT) {
			returnVal += "Type: Allow(Extended), ";
			if (ObjectType != null) {
			// returnVal += " [ ObjectType: " + ObjectType.toString();
				returnVal += " [ ObjectType: " + AD.convertGUIDtoDN(ObjectType);
			} else {
				returnVal += "[";
			}
			if (InheritedObjectType != null) {
				// returnVal += " InheritedObjectType: " + InheritedObjectType.toString() + "]";
				returnVal += " InheritedObjectType: " + AD.convertGUIDtoDN(InheritedObjectType);
			} else {
				returnVal += "]";
			}
			
			
		} else if (ACEType == ACETYPE_ACCESS_DENIED_OBJECT) {
			returnVal += "Type:  Deny(Extended),";
			if (ObjectType != null) {
				// returnVal += " [ ObjectType: " + ObjectType.toString();
				 returnVal += "[ ObjectType: " + AD.convertGUIDtoDN(ObjectType);
			} else {
				returnVal += "[";
			}
			if (InheritedObjectType != null) {
				// returnVal += " InheritedObjectType: " + InheritedObjectType.toString() + "]";
				returnVal += " InheritedObjectType: " + AD.convertGUIDtoDN(InheritedObjectType);
			} else {
				returnVal += "]";
			}
			
		} else {
			returnVal += "Type: Audit,";
		}
		
		// Parse out the inheritance flag value
		
		returnVal += "\n        InheritanceFlags(";
		if ((Inheritance & 0x1) == 0x1) {
			returnVal += "ObjectInherit,";
		}
		if ((Inheritance & 0x2) == 0x2) {
			returnVal += "ContainerInherit,";
		}
		if ((Inheritance & 0x4) == 0x4) {
			returnVal += "NoPropgateInherit,";
		}
		if ((Inheritance & 0x8) == 0x8) {
			returnVal += "InheritOnly,";
		}
		if ((Inheritance & 0x10) == 0x10) {
			returnVal += "InheritedACE";
		}
		if ((Inheritance & 0x40) == 0x40) {
			returnVal += "SuccessfulAccess";
		}
		if ((Inheritance & 0x80) == 0x80) {
			returnVal += "FailedAccess";
		}
		returnVal += ")";
		
		
		// Parse out the access mask value
		
		returnVal += "\n        AccessMask = (";
		if ((AccessMask[3] & 0x10) == 0x10) {
			returnVal += "GENERIC_ALL ";
		}
		if ((AccessMask[3] & 0x20) == 0x20) {
			returnVal += "GENERIC_EXE ";
		}
		if ((AccessMask[3] & 0x40) == 0x40) {
			returnVal += "GENERIC_WRITE ";
		}
		if ((AccessMask[3] & 0x80) == 0x80) {
			returnVal += "GENERIC_READ ";
		}
		if ((AccessMask[3] & 0x01) == 0x01) {
			returnVal += "ACCESS_SYSTEM_SECURITY ";
		}
		if ((AccessMask[0] & 0x04) == 0x04) {
			returnVal += "ACTRL_DS_LIST ";
		}
		if ((AccessMask[2] & 0x01) == 0x01) {
			returnVal += "DELETE ";
		}
		if ((AccessMask[1] & 0x02) == 0x02) {
			returnVal += "CONTROL_ACCESS ";
		}
		if ((AccessMask[0] & 0x01) == 0x01) {
			returnVal += "CREATE_CHILD ";
		}
		if ((AccessMask[0] & 0x02) == 0x02) {
			returnVal += "DELETE_CHILD ";
		}
		if ((AccessMask[0] & 0x40) == 0x40) {
			returnVal += "DELETE_TREE ";
		}
		if ((AccessMask[3] & 0x80) == 0x80) {
			returnVal += "LIST_OBJECT ";
		}
		if ((AccessMask[0] & 0x10) == 0x10) {
			returnVal += "READ_PROP ";
		}
		if ((AccessMask[0] & 0x08) == 0x08) {
			returnVal += "SELF ";
		}
		if ((AccessMask[0] & 0x20) == 0x20) {
			returnVal += "WRITE_PROP ";
		}
		if ((AccessMask[2] & 0x02) == 0x02) {
			returnVal += "READ_CONTROL ";
		}
		if ((AccessMask[2] & 0x10) == 0x10) {
			returnVal += "SYNCHRONIZE ";
		}
		if ((AccessMask[2] & 0x04) == 0x04) {
			returnVal += "WRITE_DAC ";
		}
		if ((AccessMask[2] & 0x08) == 0x08) {
			returnVal += "WRITE_OWNER ";
		}
		returnVal += ")";
		
		if (SID != null) { 
			returnVal += "\n        SID: [ DN: " + AD.convertSIDtoDN(SID) + "]\n";
		} else {
			returnVal += "\n        SID: null\n";
		}
		return(returnVal);
	}
	
	/*
	 * @author rob
	 * For testing purposes...
	 */
	public static void main(String args[]) {
		MsAce foo = new MsAce();
		System.out.println("Empty ACE (for comparison) : \n" + foo.toString());
		MsAce bar = new MsAce((byte)0,(byte)0,new byte[] {(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x80},new MsSID(new byte[] {0x01,0x05,0x00,0x00,0x00,0x00,0x00,0x05,0x15,0x00,0x00,0x00,(byte)0xf4,(byte)0x43,(byte)0xa7,(byte)0xab,(byte)0x8f,(byte)0xfb,(byte)0x60,(byte)0xc8,0x06,0x46,(byte)0x67,(byte)0xff,0x15,0x18,0x01,0x00}));
		System.out.println("Read/List ACE (for comparison) : \n" + bar.toString());
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
		byte[] retval = new byte[Size];
		int curoff = 0;
		// Start with header
		retval[curoff++] = ACEType;
		retval[curoff++] = Inheritance;
		byte[] sz = convertIntLSB(Size,2);
		if (sz != null) {
			for (int i=0; i<2; i++) {
				retval[curoff++] = sz[i];
			}
		}
		if (AccessMask != null) {
			for (int i=0; i<4; i++) {
				retval[curoff++] = AccessMask[i];
			}
		}
		if (ObjectFlags != null) {
			for (int i=0;i<4; i++) {
				retval[curoff++] = ObjectFlags[i];
			}
		}
		if (ObjectType != null) {
				byte[] ot = ObjectType.Serialize();
				if (ot != null) {
					for (int i = 0; i<ot.length; i++) {
						retval[curoff++] = ot[i];
					}
				}
		}
		if (InheritedObjectType != null) {
			byte[] it = InheritedObjectType.Serialize();
			if (it != null) {
				for (int i = 0; i < it.length; i++) {
					retval[curoff++] = it[i];
				}
			}
		}
		if (SID != null) {
			byte[] sid = SID.Serialize();
			if (sid !=null) {
				for (int i=0; i<SID.SID.size(); i++) {
					retval[curoff++] = sid[i];
				}
			}
		}
		return(retval);
	}
	
	@Override public int hashCode() {
		int retval = 17;
		retval += ACEType;
		retval += Inheritance;
		retval += Size;
		retval += SizeBytes[0] + SizeBytes[1]*256;
		retval += AccessMask[0] + AccessMask[1]*256 + AccessMask[2]*256*256 + AccessMask[3]*256*256*256;
		if (ObjectFlags != null) {
			retval += ObjectFlags[0] + ObjectFlags[1]*256 + ObjectFlags[2]*256*256 + ObjectFlags[3]*256*256*256;
		}
		if (ObjectType != null) {
			retval += ObjectType.hashCode();
		}
		if (InheritedObjectType != null) {
			retval += InheritedObjectType.hashCode();
		}
		if (SID != null) {
			retval += SID.hashCode();
		}
		return(retval);
	}
	
	public boolean equalsIgnoreInherited(Object other) {
		//System.out.println("Starting ACE loose comparison");
		if (other == null) {
			//System.out.println("Failing loose ACE comparison on null comparator");
			return false;
		}
		if (! other.getClass().getName().equalsIgnoreCase(this.getClass().getName())) {
			//System.out.println("Failing loose ACE comparison on wrong class name");
			return false;
		}
		MsAce compare = (MsAce) other;
		if (ACEType != compare.ACEType) {
			//System.out.println("Failing loose ACE comparison on type mismatch");
			return false;
		}
		if ((Inheritance & 0x6f) != (compare.Inheritance & 0x6f)) {
			//System.out.println("Failing loose ACE comparison on inheritance mismatch");
			return false;
		}
		if (Size != compare.Size) {
			//System.out.println("Failing loose ACE comparison on size mismatch");
			return false;
		}
	
		if (SizeBytes[0] != compare.SizeBytes[0] || SizeBytes[1] != compare.SizeBytes[1]) {
			//System.out.println("Failing loose ACE comparison on sizebytes mismatch");
			return false;
		}
		if (((AccessMask[0] & 0x7f) != (compare.AccessMask[0] & 0x7f)) || ((AccessMask[1] & 0x7f) != (compare.AccessMask[1] & 0x7f)) || ((AccessMask[2] & 0x7f) != (compare.AccessMask[2])) || ((AccessMask[3] & 0x7f) != (compare.AccessMask[3]))) {
			//System.out.println("Failing loose ACE comparison on accessmask mismatch");
			//System.out.println("Values are " + this + " and " + other);
			//System.out.println(AccessMask[0] + " " + AccessMask[1] + " " + AccessMask[2] + " " + AccessMask[3] + " versus " + compare.AccessMask[0] + " " + compare.AccessMask[1] + " " + compare.AccessMask[2] + " " + compare.AccessMask[3]);
			return false;
		}
		if (ObjectFlags != null && compare.ObjectFlags != null) {
			if (ObjectFlags[0] != compare.ObjectFlags[0] || ObjectFlags[1] != compare.ObjectFlags[1] || ObjectFlags[2] != compare.ObjectFlags[2] || ObjectFlags[3] != compare.ObjectFlags[3]) {
				return false;
			}
		} else {
			if ((ObjectFlags != null && compare.ObjectFlags == null) || (ObjectFlags == null && compare.ObjectFlags != null)) {
				return false;
			}
		}
		if (ObjectType == null) {
			if (compare.ObjectType != null) {
				return false;
			}
		} else {
			if (compare.ObjectType == null || ! ObjectType.equals(compare.ObjectType)) {
				return false;
			}
		}
		if (InheritedObjectType == null) {
			if (compare.InheritedObjectType != null) {
				return false;
			}
		} else {
			if (compare.InheritedObjectType == null || ! InheritedObjectType.equals(compare.InheritedObjectType)) {
				return false;
			}
		}
		if (SID == null) {
			if (compare.SID != null) {
				return false;
			}
		} else {
			if (compare.SID == null || ! SID.equals(compare.SID)) {
				return false;
			}
		}
		return true;
	}
	@Override public boolean equals(Object other) {
		//System.out.println("Beginning comparison of ACEs");
		if (other == null) {
		//	System.out.println("ACE comparison fails because other is null");
			return false;
		}
		if (! other.getClass().getName().equalsIgnoreCase(this.getClass().getName())) {
		//	System.out.println("Classes differ!");
			return false;
		}
		MsAce compare = (MsAce) other;
		if ((ACEType != compare.ACEType)) {
		//	System.out.println("ACE comparison failed on ACEType");
			return false;
		}
		if ((Inheritance & 0x7f) != (compare.Inheritance & 0x7f)) {
		//	System.out.println("ACE comparison failed on Inheritance");
			return false;
		}
		if ((Size != compare.Size)) {
		//	System.out.println("ACE comparison failed on Size");
			return false;
		}
		if (SizeBytes[0] != compare.SizeBytes[0] || SizeBytes[1] != compare.SizeBytes[1]) {
		//	System.out.println("ACE comparison failed on SizeBytes");
			return false;
		}
		if (((AccessMask[0] & 0x7f) != (compare.AccessMask[0] & 0x7f)) || ((AccessMask[1] & 0x7f) != (compare.AccessMask[1] & 0x7f)) || ((AccessMask[2] & 0x7f) != (compare.AccessMask[2])) || ((AccessMask[3] & 0x7f) != (compare.AccessMask[3]))) {
		//	System.out.println("ACE comparison failed on AccessMask");
			return false;
		}
		if (ObjectFlags != null && compare.ObjectFlags != null) {
			if (ObjectFlags[0] != compare.ObjectFlags[0] || ObjectFlags[1] != compare.ObjectFlags[1] || ObjectFlags[2] != compare.ObjectFlags[2] || ObjectFlags[3] != compare.ObjectFlags[3]) {
				return false;
			}
		} else {
			if ((ObjectFlags != null && compare.ObjectFlags == null) || (ObjectFlags == null && compare.ObjectFlags != null)) {
				return false;
			}
		}
		if (ObjectType == null) {
			if (compare.ObjectType != null) {
				return false;
			}
		} else {
			if (compare.ObjectType == null || ! ObjectType.equals(compare.ObjectType)) {
				return false;
			}
		}
		if (InheritedObjectType == null) {
			if (compare.InheritedObjectType != null) {
				return false;
			}
		} else {
			if (compare.InheritedObjectType == null || ! InheritedObjectType.equals(compare.InheritedObjectType)) {
				return false;
			}
		}
		if (SID == null) {
			if (compare.SID != null) {
				return false;
			}
		} else {
			if (compare.SID == null || ! SID.equals(compare.SID)) {
				return false;
			}
		}
		return true;
	}
	
}
