package edu.duke.idms.oracle.security;

import java.util.ArrayList;
// import java.util.Arrays;

/*
 * @author: rob
 * 
 * Class encapsulating the ACL (a component of MS Security Descriptor objects).  Contains zero or more ACE objects.
 * 
 */
 class MsAcl {
	byte[] Revision;		// 2-byte ACL Revision 
	int AclSize;			// Short integer ACL size
	byte[] AclSizeBytes;	// 2-byte ACL Size in lsb byte-order
	int AceCount;			// 32-bit integer count of ACEs in this ACL
	byte[] AceCountBytes;	// 4-byte ACE count in lsb byte-order
	ArrayList<MsAce> Aces;	// ArrayList of msACE objects describing the ACEs in this ACL
	
	protected MsAcl() {
		// Null creator
		Revision = new byte[2];
		AclSize=0;
		AclSizeBytes = new byte[2];
		AceCount=0;
		AceCountBytes = new byte[4];
		Aces = new ArrayList<MsAce>();
	}
	
	protected MsAcl(byte[] bytes) {
		Revision = new byte[]{bytes[0],bytes[1]};
		AclSizeBytes = new byte[]{bytes[2],bytes[3]};
		AclSize = (int) (((bytes[3] < 0)?bytes[3]+256:bytes[3]) << 8) + ((bytes[2]<0)?bytes[2]+256:bytes[2]);
		AceCountBytes = new byte[]{bytes[4],bytes[5],bytes[6],bytes[7]};
		AceCount = (int)((((bytes[7] < 0)?bytes[7]+256:bytes[7])<<24) + (((bytes[6] < 0)?bytes[6]+256:bytes[6]) << 16) + (((bytes[5] < 0)?bytes[5]+256:bytes[5]) << 8) + ((bytes[4] < 0)?bytes[4]+256:bytes[4]));
		Aces = new ArrayList<MsAce>();
		int count = 8;
		for (int i = 0; i < AceCount; i ++) {

			byte [] sub = new byte[AclSize - count];
			for (int foo = 0; foo < AclSize - count; foo ++) {
				sub[foo] = bytes[foo+count];
			}
			MsAce intermed = new MsAce(sub);
			Aces.add(intermed);
			count += intermed.Size;
		}
	}
	@Override public String toString() {
		String retval = "";
		retval += "AclRevision: " + (int) Revision[0] + (int) (Revision[1] << 8) + ", AclSize: " + (int) AclSize + " with " + AceCount + " ACEs \n\n";
		for (int i = 0; i < AceCount; i ++) {
			retval += "     ==> " + Aces.get(i).toString() + "\n";
		}
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
		byte[] retval = new byte[AclSize]; // we already have a size computed here
		int curoff = 0;
		
		// Start with the header information
		if (Revision != null) {
			for (int i = 0; i < 2; i++) {
				retval[curoff++] = Revision[i];
			}
		}
		byte[] sz = convertIntLSB(AclSize,2);
		if (sz != null) {
			for (int i =0; i < 2; i++) {
				retval[curoff++] = sz[i];
			}
		}
		byte[] ac = convertIntLSB(AceCount,4);
		if (ac != null) {
			for (int i=0; i<4; i++) {
				retval[curoff++] = ac[i];
			}
		}
		
		// Then insert all the ACEs in serialized form
		if (Aces != null) {
			for (int i = 0; i < AceCount; i++) {
				if (Aces.get(i) != null) {
					byte[] ab = Aces.get(i).Serialize();
					if (ab != null) {
						for (int j = 0; j < ab.length; j++) {
							retval[curoff++] = ab[j];
						}
					}
				}
			}
		}
		return(retval);
	}
	
	@Override public int hashCode() {
		int retval = 23;
		retval += Revision[0] + Revision[1]*256;
		retval += AclSize;
		retval += AclSizeBytes[0] + AclSizeBytes[1]*256;
		retval += AceCount;
		retval += AceCountBytes[0] + AceCountBytes[1]*256 + AceCountBytes[2]*256*256 + AceCountBytes[3]*256*256*256;
		for (int i = 0; i < Aces.size(); i++) {
			retval += Aces.get(i).hashCode();
		}
		return retval;
	}
	
	@Override public boolean equals(Object other) {
		if (other == null) {
			return false;
		}
		if (! other.getClass().getName().equalsIgnoreCase(this.getClass().getName())) {
			return false;
		}
		MsAcl compare = (MsAcl)other;
		if (Revision[0] != compare.Revision[0] || Revision[1]!= compare.Revision[1]) {
			return false;
		}
		if (AclSize != compare.AclSize || AceCount != compare.AceCount) {
			return false;
		}
		for (int i = 0; i < AceCount; i++) {
			if (! Aces.get(i).equals(compare.Aces.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	public boolean containsAce(MsAce target) {
		for (int i = 0; i < Aces.size(); i++) {
			if (target.equalsIgnoreInherited(Aces.get(i))) {
				return true;
			}
		}
		return false;
	}
	
	// Add an ACE to the ACL.  Returns the new ACL size for upstream updating of size/offset numbers
	
	public int addAce(MsAce toAdd) {
		int retval = AclSize;  // Start from current size
		if (this.containsAce(toAdd)) {
			return retval;  // do nothing if the ACE is already present
		}
		//
		// If the new ACE is a deny ACE, add it to the end of the deny list.
		// If the new ACE is a permit ACE, add it to the end of the whole list (which means the end of the arraylist)
		// Microsoft likes to list deny ACEs before permit ACEs
		if (toAdd.isAllow()) { 
			// Actually, add to the end of the *direct* list
			int i;
			for (i =0; i<AceCount; i++) {
				if ((Aces.get(i).isAllow()) && ((Aces.get(i).Inheritance & 0x10) == 0x10)) {
					// This is an inherited allow ACE
					break;
				}
			} 
			// Add to the end of the list (roughly)
			if (i != AceCount) {
				Aces.add(i,toAdd);
			} else {
				Aces.add(toAdd);
			}
			AclSize += toAdd.Size;
			retval = AclSize;
			AceCount += 1;
			AclSizeBytes = convertIntLSB(AclSize,2);
			AceCountBytes = convertIntLSB(AceCount,4);
			return retval;
		} else {
			// Add after the last deny ACE that isn't inherited
			for (int i=0;i<AceCount;i++) {
				if ((Aces.get(i).isAllow()) || (Aces.get(i).isDeny() && ((Aces.get(i).Inheritance & 0x10) == 0x10))) {
					Aces.add(i,toAdd); // add
					AclSize += toAdd.Size;
					retval = AclSize;
					AceCount += 1;
					AclSizeBytes = convertIntLSB(AclSize,2);
					AceCountBytes = convertIntLSB(AceCount,4);
					return retval;
				}
			}
		}
		return AclSize;  // if we don't change anything, just return what we already have.
	}
	
	public int removeAce(MsAce toRemove) {
		int retval = AclSize;  // Start from the current size
		//
		// If the ACE is an inherited ACE, do nothing adn return the current size
		if (toRemove.isInherited()) {
			return AclSize;
		}
		//
		// If the Ace is not present in the ACL, simply return the current size.
		// If the Ace is present in the ACL, find it and remove it from the Aces structure
		// then return the new size of the ACL for upstream use
		if (! this.containsAce(toRemove)) {
			return AclSize;
		} else {
			// We have it -- remove it
			for (int i=0; i < AceCount; i++) {
				if (toRemove.equals(Aces.get(i))) {
					int decsize = Aces.get(i).Size;
					Aces.remove(i);
					AclSize -= decsize;
					retval = AclSize;
					AceCount -= 1;
					AclSizeBytes = convertIntLSB(AclSize,2);
					AceCountBytes = convertIntLSB(AceCount,4);
					return retval;
				} 
			}
		}
		return retval;
	}
}
