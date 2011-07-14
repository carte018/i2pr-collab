package edu.duke.idms.oracle.security;

public class MsGUID {
	byte[] GUIDBytes;
	int ByteCount;
	public MsGUID(byte[] bytes) {
		GUIDBytes = new byte[16];  // All guids are 16 bytes long.  Period.
		ByteCount = 16;
		if (bytes != null) {
			for (int i = 0; i < ByteCount; i ++) {
				GUIDBytes[i] = bytes[i];
			}
		}
	}
	@Override public String toString() {
		String retval = "";
		String hexval = "";
	
		for (int i = 0; i < ByteCount; i ++) {
			hexval = Integer.toHexString(unsignextend(GUIDBytes[i],0));
			if (hexval.length() < 2) {
				for (int j = 0; j < 2-hexval.length(); j ++) {
					// DEBUG System.out.println("Adding 0 to " + hexval);
					hexval = "0" + hexval;
				}
			}
			retval += "\\" + hexval;
		}
		return retval;
	}
	public String toHuman() {
		String retval = "";
		String hexval = "";
		int ordering[] = {3,2,1,0,5,4,7,6,8,9,10,11,12,13,14,15};
		for (int i=0; i< ByteCount; i++) {
			hexval = Integer.toHexString(unsignextend(GUIDBytes[ordering[i]],0));
			if (hexval.length() < 2) {
				for (int j=0; j< 2-hexval.length(); j++) {
					hexval = "0" + hexval;
				}
			}
			retval += hexval;
			if (i == 3 || i == 5 || i == 7 || i == 9) {
				retval += "-";
			}
		}
		return retval;
	}
	private int unsignextend(byte x, int shift) {
		int y;
		y = x;
		if (y < 0) {
			y += 256;
		}
		for (int i = 0; i < shift; i++) {
			y *= 256;
		}
		return y;
	}
	
	@Override public int hashCode() {
		int retval = 42;
		for (int i=0;i<ByteCount;i++) {
			retval += unsignextend(GUIDBytes[i],i);    // will overflow and rotate as necessary
		}
		return(retval);
	}
	
	@Override public boolean equals(Object other) {
		boolean retval = true;
		if (other == null) {
			return false;
		}
		if (! other.getClass().getName().equalsIgnoreCase(this.getClass().getName())) {
			return false;
		}
		for (int i = 0; i < ByteCount; i++) {
			if (GUIDBytes[i] != (byte) (((MsGUID)(other)).GUIDBytes[i])) {
				retval = false;
			}
		}
		return(retval);
	}
	
	
	
	public byte[] Serialize() {
		return GUIDBytes;  
	}
}
