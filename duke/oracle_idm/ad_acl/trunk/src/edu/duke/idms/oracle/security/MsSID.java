package edu.duke.idms.oracle.security;
import java.util.ArrayList;
public class MsSID {
	ArrayList<Byte> SID;
	int SidSize;
	
	protected MsSID() {
		SID = new ArrayList<Byte>();
		SidSize = 0;
	}
	
	protected MsSID(byte[] bytes) {
		
		if (bytes == null) {
			SID = new ArrayList<Byte>();
			SidSize = 0;
			return;
		}
		SID = new ArrayList<Byte>();
		SidSize = 8+(4*((bytes[1]<0)?bytes[1]+256:bytes[1]));
		for (int i = 0; i < SidSize; i++) {
			SID.add(new Byte(bytes[i]));
		}
	}
	@Override public String toString() {
		String retval = "";
		
		if (SidSize == 0) {
			return retval;
		}

		retval = "S-" + (int)(SID.get(0).byteValue());
		retval += "-" + (unsignextend(SID.get(7).byteValue(),0) + unsignextend(SID.get(6).byteValue(),1) + unsignextend(SID.get(5).byteValue(),2) + unsignextend(SID.get(4).byteValue(),3) + unsignextend(SID.get(3).byteValue(),4) + unsignextend(SID.get(2).byteValue(),5));
		int count = 0;
		// DEBUG System.out.println("Expecting " +  SID.get(1).byteValue());
		for (int i = 0; i < (int) (SID.get(1).byteValue()); i ++) {
			//retval += "-" + ((int)(SID.get(8+count).byteValue()) + (int)(SID.get(9+count).byteValue() << 8) + (int)(SID.get(10+count).byteValue() << 16 + (int)(SID.get(11+count).byteValue() << 24)));
			retval += "-" + (unsignextend(SID.get(8+count).byteValue(),0) + unsignextend(SID.get(9+count).byteValue(),1) + unsignextend(SID.get(10+count).byteValue(),2) + unsignextend(SID.get(11+count).byteValue(),3));
			count += 4;
		}

		return(retval);
	}	
	private long unsignextend(byte x, int shift) {
		long y;
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
		for (int i = 0; i < SidSize; i++) {
			retval += unsignextend(SID.get(i),i);	// This will roll over as needed
		}
		return(retval);
	}
	
	@Override public boolean equals(Object other) {
		boolean retval = false;
		if (other == null) {
			return false;
		}
		if (! other.getClass().getName().equalsIgnoreCase(this.getClass().getName())) {
			return false;
		}
		if (this.toString().equals(other.toString())) {
			retval = true;
		}
		return(retval);
	}
	
	
	
	public byte[] Serialize() {
		byte[] retval = new byte[SidSize];
		if (SID != null) {
			for (int i = 0; i < SidSize; i++) {
				retval[i] = SID.get(i);
			}
			return(retval);
		} else {
			return null;  // placeholder
		}
	}
}
