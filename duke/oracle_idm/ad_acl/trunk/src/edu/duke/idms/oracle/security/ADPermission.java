package edu.duke.idms.oracle.security;

public class ADPermission extends Permission {
	
	public ADPermission() {
		SubjectDN = null;
		Mode = null;
		Inherit = null;
		Operation = null;
		AttributeName = null;
		InheritorName = null;
	}
	
	public ADPermission(String dn,PermissionMode mode,PermissionInheritance inherit,PermissionOperation op,String attr,String inheritor) {
		SubjectDN = dn;
		Mode = mode;
		Inherit = inherit;
		Operation = op;
		AttributeName = attr;
		InheritorName = inheritor;
	}
	
	@Override public int hashCode() {
		int retval = 42;
		retval += SubjectDN.hashCode();
		retval += AttributeName.hashCode();
		if (Mode == PermissionMode.PERM_ALLOW) {
			retval *= 8;
		} else {
			retval *= 2;
		}
		if (Inherit == PermissionInheritance.PERM_SELF) {
			retval /= 4;
		} else {
			retval /= 2;
		}
		if (Operation == PermissionOperation.PERM_READ) {
			retval *= 3;
		} else {
			retval *= 5;
		}
		return retval;
	}
	
	@Override public boolean equals(Object other) {
		if (other == null || ! other.getClass().getName().equals(this.getClass().getName())) {
			return false;
		}
		ADPermission compare = (ADPermission) other;
		if (((SubjectDN == null && compare.SubjectDN != null) || (SubjectDN != null && compare.SubjectDN == null)) || ! SubjectDN.equals(compare.SubjectDN)) {
			return false;
		}
		if (((AttributeName == null && compare.AttributeName != null) || (AttributeName != null && compare.AttributeName == null)) || ! AttributeName.equals(compare.AttributeName)) {
			return false;
		}
		if (((Mode == null && compare.Mode != null) || (Mode != null && compare.Mode == null)) || (! Mode.equals(compare.Mode))) {
			return false;
		}
		if (((Inherit == null && compare.Inherit != null) || (Inherit != null && compare.Inherit == null)) || (! Inherit.equals(compare.Inherit))){
			return false;
		}
		if (((Operation == null && compare.Operation != null) || (Operation != null && compare.Operation == null)) || (! Operation.equals(compare.Operation))) {
			return false;
		}
		return true;
	}
}
