package edu.duke.idms.oracle.security;

public class Permission {
	public String SubjectDN;
	public PermissionMode Mode;
	public PermissionInheritance Inherit;
	public PermissionOperation Operation;
	public String AttributeName;
	public String InheritorName;
}
