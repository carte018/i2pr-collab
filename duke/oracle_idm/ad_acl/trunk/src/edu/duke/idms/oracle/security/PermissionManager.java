package edu.duke.idms.oracle.security;

import java.util.Set;

public interface PermissionManager {
	public boolean hasPermission(Object context,SecuredResource SR,Permission P);
	public void addPermission(Object context,SecuredResource SR,Set<Permission> perms);
	public void removePermission(Object context,SecuredResource SR, Set<Permission> perms);
	public void addPermission(Object context,SecuredResource SR,Permission perm);
	public void removePermission(Object context,SecuredResource SR,Permission perm);
}
