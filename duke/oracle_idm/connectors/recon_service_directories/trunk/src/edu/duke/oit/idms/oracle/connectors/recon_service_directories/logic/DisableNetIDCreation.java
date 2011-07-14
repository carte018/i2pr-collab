package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.Map;

import javax.naming.directory.DirContext;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;

import netscape.ldap.LDAPModification;

import Thor.API.Operations.tcUserOperationsIntf;



/**
 * @author shilen
 *
 */
public class DisableNetIDCreation extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {
    
    if (modificationType == LDAPModification.ADD) {
      if (containsCreationOff(values)) {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "1");
      }
    } else if (modificationType == LDAPModification.DELETE) {
      if (values == null || values.length == 0) {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "0");
      } else if (containsCreationOff(values)) {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "0");
      }
    } else if (modificationType == LDAPModification.REPLACE) {
      if (values == null || values.length == 0) {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "0");
      } else if (containsCreationOff(values)) {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "1");
      } else {
        oimAttributes.put("USR_UDF_IS_NETIDCREATEDISABLED", "0");
      }
    }
  }
  
  /**
   * @param values
   * @return
   */
  private boolean containsCreationOff(String[] values) {
    for (int i = 0; i < values.length; i++) {
      if (values[i].toLowerCase().equals("netid autocreate off")) {
        return true;
      }
    }
    return false;
  }

}
