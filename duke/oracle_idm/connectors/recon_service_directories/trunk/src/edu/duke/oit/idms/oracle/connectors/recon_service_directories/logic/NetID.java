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
public class NetID extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {
    
    if (modificationType == LDAPModification.ADD) {
      String eppn = values[0] + "@duke.edu";
      oimAttributes.put("USR_UDF_EDUPERSONPRINCIPALNAME", eppn);
    } else if (modificationType == LDAPModification.DELETE) {
      oimAttributes.put("USR_UDF_EDUPERSONPRINCIPALNAME", "");
    } else if (modificationType == LDAPModification.REPLACE) {
      if (values != null && values.length > 0) {
        String eppn = values[0] + "@duke.edu";
        oimAttributes.put("USR_UDF_EDUPERSONPRINCIPALNAME", eppn);
      } else {
        oimAttributes.put("USR_UDF_EDUPERSONPRINCIPALNAME", "");
      }
    }
  }
}
