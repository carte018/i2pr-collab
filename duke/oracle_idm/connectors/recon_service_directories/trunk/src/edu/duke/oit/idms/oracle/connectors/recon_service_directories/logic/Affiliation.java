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
public class Affiliation extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {

    if (modificationType == LDAPModification.ADD) {
      for (int i = 0; i < values.length; i++) {
        oimAttributes.put("USR_UDF_IS_" + values[i].toUpperCase(), "1");
      }
    } else if (modificationType == LDAPModification.REPLACE) {
      oimAttributes.put("USR_UDF_IS_STAFF", "0");
      oimAttributes.put("USR_UDF_IS_STUDENT", "0");
      oimAttributes.put("USR_UDF_IS_EMERITUS", "0");
      oimAttributes.put("USR_UDF_IS_FACULTY", "0");
      oimAttributes.put("USR_UDF_IS_ALUMNI", "0");
      oimAttributes.put("USR_UDF_IS_AFFILIATE", "0");
      
      for (int i = 0; i < values.length; i++) {
        oimAttributes.put("USR_UDF_IS_" + values[i].toUpperCase(), "1");
      }
    } else if (modificationType == LDAPModification.DELETE) {
      if (values == null || values.length == 0) {
        oimAttributes.put("USR_UDF_IS_STAFF", "0");
        oimAttributes.put("USR_UDF_IS_STUDENT", "0");
        oimAttributes.put("USR_UDF_IS_EMERITUS", "0");
        oimAttributes.put("USR_UDF_IS_FACULTY", "0");
        oimAttributes.put("USR_UDF_IS_ALUMNI", "0");
        oimAttributes.put("USR_UDF_IS_AFFILIATE", "0");     
      } else {
        for (int i = 0; i < values.length; i++) {
          oimAttributes.put("USR_UDF_IS_" + values[i].toUpperCase(), "0");
        }
      }
    }
  }

}
