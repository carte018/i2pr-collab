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
public class ChallengeResponse extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {

    if (modificationType == LDAPModification.ADD) {
      oimAttributes.put("USR_UDF_HAS_CHALRESP", "1");
    } else if (modificationType == LDAPModification.DELETE) {
      oimAttributes.put("USR_UDF_HAS_CHALRESP", "0");
    } else if (modificationType == LDAPModification.REPLACE) {
      if (values != null && values.length > 0) {
        oimAttributes.put("USR_UDF_HAS_CHALRESP", "1");
      } else {
        oimAttributes.put("USR_UDF_HAS_CHALRESP", "0");
      }
    }
  }
}
