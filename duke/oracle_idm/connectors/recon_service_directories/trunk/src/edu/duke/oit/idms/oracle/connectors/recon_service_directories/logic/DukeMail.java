package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.Map;

import javax.naming.directory.DirContext;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;

import netscape.ldap.LDAPModification;

import Thor.API.Operations.tcUserOperationsIntf;


/**
 * @author shilen
 */
public class DukeMail extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {
    
    PersonRegistryAttribute prAttr = new PersonRegistryAttribute("urn:mace:duke.edu:idms:acpub-email");

    if (modificationType == LDAPModification.ADD) {
      if (values[0].equals("1")) {
        String uid = getAttribute(context, "uid", dn).toLowerCase();
        oimAttributes.put("USR_UDF_ACPUBEMAIL", uid + "@duke.edu");
        prAttr.addValueToAdd(uid + "@duke.edu");
      }
    } else if (modificationType == LDAPModification.DELETE) {
      oimAttributes.put("USR_UDF_ACPUBEMAIL", "");
      prAttr.setRemoveAllValues(true);
    } else if (modificationType == LDAPModification.REPLACE) {
      if (values == null || values.length == 0 || !values[0].equals("1")) {
        oimAttributes.put("USR_UDF_ACPUBEMAIL", "");
        prAttr.setRemoveAllValues(true);
      } else {
        String uid = getAttribute(context, "uid", dn).toLowerCase();
        oimAttributes.put("USR_UDF_ACPUBEMAIL", uid + "@duke.edu");
        prAttr.addValueToAdd(uid + "@duke.edu");
      }
    }
    
    if (prAttr.isModifying()) {
      prAttributes.put(prAttr.getUrn(), prAttr);
    }
  }
}
