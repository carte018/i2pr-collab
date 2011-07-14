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
public class DempoEmail extends LogicBase {

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {
    
    PersonRegistryAttribute prAttr = new PersonRegistryAttribute("urn:mace:duke.edu:idms:dempo-email");
    
    if (attributeName.toLowerCase().equals("dudempoid")) {
      if (modificationType == LDAPModification.ADD) {
        String dudempoemailtarget = getAttribute(context, "dudempoemailtarget", dn);
        if (dudempoemailtarget != null && !dudempoemailtarget.equals("")) {
          String newValue = values[0].toLowerCase() + "@mc.duke.edu";
          oimAttributes.put("USR_UDF_DEMPOEMAIL", newValue);
          prAttr.addValueToAdd(newValue);
        }
      } else if (modificationType == LDAPModification.DELETE) {
        oimAttributes.put("USR_UDF_DEMPOEMAIL", "");
        prAttr.setRemoveAllValues(true);
      } else if (modificationType == LDAPModification.REPLACE) {
        if (values != null && values.length > 0) {
          String dudempoemailtarget = getAttribute(context, "dudempoemailtarget", dn);
          if (dudempoemailtarget != null && !dudempoemailtarget.equals("")) {
            String newValue = values[0].toLowerCase() + "@mc.duke.edu";
            oimAttributes.put("USR_UDF_DEMPOEMAIL", newValue);
            prAttr.addValueToAdd(newValue);
          }
        } else {
          oimAttributes.put("USR_UDF_DEMPOEMAIL", "");
          prAttr.setRemoveAllValues(true);
        }
      }
    } else if (attributeName.toLowerCase().equals("dudempoemailtarget")) {
      if (modificationType == LDAPModification.ADD) {
        String dudempoid = getAttribute(context, "dudempoid", dn);
        if (dudempoid != null && !dudempoid.equals("")) {
          String newValue = dudempoid.toLowerCase() + "@mc.duke.edu";
          oimAttributes.put("USR_UDF_DEMPOEMAIL", newValue);
          prAttr.addValueToAdd(newValue);
        }
      } else if (modificationType == LDAPModification.DELETE) {
        oimAttributes.put("USR_UDF_DEMPOEMAIL", "");
        prAttr.setRemoveAllValues(true);
      } else if (modificationType == LDAPModification.REPLACE) {
        if (values != null && values.length > 0) {
          String dudempoid = getAttribute(context, "dudempoid", dn);
          if (dudempoid != null && !dudempoid.equals("")) {
            String newValue = dudempoid.toLowerCase() + "@mc.duke.edu";
            oimAttributes.put("USR_UDF_DEMPOEMAIL", newValue);
            prAttr.addValueToAdd(newValue);
          }
        } else {
          oimAttributes.put("USR_UDF_DEMPOEMAIL", "");
          prAttr.setRemoveAllValues(true);
        }
      }
    }
    
    if (prAttr.isModifying()) {
      prAttributes.put(prAttr.getUrn(), prAttr);
    }
  }
}
