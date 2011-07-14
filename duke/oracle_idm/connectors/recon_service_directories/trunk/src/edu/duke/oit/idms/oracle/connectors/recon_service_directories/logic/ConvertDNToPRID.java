package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.directory.DirContext;

import org.apache.log4j.Logger;

import Thor.API.Operations.tcUserOperationsIntf;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;
import edu.duke.oit.idms.urn.URNLookup;


/**
 * @author shilen
 *
 */
public class ConvertDNToPRID extends LogicBase {

  private org.apache.log4j.Logger LOG = Logger.getLogger(ConvertDNToPRID.class);

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {

    if (values == null || values.length == 0) {
      return;
    }

    if (personRegistryHelper == null) {
      // feed to PR must be disabled...
      return;
    }
    
    try {

      String urn = URNLookup.getUrn(attributeName);

      PersonRegistryAttribute prAttr = prAttributes.get(urn);
      List<String> valuesToAddAfterConvert = new ArrayList<String>();
      List<String> valuesToRemoveAfterConvert = new ArrayList<String>();

      Iterator<String> valuesToAddBeforeConvert = prAttr.getValuesToAdd().iterator();
      Iterator<String> valuesToRemoveBeforeConvert = prAttr.getValuesToRemove()
          .iterator();
      
      boolean errorFindingPRID = false;

      while (valuesToAddBeforeConvert.hasNext()) {
        long prid = personRegistryHelper.getPRIDFromLDAPKey(OIMAPIWrapper
            .getLDAPKeyFromDN(valuesToAddBeforeConvert.next()));
        if (prid < 0) {
          errorFindingPRID = true;
        }
        valuesToAddAfterConvert.add("" + prid);
      }
      
      while (valuesToRemoveBeforeConvert.hasNext()) {
        long prid = personRegistryHelper.getPRIDFromLDAPKey(OIMAPIWrapper
            .getLDAPKeyFromDN(valuesToRemoveBeforeConvert.next()));
        if (prid < 0) {
          errorFindingPRID = true;
        }
        valuesToRemoveAfterConvert.add("" + prid);
      }
      
      if (errorFindingPRID) {
        LOG.error("Error while getting the PRID from duLDAPKey. DN " + dn + " might require a resync.");
        valuesToAddAfterConvert = new ArrayList<String>();
        valuesToRemoveAfterConvert = new ArrayList<String>();
      }
      
      prAttr.setValuesToAdd(valuesToAddAfterConvert);
      prAttr.setValuesToRemove(valuesToRemoveAfterConvert);
      prAttributes.put(urn, prAttr);
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
