package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.Map;

import javax.naming.directory.DirContext;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;

import Thor.API.Operations.tcUserOperationsIntf;


/**
 * @author shilen
 *
 */
public interface Logic {

  /**
   * Perform Logic
   * @param oimAttributes 
   * @param prAttributes 
   * @param attributeName 
   * @param values 
   * @param modificationType 
   * @param moUserUtility 
   * @param context 
   * @param personRegistryHelper 
   * @param dn
   */
  public void doSomething(Map<String, String> oimAttributes, Map<String, PersonRegistryAttribute> prAttributes,
      String attributeName, String[] values, int modificationType,
      tcUserOperationsIntf moUserUtility, DirContext context, PersonRegistryHelper personRegistryHelper, String dn);
}

