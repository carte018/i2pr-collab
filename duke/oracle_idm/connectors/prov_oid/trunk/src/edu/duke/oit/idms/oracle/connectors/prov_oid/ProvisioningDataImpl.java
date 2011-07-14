package edu.duke.oit.idms.oracle.connectors.prov_oid;

import edu.duke.oit.idms.oracle.util.ProvisioningData;


/**
 * @author shilen
 *
 */
public class ProvisioningDataImpl extends ProvisioningData {
  
  /**
   * connector name
   */
  public final static String connectorName = OIDProvisioning.connectorName;
  
  private static ProvisioningDataImpl cfg = null;

  private ProvisioningDataImpl() {
    super(connectorName);
  }
  
  /**
   * Get instance of class.
   * @return ProvisioningDataImpl
   */
  protected static ProvisioningDataImpl getInstance() {
    if (cfg == null) {
      cfg = new ProvisioningDataImpl();
    }
    return cfg;
  }
}
