package edu.duke.oit.idms.oracle.connectors.recon_validate;

import edu.duke.oit.idms.oracle.util.ProvisioningData;


/**
 * @author rob
 *
 */
public class ValidateDataImpl extends ProvisioningData {
  
  /**
   * connector name
   */
  public final static String connectorName = "VALIDATE_RECONCILIATION";
  
  private static ValidateDataImpl cfg = null;

  private ValidateDataImpl() {
    super(connectorName);
  }
  
  /**
   * Get instance of class.
   * @return ProvisioningDataImpl
   */
  protected static ValidateDataImpl getInstance() {
    if (cfg == null) {
      cfg = new ValidateDataImpl();
    }
    return cfg;
  }
}
