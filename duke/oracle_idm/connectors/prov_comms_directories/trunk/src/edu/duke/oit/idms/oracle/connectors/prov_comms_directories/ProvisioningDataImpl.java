package edu.duke.oit.idms.oracle.connectors.prov_comms_directories;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.duke.oit.idms.oracle.util.ProvisioningData;

@SuppressWarnings("unchecked")

/**
 * @author shilen
 *
 */
public class ProvisioningDataImpl extends ProvisioningData {

  /**
   * connector name
   */
  public final static String connectorName = "COMMSDIR_PROVISIONING2";

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

  /**
   * Whether createMailbox should be disabled in the adapter code.
   * @return boolean
   */
  public boolean isCreateMailboxDisabled() {

    String value = (String) cfg.getAllProperties().get("createmailbox.disabled");
    if (value.equals("true")){
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Whether createMailbox should be disabled without errors in the adapter code.
   * @return boolean
   */
  public boolean isCreateMailboxDisabledWithoutErrors() {

    String value = (String) cfg.getAllProperties().get("createmailbox.disabledwithouterrors");
    if (value.equals("true")){
      return true;
    }
    else {
      return false;
    }
  }

  public void showAllProperties() {

    Map<String, String> allProperties = cfg.getAllProperties();
    Iterator<String> iter = allProperties.keySet().iterator();
    while (iter.hasNext()) {
      String property = iter.next();
      String value = allProperties.get(property); 
      CommsDirectoriesProvisioning.logger.info(CommsDirectoriesProvisioning.connectorName + " property=" + property + " has value=" + value);
    }
  }  

  public HashMap<String, String> getMailOnlyAttributes() {
    HashMap<String, String> mailOnlyAttributes = new HashMap<String, String>();
    HashMap<String, String> props = (HashMap<String, String>) getAllProperties();
    Set<String> keys = props.keySet();
    Iterator<String> keyIterator = keys.iterator();
    while (keyIterator.hasNext()) {
      String key = keyIterator.next();
      if (key.startsWith("default.attribute.")) {
        String value = props.get(key);
        key = key.substring("default.attribute.".length());
        mailOnlyAttributes.put(key, value);
      }
    }		
    return mailOnlyAttributes;
  }
}
