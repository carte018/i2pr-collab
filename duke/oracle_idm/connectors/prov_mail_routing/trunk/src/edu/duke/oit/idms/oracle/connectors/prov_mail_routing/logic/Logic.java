package edu.duke.oit.idms.oracle.connectors.prov_mail_routing.logic;

import java.util.Map;

import javax.naming.directory.SearchResult;

import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.LDAPConnectionWrapper;
import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.ProvisioningDataImpl;

/**
 * @author shilen
 */
public interface Logic {

  /**
   * @param provisioningData 
   * @param ldapConnectionWrapper
   * @param duLDAPKey 
   * @param entryType 
   * @param attrs
   * @param result
   * @return boolean true if mailDrop was modified.
   */
  public boolean updateMailDrop(ProvisioningDataImpl provisioningData, LDAPConnectionWrapper ldapConnectionWrapper, 
      String duLDAPKey, String entryType, Map<String, String> attrs, SearchResult result);
}
