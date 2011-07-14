package edu.duke.oit.idms.oracle.provisioning;

import com.thortech.util.logging.Logger;
import com.thortech.xl.dataaccess.tcDataProvider;


/**
 * @author shilen
 *
 */
public abstract class SimpleProvisioning {
  
  /**
   * Logger
   */
  public static Logger logger = Logger.getLogger(SimpleProvisioning.class.getName());
  
  /**
   * Return code for a successful execution.
   */
  public final static String SUCCESS = "C";
  
  /**
   * Provision a user.  This gets called when a user gets provisioned for a resource when the "Create User" task runs.
   * @param dataProvider 
   * @param id This can be duLDAPKey, duDukeID, etc... whatever makes sense for the connector. 
   * @param entryType 
   * @return return code
   */
  public abstract String provisionUser(tcDataProvider dataProvider, String id, String entryType);
  
  /**
   * Update a user.  This gets called when a "Change" task runs.
   * @param dataProvider 
   * @param id This can be duLDAPKey, duDukeID, etc... whatever makes sense for the connector. 
   * @param entryType 
   * @param attribute
   * @param oldValue Note that this will not be available during retries.
   * @param newValue 
   * @return return code
   */
  public abstract String updateUser(tcDataProvider dataProvider, String id,
      String entryType, String attribute, String oldValue, String newValue);
  
  /**
   * Deprovision a user.  This gets called when the "Delete User" task runs.
   * This happens when a user is being deleted or being deprovisioned.
   * @param dataProvider 
   * @param id This can be duLDAPKey, duDukeID, etc... whatever makes sense for the connector. 
   * @param entryType 
   * @return return code
   */
  public abstract String deprovisionUser(tcDataProvider dataProvider, String id, String entryType);
  
  
}
