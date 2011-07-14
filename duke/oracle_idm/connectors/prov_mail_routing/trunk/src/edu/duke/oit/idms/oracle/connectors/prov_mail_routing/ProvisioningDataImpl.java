package edu.duke.oit.idms.oracle.connectors.prov_mail_routing;

import java.util.ArrayList;
import java.util.Iterator;

import edu.duke.oit.idms.oracle.connectors.prov_mail_routing.logic.Logic;
import edu.duke.oit.idms.oracle.util.ProvisioningData;


/**
 * @author shilen
 *
 */
public class ProvisioningDataImpl extends ProvisioningData {
  
  /**
   * connector name
   */
  public final static String connectorName = MailRoutingProvisioning.connectorName;
  
  private static ProvisioningDataImpl cfg = null;
  private ArrayList<Class<Logic>> departmentalRulesClasses = new ArrayList<Class<Logic>>();

  @SuppressWarnings("unchecked")
  private ProvisioningDataImpl() {
    super(connectorName);
    
    int count = 1;
    while (true) {
      String value = this.getProperty("logic.class." + count);
      if (value == null || value.isEmpty()) {
        break;
      }
      
      try {
        departmentalRulesClasses.add((Class<Logic>) Class.forName(value));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      
      count++;
    }
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
   * Get logic classes for departmental rules
   * @param attribute
   * @return Logic
   */
  protected ArrayList<Logic> getLogicClasses() {
    try {
      ArrayList<Logic> allLogic = new ArrayList<Logic>();
      Iterator<Class<Logic>> iter = departmentalRulesClasses.iterator();
      while (iter.hasNext()) {
        allLogic.add(iter.next().newInstance());
      }

      return allLogic;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}