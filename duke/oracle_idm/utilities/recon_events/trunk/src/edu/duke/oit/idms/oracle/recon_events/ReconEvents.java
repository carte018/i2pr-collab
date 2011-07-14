package edu.duke.oit.idms.oracle.recon_events;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcReconciliationOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

/**
 * @author shilen
 */
public class ReconEvents {

  protected static org.apache.log4j.Logger LOG = Logger.getLogger(ReconEvents.class);

  private static tcUtilityFactory ioUtilityFactory = null;

  /**
   * Used to query the reconciliations that have happened in the last day and 
   * send an email for any reconciliations that haven't been linked or manually closed.
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    try {
      PropertyConfigurator.configure(System.getenv("OIM_APP_HOME")
          + "/conf/log4j.properties");
      Properties cfg = new Properties();
      cfg.load(new FileInputStream(System.getenv("OIM_APP_HOME")
          + "/conf/properties.conf"));

      ConfigurationClient.ComplexSetting config = ConfigurationClient
          .getComplexSettingByPath("Discovery.CoreServer");

      ioUtilityFactory = new tcUtilityFactory(config.getAllSettings(), cfg
          .getProperty("oim.login.username"), cfg.getProperty("oim.login.password"));

      tcReconciliationOperationsIntf moReconUtility = (tcReconciliationOperationsIntf) ioUtilityFactory
          .getUtility("Thor.API.Operations.tcReconciliationOperationsIntf");

      StringBuffer buffer = new StringBuffer();
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

      // start date is yesterday
      Date startDate = new Date(System.currentTimeMillis() - 86400000);
      Date endDate = new Date(System.currentTimeMillis());

      String start = format.format(startDate);
      String end = format.format(endDate);

      long before = System.currentTimeMillis();
      tcResultSet moResultSet = moReconUtility.findReconciliationEvent(
          new HashMap<String, String>(), start, end);
      long after = System.currentTimeMillis();
      long diff = after - before;

      buffer
          .append("There are reconciliation events that have not been linked or closed since "
              + start + ".  The data was pulled in " + diff + "ms.  Below you will find the Event IDs.\n\n");

      int count = 0;
      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String key = moResultSet.getStringValue("Reconciliation Manager.Key");
        String status = moResultSet.getStringValue("Reconciliation Manager.Status");

        if (status == null
            || (!status.equals("Event Linked") && !status.equals("Event Closed"))) {

          if (count > 0) {
            buffer.append(", ");
          }

          count++;
          buffer.append(key + " (" + status + ")");
        }

      }

      if (count > 0) {
        buffer.append("\n\n--- Total found: " + count + "\n");
        LOG.error(buffer.toString());
      } else {
        LOG.info("No pending events found.");
      }

      System.exit(0);
    } catch (Exception e) {
      LOG.error("Exception while running application", e);
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (ioUtilityFactory != null) {
        ioUtilityFactory.close();
        ioUtilityFactory = null;
      }
    }

  }

}
