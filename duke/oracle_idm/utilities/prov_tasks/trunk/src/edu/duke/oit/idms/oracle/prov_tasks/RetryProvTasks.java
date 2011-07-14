package edu.duke.oit.idms.oracle.prov_tasks;

import java.io.FileInputStream;

import java.util.HashMap;
import java.util.Properties;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcProvisioningOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

/**
 * @author shilen
 */
public class RetryProvTasks {

  private static tcUtilityFactory ioUtilityFactory = null;

  /**
   * Used to retry open provisioning tasks for xelsysadm based on the connector name.
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: RetryProvTasks <CONNECTOR NAME>");
      System.exit(0);
    }
    
    try {
      Properties cfg = new Properties();
      cfg.load(new FileInputStream(System.getenv("OIM_APP_HOME")
          + "/conf/properties.conf"));

      ConfigurationClient.ComplexSetting config = ConfigurationClient
          .getComplexSettingByPath("Discovery.CoreServer");

      ioUtilityFactory = new tcUtilityFactory(config.getAllSettings(), cfg
          .getProperty("oim.login.username"), cfg.getProperty("oim.login.password"));

      tcProvisioningOperationsIntf moProvUtility = (tcProvisioningOperationsIntf) ioUtilityFactory
          .getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");

      tcResultSet moResultSet = moProvUtility.getAssignedOpenProvisioningTasks(1, new HashMap<String, String>(), new String[0]);

      int count = 0;
      System.out.println("CONNECTOR - USER - TASK - RETRY STATUS");

      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String objectName = moResultSet.getStringValue("Objects.Name");
        String targetUser = moResultSet.getStringValue("Process Instance.Task Information.Target User");
        String taskName = moResultSet.getStringValue("Process Definition.Tasks.Task Name");
        long taskInstanceKey = moResultSet.getLongValue("Process Instance.Task Details.Key");

        if (objectName.equals(args[0])) {
          String result = moProvUtility.retryTask(taskInstanceKey);
          System.out.println(objectName + " - " + targetUser + " - " + taskName + " - " + result);
          count++;
        }
      }

      System.out.println("Finished retrying " + count + " task(s).");

      System.exit(0);
    } catch (Exception e) {
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
