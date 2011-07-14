package edu.duke.oit.idms.oracle.prov_tasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcProvisioningOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

/**
 * @author shilen
 */
public class OpenProvTasksReport {

  protected static org.apache.log4j.Logger LOG = Logger.getLogger(OpenProvTasksReport.class);

  private static tcUtilityFactory ioUtilityFactory = null;

  /**
   * Used to send an email of all new open provisioning tasks.
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

      tcProvisioningOperationsIntf moProvUtility = (tcProvisioningOperationsIntf) ioUtilityFactory
          .getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");
      
      BufferedReader br = new BufferedReader(new FileReader(System.getenv("OIM_APP_HOME")
          + "/work/last_task_instance_key"));
      
      long lastTaskInstanceKey = new Long(br.readLine()).longValue();
      br.close();

      tcResultSet moResultSet = moProvUtility.getAssignedOpenProvisioningTasks(1, new HashMap<String, String>(), new String[0]);
      StringBuffer buffer = new StringBuffer();
      long maxTaskInstanceKey = 0;
      int totalCount = 0;
      int newCount = 0;

      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String objectName = moResultSet.getStringValue("Objects.Name");
        String targetUser = moResultSet.getStringValue("Process Instance.Task Information.Target User");
        long taskInstanceKey = moResultSet.getLongValue("Process Instance.Task Details.Key");

        if (!objectName.equals("Installation")) {
          totalCount++;

          if (taskInstanceKey > lastTaskInstanceKey) {
            newCount++;

            if (taskInstanceKey > maxTaskInstanceKey) {
              maxTaskInstanceKey = taskInstanceKey;
            }

            if (newCount > 1) {
              buffer.append(", ");
            }

            buffer.append(objectName + "/" + targetUser);
          }
        }

      }

      if (newCount > 0) {
        StringBuffer bufferAll = new StringBuffer();
        bufferAll.append("Number of new open provisioning tasks = " + newCount + ".  Total open provisioning tasks = " + totalCount + ".  ");
        bufferAll.append("Below you will find the connectors and users associated with the new provisioning tasks.\n\n");
        bufferAll.append(buffer);
        LOG.error(bufferAll.toString());

        BufferedWriter bw = new BufferedWriter(new FileWriter(System.getenv("OIM_APP_HOME")
          + "/work/last_task_instance_key"));
      
        bw.write("" + maxTaskInstanceKey);
        bw.close();
      } else {
        LOG.info("No new open provisioning tasks.");
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
