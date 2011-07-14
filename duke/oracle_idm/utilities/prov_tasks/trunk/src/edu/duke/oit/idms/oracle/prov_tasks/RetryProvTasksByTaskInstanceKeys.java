package edu.duke.oit.idms.oracle.prov_tasks;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;

import java.util.Properties;

import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcProvisioningOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

/**
 * @author shilen
 */
public class RetryProvTasksByTaskInstanceKeys {

  private static tcUtilityFactory ioUtilityFactory = null;

  private static BufferedReader br = null;

  /**
   * Used to retry open provisioning tasks for xelsysadm based on a list of task instance keys
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: RetryProvTasks <FILE OF TASK INSTANCE KEYS>");
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

      int count = 0;
      System.out.println("TASK INSTANCE KEY - RETRY STATUS");

      String taskInstanceKeyString = null;
      br = new BufferedReader(new FileReader(args[0]));
      while ((taskInstanceKeyString = br.readLine()) != null) {
        long taskInstanceKey = Integer.parseInt(taskInstanceKeyString);
        
        String result = moProvUtility.retryTask(taskInstanceKey);
        System.out.println(taskInstanceKey + " - " + result);
        count++;
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
      
      if (br != null) {
        br.close();
      }
    }

  }

}
