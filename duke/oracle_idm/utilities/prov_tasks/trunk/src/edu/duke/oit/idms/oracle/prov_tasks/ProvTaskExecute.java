package edu.duke.oit.idms.oracle.prov_tasks;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.TaskDefinitionOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.xl.util.config.ConfigurationClient;

import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;

/**
 * @author shilen
 */
public class ProvTaskExecute {

  private static tcUtilityFactory ioUtilityFactory = null;
  private static BufferedReader br = null;

  /**
   * Used to run a task for a list of users with a given connector.
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    System.out.println("This script is used to execute a single task for a given connector for a list of users.  " +
        "This script will not provision users.");
    
    if (args.length != 3) {
      System.out.println("Usage: ProvResync <CONNECTOR NAME> <FILE OF DUKEIDs> <TASK>");
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
      tcUserOperationsIntf moUserUtility = (tcUserOperationsIntf) ioUtilityFactory
          .getUtility("Thor.API.Operations.tcUserOperationsIntf");
      TaskDefinitionOperationsIntf moTaskUtility = (TaskDefinitionOperationsIntf) ioUtilityFactory
          .getUtility("Thor.API.Operations.TaskDefinitionOperationsIntf");

      System.out.println("\n\nDUKEID - CHANGE TASK EXECUTED - TASK INSTANCE KEY");

      HashMap<Long, String> taskKeys = null;

      String dukeid = null;
      br = new BufferedReader(new FileReader(args[1]));
      while ((dukeid = br.readLine()) != null) {
        
        // first get user key
        long userKey = OIMAPIWrapper.getUserKeyFromDukeID(moUserUtility, dukeid);
        
        // second get all objects for user
        tcResultSet moResultSet = moUserUtility.getObjects(userKey);

        // find the provisioned task
        long processInstanceKey = -1;
        for (int i = 0; i < moResultSet.getRowCount(); i++) {
          moResultSet.goToRow(i);
          String objectName = moResultSet.getStringValue("Objects.Name");
          String objectStatus = moResultSet.getStringValue("Objects.Object Status.Status");
          if (objectName != null && objectName.equals(args[0]) && objectStatus != null && objectStatus.equals("Provisioned")) {
            processInstanceKey = moResultSet.getLongValue("Process Instance.Key");
            break;
          }
        }
        
        if (processInstanceKey == -1) {
          System.out.println("DukeID " + dukeid + " is not provisioned with the resource.  Skipping resync.");
          continue;
        }
        
        // third if we don't already have the task keys for the change tasks, run getTaskKeys() to get them.
        if (taskKeys == null) {
          taskKeys = getTaskKeys(moTaskUtility, processInstanceKey, args[2]);
        }
        
        // forth execute each change task for the user
        Iterator<Long> iter = taskKeys.keySet().iterator();
        while (iter.hasNext()) {
          long taskKey = iter.next().longValue();
          long taskInstanceKey = moProvUtility.addProcessTaskInstance(taskKey, processInstanceKey);
          System.out.println(dukeid + " - " + taskKeys.get(taskKey) + " - " + taskInstanceKey);
        }

      }

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

  private static HashMap<Long, String> getTaskKeys(TaskDefinitionOperationsIntf moTaskUtility,
      long processInstanceKey, String taskName) throws tcAPIException, tcColumnNotFoundException {
    
    HashMap<Long, String> taskKeys = new HashMap<Long, String>();
    
    HashMap<String, String> filter = new HashMap<String, String>();
    filter.put("Process Definition.Tasks.Task Name", taskName);
    tcResultSet moResultSet = moTaskUtility.getTaskDetail(processInstanceKey, filter);
    
    for (int i = 0; i < moResultSet.getRowCount(); i++) {
      moResultSet.goToRow(i);
      long taskKey = moResultSet.getLongValue("Process Definition.Tasks.Key");
      String currTaskName = moResultSet.getStringValue("Process Definition.Tasks.Task Name");
      taskKeys.put(new Long(taskKey), currTaskName);
    }

    return taskKeys;
  }

}
