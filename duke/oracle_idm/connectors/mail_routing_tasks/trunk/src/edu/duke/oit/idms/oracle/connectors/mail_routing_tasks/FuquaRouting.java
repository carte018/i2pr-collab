package edu.duke.oit.idms.oracle.connectors.mail_routing_tasks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcTaskNotFoundException;
import Thor.API.Exceptions.tcUserNotFoundException;
import Thor.API.Operations.TaskDefinitionOperationsIntf;
import Thor.API.Operations.tcProvisioningOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.util.AttributeData;

/**
 * @author shilen
 */
public class FuquaRouting extends SchedulerBaseTask {

  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private tcUserOperationsIntf moUserUtility;
  private tcProvisioningOperationsIntf moProvUtility;
  private TaskDefinitionOperationsIntf moTaskUtility;
  private AttributeData attributeData;

  /**
   * name of connector
   */
  public final static String connectorName = "FUQUA_ROUTING";

  protected void execute() {
    logger.info(connectorName + ": Starting task.");
    attributeData = AttributeData.getInstance();

    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf", e);
      return;
    }
    
    // Get the tcProvisioningOperationsIntf object
    try {
      moProvUtility = (tcProvisioningOperationsIntf)super.getUtility("Thor.API.Operations.tcProvisioningOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcProvisioningOperationsIntf", e);
      return;
    }
    
    // Get the TaskDefinitionOperationsIntf object
    try {
      moTaskUtility = (TaskDefinitionOperationsIntf)super.getUtility("Thor.API.Operations.TaskDefinitionOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of TaskDefinitionOperationsIntf", e);
      return;
    }

    // Get Fuqua students
    try {
      Set<Integer> users = getFuquaStudents();
      executeTask(users);
    } catch (Exception e) {
      logger.error(connectorName + ": Exception while running task.", e);
    } finally {
      // Clean up
      if (moUserUtility != null) {
        moUserUtility.close();
      }
      
      if (moTaskUtility != null) {
        moTaskUtility.close();
      }
      
      if (moProvUtility != null) {
        moProvUtility.close();
      }
    }

    logger.info(connectorName + ": Task completed");
  }
  
  private Set<Integer> getFuquaStudents() throws tcAPIException, tcColumnNotFoundException {
    Hashtable<String, String> mhSearchCriteria = new Hashtable<String,String>();
    mhSearchCriteria.put("USR_UDF_IS_STUDENT", "1");
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duPSAcadCareerC1"), "FUQ");
    mhSearchCriteria.put("Users.Status", "Active");
    String[] attrs = new String[1];
    attrs[0] = "Users.Key";

    tcResultSet moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
    Set<Integer> users = new HashSet<Integer>();

    for (int i=0; i < moResultSet.getRowCount(); i++) {
      moResultSet.goToRow(i);
      int id = moResultSet.getIntValue("Users.Key");
      users.add(id);
    }
    
    mhSearchCriteria = new Hashtable<String,String>();
    mhSearchCriteria.put("USR_UDF_IS_STUDENT", "1");
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duPSAcadProgC1"), "G-BUS");
    mhSearchCriteria.put("Users.Status", "Active");

    moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);

    for (int i=0; i < moResultSet.getRowCount(); i++) {
      moResultSet.goToRow(i);
      int id = moResultSet.getIntValue("Users.Key");
      users.add(id);
    }
    
    return users;
  }
  
  private void executeTask(Set<Integer> users) throws tcAPIException, tcUserNotFoundException, tcColumnNotFoundException, tcTaskNotFoundException {
    
    Long taskKey = null;
    
    for (Integer userKey : users) {
      
      // first get all objects for user
      tcResultSet moResultSet = moUserUtility.getObjects(userKey);

      // find the provisioned task
      long processInstanceKey = -1;
      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String objectName = moResultSet.getStringValue("Objects.Name");
        String objectStatus = moResultSet.getStringValue("Objects.Object Status.Status");
        if (objectName != null && objectName.equals("MAIL_ROUTING_PROVISIONING") && objectStatus != null && objectStatus.equals("Provisioned")) {
          processInstanceKey = moResultSet.getLongValue("Process Instance.Key");
          break;
        }
      }

      if (processInstanceKey == -1) {
        logger.info(connectorName + ": User key " + userKey + " is not provisioned with the resource.  Skipping.");
        continue;
      }

      // second if we don't already have the task key for the change task, run getTaskKey() to get it.
      if (taskKey == null) {
        taskKey = getTaskKey(processInstanceKey, "Change RunDeptRules");
      }

      // third execute the change task for the user
      moProvUtility.addProcessTaskInstance(taskKey, processInstanceKey);
    }
  }
  
  private Long getTaskKey(long processInstanceKey, String taskName) throws tcAPIException, tcColumnNotFoundException  {

    HashMap<String, String> filter = new HashMap<String, String>();
    filter.put("Process Definition.Tasks.Task Name", taskName);
    tcResultSet moResultSet = moTaskUtility.getTaskDetail(processInstanceKey, filter);

    if (moResultSet.getRowCount() == 1) {
      moResultSet.goToRow(0);
      return moResultSet.getLongValue("Process Definition.Tasks.Key");
    } 

    throw new RuntimeException("Unable to find task key for " + taskName +".");
  }

}
