package edu.duke.oit.idms.oracle.connectors.recon_edir;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.Operations.tcUserOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.util.AttributeData;


public class RunConnector extends SchedulerBaseTask {
  public static String MISSING = "";
  public static String SEARCH_PATTERN = "*";
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
  private tcUserOperationsIntf moUserUtility;
  private AttributeData attributeData;
  private String[] reconAttrs = {"duEmailAlias","duNotesTargetAddress","duEmailAliasTarget"};
  
  public final static String connectorName = "EDIR_RECONCILIATION";

  protected void execute() {
    logger.info(connectorName + ": Starting task.");
    attributeData = AttributeData.getInstance();
        
    tcITResourceInstanceOperationsIntf moITResourceUtility=null;
    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcITResourceInstanceOperationsIntf");
      return;
    }

    // Get the list of people from eDirectory - Anyone with a DukeID
    LDAPConnectionWrapper ldapConnectionWrapper = new LDAPConnectionWrapper(moITResourceUtility);
    HashMap<String,Map<String,String>> eDirUsers =  ldapConnectionWrapper.doSearch("(dudukeid="+SEARCH_PATTERN+")");
    logger.info(connectorName + ": LDAP search complete");

    // Get the tcUserOperationsIntf object
    try {
      moUserUtility = (tcUserOperationsIntf)super.getUtility("Thor.API.Operations.tcUserOperationsIntf");
    } catch (tcAPIException e) {
      logger.error(connectorName + ": Unable to get an instance of tcUserOperationsIntf");
      return;
    }

    // Get a list of all users from OIM
    HashMap<String,Map<String,String>> oimUsers = getAllOIMUsers();    
    logger.info(connectorName + ": OIM search complete");
   
    // Iterate over the OIM users, comparing to the eDir list
    Iterator<String> oimIter = oimUsers.keySet().iterator();
    while (oimIter.hasNext()) {
      String key = oimIter.next();
      if (eDirUsers.containsKey(key)) {
        Map<String,String>edirVals = eDirUsers.get(key);
        Map<String,String>oimVals = oimUsers.get(key);
        //logger.info(connectorName + ": Here are the attrs:"+edirVals.toString());
        HashMap<String,String> changesMap = new HashMap<String,String>();
        
        for (int i = 0; i < reconAttrs.length; i++) {
          String newVal = edirVals.get(reconAttrs[i]);
          if (!newVal.equals(oimVals.get(reconAttrs[i]))){
            //logger.info(connectorName + ": need to change "+reconAttrs[i]+"to:" + newVal);
            changesMap.put(attributeData.getOIMAttributeName(reconAttrs[i]),newVal);
          } 
        }
        
        if (!changesMap.isEmpty()) {
          //logger.info(connectorName + ": There is something to be done for:" + key+".  Here is the pretty tostring:"+changesMap.toString());       
          try {
            // We need to create a result set containing just this user to do this update
            tcResultSet moSingleResult = findOIMUser(key);
            moUserUtility.updateUser(moSingleResult, changesMap);
            logger.info(connectorName + ": updated user:"+key);
         } catch (Exception e) {
            logger.error(connectorName + ": Error occured trying to update an OIM user("+key+"). Error:"+e.toString());
         }
        }
      } 
    }
    // Clean up and go home
    if (moUserUtility != null) {
      moUserUtility.close();
    }
    if (moITResourceUtility != null) {
      moITResourceUtility.close();
    }
    logger.info(connectorName + ": Task completed");
  }

 
  /**
   * Return a hash containing duDukeId,duNotesTargetAddress, duEmailAlias and duEmailAliasTarget of all users in OIM.
   * We use this for the big comparison that leads to updates of this information in OIM by the reconciler.
   */
  private HashMap<String,Map<String,String>> getAllOIMUsers() {

    Hashtable<String, String> mhSearchCriteria = new Hashtable<String,String>();
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeID"),SEARCH_PATTERN);
    mhSearchCriteria.put("Users.Status", "Active");
    tcResultSet moResultSet = null;
    String[] getFields = new String[reconAttrs.length+1];
    for (int i = 0; i < reconAttrs.length; i++) {
      getFields[i] = attributeData.getOIMAttributeName(reconAttrs[i]);
    }
    getFields[reconAttrs.length] = attributeData.getOIMAttributeName("duDukeId");
    Integer nOimUsers;
    
    try {
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria,getFields);
      nOimUsers = moResultSet.getRowCount();
    } catch (tcAPIException e) {
      throw new RuntimeException(e);
    }

    HashMap<String,Map<String,String>> retval =  new HashMap<String,Map<String,String>>();

    for (int i=0; i < nOimUsers; i++) {
      try {
        moResultSet.goToRow(i);
        String id = moResultSet.getStringValue(attributeData.getOIMAttributeName("duDukeID"));
        try {
          Map<String, String> oimVals = new HashMap<String, String>();
          for (int j = 0; j < reconAttrs.length; j++) {
            String val = moResultSet.getStringValue(attributeData.getOIMAttributeName(reconAttrs[j]));
            oimVals.put(reconAttrs[j], val.isEmpty() ? MISSING:val);
          }
          retval.put(id, oimVals);
        }catch (Exception e1){
          logger.error(connectorName + ": Error occured setting up an OIM user. ID: "+id+". Error:"+e1.toString());
        }
      } catch (Exception e) {
        logger.error(connectorName + ": Error occured setting up an OIM user before id was read. Error:"+e.toString());
      }
    }
    return retval;
  }
  
  private tcResultSet findOIMUser(String duDukeId) throws Exception {
    String[] attrs = new String[2];
    attrs[0] = "Users.Key";
    attrs[1] = "Users.User ID";
 
    Hashtable<String,String> mhSearchCriteria = new Hashtable<String,String>();
    if (attributeData == null) {
      attributeData = AttributeData.getInstance();
    }
    mhSearchCriteria.put(attributeData.getOIMAttributeName("duDukeId"), duDukeId);

    tcResultSet moResultSet;
    try {
      moResultSet = moUserUtility.findUsersFiltered(mhSearchCriteria, attrs);
      if (moResultSet.getRowCount() > 1) {
        throw new Exception("Got " + moResultSet.getRowCount() + " rows for duDukeId " + duDukeId);
      }
    } catch (tcAPIException e) {
      throw new Exception(e);
    }
    
    return moResultSet;
  }
}
