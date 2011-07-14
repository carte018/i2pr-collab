package edu.duke.oit.idms.oracle.connectors.recon_grouper_to_service_directories;


import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.AttributeInUseException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;


import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.dataaccess.tcDataProvider;
import com.thortech.xl.util.logging.LoggerModules;

/**
 * @author liz, prior art from shilen
 */
public class LDAPConnectionWrapper {

  private String baseDn = "dc=duke,dc=edu";
  //  private String baseDn = "ou=people,dc=duke,dc=edu";
  String regex = "^urn:mace:duke.edu:groups:siss:courses:(\\S+):(\\S+):(\\S+):(\\S+):(\\S+):(\\S+)$";
  Pattern pat = null;

  private tcDataProvider dataProvider = null;

  private LdapContext context = null;
  private boolean testMode = false;
  private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
 
  /**
   * Production entry point - use OIM configuration
   */
  public LDAPConnectionWrapper() {
    this(false);
  }

  /**
   * @param test - This allows this class to be run outside of OIM for debugging.  
   * If test is false it will use OIM.
   */
  public LDAPConnectionWrapper(boolean test) {
    this.testMode = test;
    logger.info( ": Starting task.");

    if (testMode) {
     logger.warn( ": Warning!  Test Mode.");
     this.context = createMockConnection();
    } else {
      this.context = createConnection();
    }
    pat = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
  }


  private LdapContext createMockConnection() {
    Hashtable<String, String> environment = new Hashtable<String, String>(11);
    environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    environment.put(Context.PROVIDER_URL, "ldap://SUPPRESSED");
    environment.put(Context.SECURITY_AUTHENTICATION, "simple");
    environment.put(Context.SECURITY_PRINCIPAL, "SUPPRESSED");
    environment.put(Context.SECURITY_CREDENTIALS, "SUPPRESSED");
    environment.put(Context.SECURITY_PROTOCOL, "ssl");
    // Create the initial context
    LdapContext ctx;
    try {
      ctx = new InitialLdapContext(environment,null);
    } catch (NamingException e) {
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
    }
    return ctx;
  }
  
  /**
   * Create a new connection to LDAP based on properties defined in the IT Resource.
   * @return ldap context
   */
  private LdapContext createConnection() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
      .getUtility(dataProvider,"Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map<String, String> parameters = new HashMap<String, String>();
      Map<String, String> resourceMap = new HashMap<String, String>();
      resourceMap.put("IT Resources.Name", "SVCDIR_PROVISIONING");
      tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
      long resourceKey = moResultSet.getLongValue("IT Resources.Key");

      moResultSet = null;
      moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
        String value = moResultSet
        .getStringValue("IT Resources Type Parameter Value.Value");
        parameters.put(name, value);
      }

      Hashtable<String, String> environment = new Hashtable<String, String>();
      environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      environment.put(Context.PROVIDER_URL, (String) parameters.get("providerUrl"));
      environment.put(Context.SECURITY_AUTHENTICATION, "simple");
      environment.put(Context.SECURITY_PRINCIPAL, (String) parameters.get("userDN"));
      environment.put(Context.SECURITY_CREDENTIALS, (String) parameters.get("userPassword"));
      environment.put(Context.SECURITY_PROTOCOL, "ssl");
      return new InitialLdapContext(environment, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed while creating LDAP connection: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }

  /**
   * @return ldap context
   */
  public LdapContext getConnection() {
    return context;
  }

  /**
   * Get an LDAP entry
   * @param duDukeId
   * @return SearchResult
   */
  public SearchResult findEntry(String duDukeId) {

    String filter = "(duDukeID="+duDukeId+")";
    NamingEnumeration<SearchResult> results = doSearch(filter);
    if (results.hasMoreElements()) {
      try {
        return (SearchResult) results.next();
      } catch (NamingException e) {
        logger.error("Failed searching for "+duDukeId+": " + e.getMessage(), e);
      }
    }
    return null;
  }


  /**
   * Add a group to this member's isMemberOf attributes.
   * @param duDukeId
   * @param groupName
   * @return boolean success
   */
  public boolean addMemberToGroup(String duDukeId, String groupName) {

    SearchResult sr = findEntry(duDukeId);

    if (sr == null) {
      logger.error("Failed to find user in LDAP:"+duDukeId);
      return false;
    }
    try {
      String distName =sr.getNameInNamespace();
      Attributes ar = sr.getAttributes();

      Attributes modAttrs = new BasicAttributes();
      Attribute modAttr = new BasicAttribute("isMemberOf");
       if (groupName != null && !groupName.equals("")) {
          modAttr.add(groupName);
        }          
      modAttrs.put(modAttr);
        
      if (checkForCourseGroup(groupName)){
        // If this is a course group, add the eduCourseMember and eduCourseOffering attributes
        // [roleStr]@urn:mace:duke.edu:courses:[subject]:[catalog number],section=[section number],class=[class number],term=[term number]
        // roleStr = students=> Learner, instructors => Instructor, TAs=> TeachingAssistant
        // the eduCourseOffering attribute of a person's entry in LDAP will also be populated in the same format as eduCourseMember, but without the role.
        //      here's an example in production
        //      ismemberof: urn:mace:duke.edu:groups:siss:courses:TEST:102:01:1000:9999:students
        //      educoursemember: Learner@urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
        //      educourseoffering: urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
        
            
        boolean needIt = false;
        try {
          Attribute ne = ar.get("objectClass");
          if (ne == null || !ne.contains("eduCourse")){
            needIt = true;
          }
        } catch (Exception e){
          needIt = true;
        }
        if (needIt){
          Attribute oc = new BasicAttribute("objectClass");
          oc.add("eduCourse");
          modAttrs.put(oc);
        }
        Attribute member = new BasicAttribute("eduCourseMember");
        member.add(getCourseString(groupName, true));
        modAttrs.put(member);
        Attribute course = new BasicAttribute("eduCourseOffering");
        course.add(getCourseString(groupName, false));
        modAttrs.put(course);
      }
      
      if (checkForDCalGroup(groupName)){
        // If the group is duke:oit:csi:is:services:dcal -
        //    * make sure the objectClass duProvision exists in LDAP
        //    * add duEligible=oraclecalendar attribute.  Note that this is a multi-valued attribute.  
        boolean needIt = false;
        try {
          Attribute ne = ar.get("objectClass");
          if (ne == null || !ne.contains("duProvision")){
            needIt = true;
          }
        } catch (Exception e){
          needIt = true;
        }
        if (needIt){
          Attribute oc = new BasicAttribute("objectClass");
          oc.add("duProvision");
          modAttrs.put(oc);
        }
        
        Attribute eligible = new BasicAttribute("duEligible");
        eligible.add("oraclecalendar");
        modAttrs.put(eligible);
     }
        
      try {
        context.modifyAttributes(distName,DirContext.ADD_ATTRIBUTE, modAttrs);
      } catch (AttributeInUseException aiue) {
        logger.warn("Tried to add a group ("+groupName+") that already existed for "+duDukeId);
      }
    } catch (NamingException e) {
      logger.error("Failed while adding a group ("+groupName+") for "+duDukeId+": " + e.getMessage(), e);
      return false;
    }   
    return true;

  }

  private String getCourseString(String groupName,boolean plusRole) {
    Matcher matcher = pat.matcher(groupName);
    String retVal = "";
    if (matcher.find()){
      retVal = "urn:mace:duke.edu:courses:"+matcher.group(1)+
        ":"+matcher.group(2)+",section="+matcher.group(3)+",class="+matcher.group(4)+",term="+matcher.group(5);
      if (plusRole) {
        String role = "TeachingAssistant";
         if (matcher.group(6).equals("students")) {
          role = "Learner";
        }else if (matcher.group(6).equals("instructors")){
          role = "Instructor";
        }
        retVal = role + "@"+retVal;
      }
        
    }
    return retVal;
  }

  private boolean checkForCourseGroup(String groupName) {
    Matcher matcher = pat.matcher(groupName);
    boolean found = false;
    
    if (matcher.find()){
//      logger.info("I found the subject "+matcher.group(1)+
//          " the catalog number:" + matcher.group(2) +
//          " the section number:" + matcher.group(3) +
//          " the class number:" + matcher.group(4) +
//          " the term number:" + matcher.group(5) +
//          " and the type:" + matcher.group(6));

       found = true;

    }
    return found;
  }

  private boolean checkForDCalGroup(String groupName) {
    String dCalRegEx = "^urn:mace:duke.edu:groups:oit:csi:is:services:dcal$";
    return groupName.matches(dCalRegEx);
  }
  
  
  /**
   * rename a group - Basically this means find all members and change their membership
   * @param oldGroup
   * @param newGroup
   * @return Vector containing all the failed LDAPKeys.  
   */

  public Vector<String> renameGroup(String oldGroup, String newGroup) {
    Vector<String> fails = new Vector<String>();

    String filter = "(isMemberOf="+oldGroup+")";
    NamingEnumeration<SearchResult> results = doSearch(filter);
    while (results.hasMoreElements()) {
      SearchResult sr = null;
      try {
        sr = (SearchResult) results.next();
        String distName =sr.getNameInNamespace();

        try {
          // First, remove the oldGroup 
          ModificationItem[] rmGroup = new ModificationItem[1];
          rmGroup[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("isMemberOf", oldGroup));
          context.modifyAttributes(distName, rmGroup);
          logger.info("removed group "+oldGroup+" for user:"+distName);
          
          // Now add the newGroup
          ModificationItem[] addGroup = new ModificationItem[1];
          addGroup[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("isMemberOf", newGroup));
          context.modifyAttributes(distName, addGroup);         
          logger.info("added group "+newGroup+" for user:"+distName);
        } catch (NamingException e) {
          logger.error("For user:"+distName+" failed to rename group "+oldGroup+" to:"+newGroup+" - "+e.getMessage());
          fails.add(distName);
        }
      } catch (NamingException e) {
        // If we fail here it means we didn't know who the failure was actually for. Might need to fail the whole rename?
        logger.error("Failed to rename group "+oldGroup+" to:"+newGroup+" - "+e.getMessage());
        fails.add("some member");
      }

    }
    return fails;
  }

  /**
   * Remove a group from a user's ldap record
   * @param duDukeId
   * @param groupName - group to be removed
   * @return boolean - success
   */

  public boolean removeMemberFromGroup(String duDukeId, String groupName) {
    SearchResult sr = findEntry(duDukeId);

    if (sr == null) {
      logger.error("Failed to find user in LDAP:"+duDukeId);
      // Shilen mentioned that sometimes a user is removed from LDAP and then their Grouper information is updated.
      // This means that a Grouper removeMember could fire for someone who is not in LDAP.
      // This means that we could have reached this state in an error-free state.  Lets just leave quietly.
      return true; //no error
    }
    Vector<ModificationItem> mods = new Vector<ModificationItem>();
 
    try {
      Attributes ar = sr.getAttributes();
      String distName =sr.getNameInNamespace();
       
      if (checkForCourseGroup(groupName)){

//      here's an example in production
//      ismemberof: urn:mace:duke.edu:groups:siss:courses:TEST:102:01:1000:9999:students
//      educoursemember: Learner@urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
//      educourseoffering: urn:mace:duke.edu:courses:TEST:102,section=01,class=1000,term=9999
        
        // Check if they have the eduCourseMember attribute - only remove if it is there!
        Attribute eligible = ar.get("eduCourseMember");
        if (eligible != null && eligible.contains(getCourseString(groupName, true))){
          mods.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE,
              new BasicAttribute("eduCourseMember", getCourseString(groupName, true))));
        }
        // Check if they have the eduCourseOffering attribute:
        eligible = ar.get("eduCourseOffering");
        if (eligible != null && eligible.contains(getCourseString(groupName, false))){
          mods.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE,
              new BasicAttribute("eduCourseOffering", getCourseString(groupName, false))));
        }
      }
      
      if (checkForDCalGroup(groupName)){
        // If the group is duke:oit:csi:is:services:dcal -
        //    * remove duEligible=oraclecalendar from LDAP. 
        Attribute eligible = ar.get("duEligible");
        if (eligible != null && eligible.contains("oraclecalendar")){
          mods.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE,
              new BasicAttribute("duEligible", "oraclecalendar")));
        }
      }
      // Check if they have this course - If they don't don't bother removing it
      Attribute memberList = ar.get("isMemberOf");

      if(memberList != null &&memberList.contains(groupName)){
        mods.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE,
            new BasicAttribute("isMemberOf", groupName)));
      }else{
				logger.info("For some strange reason i can't find "+groupName+" in their memberList");
			}
      // Quit if there is nothing to do
      if (mods.isEmpty()){
      	logger.info("This person has nothing to remove:"+groupName);
        return true;
      }
      
      ModificationItem[] modAry = new ModificationItem[mods.size()];
      int count = 0;
      
      Enumeration<ModificationItem> e = mods.elements();
      
      while( e.hasMoreElements() ) {
          modAry[ count ] = (ModificationItem) e.nextElement();
          count += 1;
      }
      

      context.modifyAttributes(distName, modAry);

      //context.modifyAttributes(distName,DirContext.REMOVE_ATTRIBUTE, ar);
    } catch (NamingException e) {
      logger.error("Failed while removing a group ("+groupName+") for "+duDukeId+": " + e.getMessage(), e);
      return false;
    }
    return true;
  }

  private NamingEnumeration<SearchResult> doSearch(String filter) {

    String[] attrs = {"duLDAPKey","isMemberOf", "objectClass", "eduCourseMember", "eduCourseOffering","duEligible"};
    SearchControls ctls = new SearchControls();
    ctls. setReturningObjFlag (true);
    ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);  
    ctls.setReturningAttributes(attrs);

    NamingEnumeration<SearchResult> results = null;
    try {
      try {
        results = context.search(baseDn, filter, ctls);
      } catch (NamingException e) {
        // let's try reconnecting and then searching again.  if it still fails, we'll let the exception be thrown.
        reconnect();
        results = context.search(baseDn, filter, ctls);
      }
      return results;
    } catch (NamingException e) {
      throw new RuntimeException("Failed while querying LDAP: " + e.getMessage(), e);
    }
  }


  /**
   * Reconnect to ldap
   */
  private void reconnect() {
    try {
      context.close();
    } catch (NamingException e) {
      // this is okay
    }

    this.context = createConnection();
    logger.info(": Reconnected to LDAP.");
  }

  /**
   * Close ldap connection when this class is about to be garbage collected.
   */
  protected void finalize() throws Throwable {
    if (context != null) {
      try {
        context.close();
      } catch (NamingException e) {
        // this is okay
      }
    }
    super.finalize();
  }
}
