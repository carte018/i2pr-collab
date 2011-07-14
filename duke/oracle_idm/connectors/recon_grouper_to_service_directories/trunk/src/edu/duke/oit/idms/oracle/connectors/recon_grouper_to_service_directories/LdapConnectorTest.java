package edu.duke.oit.idms.oracle.connectors.recon_grouper_to_service_directories;

import java.util.regex.Matcher;
import java.util.regex.Pattern;





public class LdapConnectorTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    
// //   duke:siss:courses:[subject]:[catalog number]:[section number]:[class number]:[term number]:students
//    String myStr = "duke:siss:courses:AAAS:101:01:2599:1285:students";
//    //String myStr = "test:course_1";
//    myStr = myStr.replaceFirst("duke", "urn:mace:duke.edu:groups");
//    System.out.println("now myStr is:"+myStr);
//    String regex = "^urn:mace:duke.edu:groups:siss:courses:(\\w+):(\\d+):(\\d+):(\\d+):(\\d+):(\\w+)$";
//    Pattern pat = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
//    Matcher matcher = pat.matcher(myStr);
//    boolean found = false;
//    
//    if (matcher.find()){
//      System.out.println("I found the subject "+matcher.group(1)+
//          " the catalog number:" + matcher.group(2) +
//          " the section number:" + matcher.group(3) +
//          " the class number:" + matcher.group(4) +
//          " the term number:" + matcher.group(5) +
//          " and the type:" + matcher.group(6));
//
//       found = true;
//
//    }

    //we will iterate across all the group adds and removes.  Be sure to make a failure list to track users we should
    // not work on anymore because they had some sort of error.
    LDAPConnectionWrapper ldapConnectionWrapper = new LDAPConnectionWrapper(true);

//    boolean success = ldapConnectionWrapper.addMemberToGroup("0421185","test:group_4");
//    System.out.println("addGroup returned"+success);
//    success = ldapConnectionWrapper.addMemberToGroup("0421185","test:group_5");
//    System.out.println("addGroup returned"+success);
//    boolean success = ldapConnectionWrapper.addMemberToGroup("0421185","urn:mace:duke.edu:groups:oit:csi:is:services:dcal");
//    System.out.println("addGroup returned"+success);

//    boolean success = ldapConnectionWrapper.addMemberToGroup("0421185","urn:mace:duke.edu:groups:siss:courses:TEST:102:01:1000:9999:students");
//    System.out.println("addGroup returned"+success);
    
//    boolean success = ldapConnectionWrapper.addMemberToGroup("0421185","urn:mace:duke.edu:groups:siss:courses:ACCOUNTG:240:101:16615:1260:students");
//    System.out.println("addGroup returned"+success);
 
//    Vector failedList;
//    failedList = ldapConnectionWrapper.renameGroup("test:group_5","test:another_newname_group_5");
//    System.out.println("renameGroup returned:"+failedList.toString());
//    success = ldapConnectionWrapper.removeMemberFromGroup("0421185","test:newname_group_1");
  boolean success = ldapConnectionWrapper.removeMemberFromGroup("0421185","urn:mace:duke.edu:groups:siss:courses:TEST:102:01:1000:9999:students");
//    
//    boolean success = ldapConnectionWrapper.removeMemberFromGroup("0421185","urn:mace:duke.edu:groups:oit:csi:is:services:dcal");
//      //boolean success = ldapConnectionWrapper.removeMemberFromGroup("0421185","test:group_5");
    System.out.println("removeGroup returned:"+success);


  }

}
