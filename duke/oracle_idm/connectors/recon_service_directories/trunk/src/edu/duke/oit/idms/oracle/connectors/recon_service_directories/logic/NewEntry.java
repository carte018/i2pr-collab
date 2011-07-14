package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import java.security.SecureRandom;

import javax.naming.directory.DirContext;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;

import Thor.API.Operations.tcUserOperationsIntf;

import edu.duke.oit.idms.oracle.util.AttributeData;


/**
 * @author shilen
 *
 */
public class NewEntry extends LogicBase {
  
  private char[] passwordChars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                   'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                                   '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                                   '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_'};
  
  private AttributeData attributeData = AttributeData.getInstance();

  public void doSomething(Map<String, String> oimAttributes,
      Map<String, PersonRegistryAttribute> prAttributes, String attributeName, String[] values,
      int modificationType, tcUserOperationsIntf moUserUtility, DirContext context,
      PersonRegistryHelper personRegistryHelper, String dn) {
    
    oimAttributes.put(attributeData.getOIMAttributeName("sn"), "BOGUS");
    oimAttributes.put(attributeData.getOIMAttributeName("givenName"), "BOGUS");
    
    String entryType = null;
    if (dn.toLowerCase().endsWith("ou=people,dc=duke,dc=edu")) {
      entryType = "people";
    } else if (dn.toLowerCase().endsWith("ou=test,dc=duke,dc=edu")) {
      entryType = "test";
    } else if (dn.toLowerCase().endsWith("ou=accounts,dc=duke,dc=edu")) {
      entryType = "accounts";
    } else {
      throw new RuntimeException("Unknown entry type for dn " + dn);
    }
    
    oimAttributes.put("USR_UDF_ENTRYTYPE", entryType);

    oimAttributes.put("Users.Role", "Full-Time");
    oimAttributes.put("Organizations.Organization Name", "Xellerate Users");
    oimAttributes.put("Users.Xellerate Type", "End-User");
    oimAttributes.put("Organizations.Key", "1");
    try {
      oimAttributes.put("Users.Password", generatePassword());
    } catch (Exception e) {
      throw new RuntimeException("Error generating password", e);
    }
    
    // be sure affiliation values never get null!!!!
    if (!oimAttributes.containsKey("USR_UDF_IS_STAFF")) {
      oimAttributes.put("USR_UDF_IS_STAFF", "0");
    }
    
    if (!oimAttributes.containsKey("USR_UDF_IS_STUDENT")) {
      oimAttributes.put("USR_UDF_IS_STUDENT", "0");
    }
    
    if (!oimAttributes.containsKey("USR_UDF_IS_EMERITUS")) {
      oimAttributes.put("USR_UDF_IS_EMERITUS", "0");
    }
    
    if (!oimAttributes.containsKey("USR_UDF_IS_FACULTY")) {
      oimAttributes.put("USR_UDF_IS_FACULTY", "0");
    }
    
    if (!oimAttributes.containsKey("USR_UDF_IS_ALUMNI")) {
      oimAttributes.put("USR_UDF_IS_ALUMNI", "0");
    }
    
    if (!oimAttributes.containsKey("USR_UDF_IS_AFFILIATE")) {
      oimAttributes.put("USR_UDF_IS_AFFILIATE", "0");
    }
  }
  
  /**
   * Generate random password
   * @return random password
   * @throws Exception
   */
  public String generatePassword() throws Exception {
    return RandomStringUtils.random(60, 0, 73, false, false, passwordChars, SecureRandom.getInstance("SHA1PRNG"));
  }
}
