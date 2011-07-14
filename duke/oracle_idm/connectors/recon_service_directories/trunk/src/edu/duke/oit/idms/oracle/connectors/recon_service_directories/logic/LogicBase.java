package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;


/**
 * @author shilen
 *
 */
public abstract class LogicBase implements Logic {
  
  /**
   * @param context
   * @param attributeName
   * @param dn
   * @return String
   */
  public String getAttribute(DirContext context, String attributeName, String dn) {
    String attrs[] = { attributeName };

    SearchControls cons = new SearchControls(SearchControls.OBJECT_SCOPE, 0, 0, attrs,
        false, false);

    try {
      NamingEnumeration<SearchResult> results = context.search(dn, "(objectclass=*)",
          cons);
      
      if (!results.hasMoreElements()) {
        // odd
        return null;
      }

      SearchResult entry = results.next();
      Attributes attributes = entry.getAttributes();
        
      if (attributes.get(attributeName) == null) {
        return null;
      } else {
        return (String)attributes.get(attributeName).get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
