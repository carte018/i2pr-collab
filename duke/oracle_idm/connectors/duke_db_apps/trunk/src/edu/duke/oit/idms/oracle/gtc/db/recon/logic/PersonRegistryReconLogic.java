package edu.duke.oit.idms.oracle.gtc.db.recon.logic;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import com.thortech.xl.gc.vo.designtime.Provider;

import edu.duke.oit.idms.oracle.gtc.db.recon.TargetIdentityRecord;

/**
 * @author shilen
 *
 */
public class PersonRegistryReconLogic extends LogicBase {

  public TargetIdentityRecord doSomething(TargetIdentityRecord targetRecord, Connection conn) {
    
    String[] privacyAttributesNeedValueChange = { "DUDUKEMAILADDRESSPRIVACY",
        "DUDUKEPHYSICALADDRESSPRIVACY", "DUENTRYPRIVACY", "DUHOMEADDRESSPRIVACY",
        "DUHOMEPHONEPRIVACY", "DUMAILPRIVACY", "DUTELEPHONE1PRIVACY",
        "DUTELEPHONE2PRIVACY" };
    
    Hashtable parentData = targetRecord.getParentData();
    
    // last name can never be null in OIM
    if (parentData.get("SN") == null || parentData.get("SN").equals("")) {
      parentData.put("SN", "BOGUS");
    }

    // first name can never be null in OIM
    if (parentData.get("GIVENNAME") == null || parentData.get("GIVENNAME").equals("")) {
      parentData.put("GIVENNAME", "BOGUS");
    }
    
    // for privacy attributes, we need to convert values from "private" -> "N" and "public" -> "Y"
    for (int i = 0; i < privacyAttributesNeedValueChange.length; i++) {
      String attrName = privacyAttributesNeedValueChange[i];
      String value = (String)parentData.get(attrName);

      if (value != null && !value.equals("")) {
        if (value.equals("private")) {
          parentData.put(attrName, "N");
        } else if (value.equals("public")) {
          parentData.put(attrName, "Y");
        }
      }
    }
    
    targetRecord.setParentData(parentData);
    
    return targetRecord;
  }

  public void doSomething(Connection conn, Provider providerData) throws SQLException {
    // Here we'll check to see if duBlockMail or duEmailChangeNotifyReq are changing
    // and if so update LDAP.  Note that duBlockMail transforms from a date to 2 values:
    // 1.  internet   2.  internet|<Date>  For instance internet|20090506.
    
    // And if there's an event to remove either attribute, the old value will be marked as 'foo'.
    
    String selectSQL = "select record_id, table_key, column_name, old_value, new_value from pr_event_log where " +
      "column_name = 'duEmailChangeNotifyReq' or column_name = 'duBlockMail' order by record_id";
    
    String deleteSQL = "delete from pr_event_log where record_id = ?";
    
    PreparedStatement ps1 = null;
    PreparedStatement ps2 = null;
    ResultSet rs = null;
    LdapContext context = null;
    try {
      ps1 = conn.prepareStatement(selectSQL);
      ps2 = conn.prepareStatement(deleteSQL);

      rs = ps1.executeQuery();
      while (rs.next()) {
        long recordId = new Long(rs.getLong(1)).longValue();
        long prid = Long.parseLong(rs.getString(2).replaceAll("PRID=", ""));
        String column = rs.getString(3);
        String oldValue = rs.getString(4);
        String newValue = rs.getString(5);
        String dukeid = getDukeIDFromPRID(conn, prid);
        
        // now let's update ldap
        if (context == null) {
          context = connectLDAP(providerData);
        }
        updateLDAP(context, dukeid, column, oldValue, newValue);
        
        // if the update to ldap succeeded, let's remove the event log entry
        ps2.setLong(1, recordId);
        ps2.executeUpdate();
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      if (ps1 != null) {
        try {
          ps1.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
       
      if (ps2 != null) {
        try {
          ps2.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (rs != null) {   
        try {
          rs.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (context != null) {
        try {
          context.close();
        } catch (NamingException e) {
          // this is okay
        }
      }
    }
  }
  
  protected LdapContext connectLDAP(Provider providerData) {
    String customData = (String)providerData.getRuntimeParams().get("customDataForLogicClass1");
    String password = (String)providerData.getRuntimeParams().get("customDataForLogicClass2");
    
    // the custom data is in the format:  ldapProvider,binddin,password
    String[] values = customData.split(",");
    
    Hashtable environment = new Hashtable();
    environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    environment.put(Context.PROVIDER_URL, values[0]);
    environment.put(Context.SECURITY_AUTHENTICATION, "simple");
    environment.put(Context.SECURITY_PRINCIPAL, values[1]);
    environment.put(Context.SECURITY_CREDENTIALS, password);
    environment.put(Context.SECURITY_PROTOCOL, "ssl");
    
    LdapContext context = null;
    try {
      context = new InitialLdapContext(environment, null);
      return context;
    } catch (NamingException e) {
     throw new RuntimeException(e); 
    }
  }
  
  protected void updateLDAP(LdapContext context, String dukeid, String column, String oldValue, String newValue) {
    String attrs[] = { "duBlockMail", "duEmailChangeNotifyReq" };

    SearchControls cons = new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0,
        attrs, false, false);
    
    Attributes modAttrs = new BasicAttributes();
    if (column.equals("duEmailChangeNotifyReq")) {
      Attribute modAttr = new BasicAttribute(column);

      if (newValue != null && !newValue.equals("")) {
        modAttr.add(newValue);
      }
      
      modAttrs.put(modAttr);
    } else if (column.equals("duBlockMail")) {
      Attribute modAttr = new BasicAttribute(column);

      if (newValue != null && !newValue.equals("")) {
        modAttr.add("internet|" + newValue);
        modAttr.add("internet");
      }
      
      modAttrs.put(modAttr);
    } else {
      throw new RuntimeException("Unexepcted attribute in updateLDAP(): " + column);
    }
    
    String filter = "(dudukeid=" + dukeid + ")";
    try {
      NamingEnumeration results = context.search("dc=duke,dc=edu",
          filter, cons);
      if (results.hasMoreElements()) {
        SearchResult entry = (SearchResult)results.next();
        
        context.modifyAttributes(entry.getName() + ",dc=duke,dc=edu", LdapContext.REPLACE_ATTRIBUTE, modAttrs);
      } else {
        throw new RuntimeException("Unable to find dukeid " + dukeid + " in LDAP.");
      }
    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getDukeIDFromPRID(Connection conn, long prid) throws SQLException {
    String dukeidSQL = "select pr_update.getURNStringValue('urn:mace:duke.edu:idms:dukeid', ?) as dukeid from dual";
    PreparedStatement ps1 = null;
    ResultSet rs = null;
    
    try {
      ps1 = conn.prepareStatement(dukeidSQL);
      ps1.setLong(1, prid);
      rs = ps1.executeQuery();

      if (rs.next()) {
        String dukeid = rs.getString(1);
        return dukeid;
      } else {
        throw new RuntimeException("Unexpected error.  Unable to get DukeID from PRID=" + prid);
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      if (ps1 != null) {
        try {
          ps1.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
      
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // this is okay
        }
      }
    }
  }
}
