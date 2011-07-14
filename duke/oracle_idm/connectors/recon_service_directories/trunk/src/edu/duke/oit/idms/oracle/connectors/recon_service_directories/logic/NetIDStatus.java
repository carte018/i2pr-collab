package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.naming.directory.DirContext;

import org.apache.log4j.Logger;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Operations.tcUserOperationsIntf;

import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryAttribute;
import edu.duke.oit.idms.oracle.connectors.recon_service_directories.PersonRegistryHelper;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;

/**
 * 
 * @author Michael Meadows (mm310)
 *
 */

public class NetIDStatus extends LogicBase {
	
	private org.apache.log4j.Logger LOG = Logger.getLogger(NetIDStatus.class);
	
	public void doSomething(Map<String, String> oimAttributes,
			Map<String, PersonRegistryAttribute> prAttributes,
			String attributeName, String[] values, int modificationType,
			tcUserOperationsIntf moUserUtility, DirContext context,
			PersonRegistryHelper personRegistryHelper, String dn) {
		
		// Test for all exclusions, if found log and exit
		
		if (!attributeName.toLowerCase().equals("dunetidstatus")) {
			LOG.info("NetIDStatus class called with attribute - "+ attributeName + ", when dunetidstatus was expected.");
			return;
		}
		
		if (values.length > 1) {
			LOG.info("NetIDStatus class called with multivalued attribute, when singlevalued attribute was expected.");
			return;
		}
		
		String value = "";
		if (values.length > 0)
			value = values[0];

		if (!isNetIDStatusChanged (moUserUtility, dn, value)) {

			LOG.info("NetIDStatus class called with NetIDStatus replace value = current value = " + value + ". Nothing to do.");
			return;
		}
		// end exclusions
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String todayString = sdf.format(new Date());
		oimAttributes.put("USR_UDF_NETIDSTATUSDATE", todayString);
		
	}
	
	private boolean isNetIDStatusChanged (tcUserOperationsIntf moUserUtility, String dn, String newNetIDStatus) {

		String duLDAPKey = OIMAPIWrapper.getLDAPKeyFromDN(dn);
		Map<String, String> queryAttributes = new HashMap<String, String>();
		String [] resultsAttributes = {"USR_UDF_NETIDSTATUS"};

		queryAttributes.put("USR_UDF_LDAPKEY", duLDAPKey);
		String oldNetIDStatus = null;
			
		try {
			tcResultSet results = (moUserUtility.findUsersFiltered(queryAttributes, resultsAttributes));
			oldNetIDStatus = results.getStringValue("USR_UDF_NETIDSTATUS");	
		} catch (tcAPIException e) {
			LOG.info("tcAPIException: " + e.getMessage(), e);
			throw new RuntimeException(e);
		} catch (tcColumnNotFoundException e) {
			LOG.info("tcColumnNotFoundException: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}
		return (!oldNetIDStatus.equals(newNetIDStatus));
	}
}
