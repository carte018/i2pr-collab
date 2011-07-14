package edu.duke.oit.idms.oracle.connectors.tasks_email.revocation;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchResult;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.util.logging.Logger;
import com.thortech.xl.scheduler.tasks.SchedulerBaseTask;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;

public class DeleteCommsRemovedStatus extends SchedulerBaseTask  {

	private String connectorName = "DELETE_COMMS_REMOVED_STATUS";
	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);

	@SuppressWarnings("unused")
  @Override
	protected void execute() {
		logger.info(EmailUsers.addLogEntry(connectorName, "BEGIN", "Starting deletion of COMMSDIR entries (mailUserStatus=removed)."));
		tcITResourceInstanceOperationsIntf moITResourceUtility = null;
		try {
			moITResourceUtility = (tcITResourceInstanceOperationsIntf) super.getUtility("Thor.API.Operations.tcITResourceInstanceOperationsIntf");
		} catch (tcAPIException e) {
			logger.error(EmailUsers.addLogEntry(connectorName, "FAILED", "Couldn't get tcITResourceInstanceOperationsIntf from OIM."), e);
			if (moITResourceUtility != null) moITResourceUtility.close();

		}

		LDAPConnectionWrapper ldapConnectionWrapper = LDAPConnectionWrapper.getInstance(connectorName, moITResourceUtility);
		NamingEnumeration<SearchResult> results = ldapConnectionWrapper.findEntriesRemovedStatus(connectorName);

		int count = 0;
		while (results.hasMoreElements()) {
			++count;
			String dn = null;
			try {
				dn = results.next().getNameInNamespace();
				ldapConnectionWrapper.deleteEntry(connectorName, dn);
				logger.info(EmailUsers.addLogEntry(connectorName, "INFO", "Deleted entry: " + dn));
			} catch (NamingException e) {
				logger.info(EmailUsers.addLogEntry(connectorName, "WARNING", "Error removing COMMSDIR entry: " + dn), e);
			}
		}
		logger.info(EmailUsers.addLogEntry(connectorName, "SUCCESS", "Completed deletion of a total of " + count + " COMMSDIR entries (mailUserStatus=removed)"));

	}

}
