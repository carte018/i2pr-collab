package edu.duke.oit.idms.oracle.connectors.tasks_email.routing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.thortech.util.logging.Logger;
import com.thortech.xl.util.logging.LoggerModules;

import edu.duke.oit.idms.oracle.connectors.tasks_email.EmailUsers;

import Thor.API.tcResultSet;
import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcITResourceNotFoundException;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

public class RoutingFileLastUpdate {

	private static Logger logger = Logger.getLogger(LoggerModules.XL_SCHEDULER_TASKS);
	private Date currentStartDate = new Date();
	private Date lastSuccessDate = null;
	private int taskID;
	
	public RoutingFileLastUpdate(tcITResourceInstanceOperationsIntf moITResourceUtility) {
		try {
			getTaskEnd(moITResourceUtility);
		}  catch (SQLException e) {
			String msg = EmailUsers.addLogEntry(MailRouting.connectorName, "FAILED", "Could not retrieve last successful task start date from OIM.");
			logger.error(msg, e);
		} catch (tcAPIException e) {
			String msg = EmailUsers.addLogEntry(MailRouting.connectorName, "FAILED", "Could not retrieve database connection information from OIM.");
			logger.error(msg, e);
		} catch (Exception e) {
			String msg = EmailUsers.addLogEntry(MailRouting.connectorName, "FAILED", e.getMessage());
			logger.error(msg, e);
		}
	}

	private void getTaskEnd(tcITResourceInstanceOperationsIntf moITResourceUtility) throws tcAPIException, SQLException, tcColumnNotFoundException, tcITResourceNotFoundException, ClassNotFoundException {
		PreparedStatement select = null;
		ResultSet queryResults = null;
			Connection conn = getConnection(moITResourceUtility);
			select = conn.prepareStatement("SELECT TASK_ID, TASK_START FROM OIM_TASK_LOG WHERE TASK_NAME = '" + MailRouting.connectorName
					+ "' ORDER BY TASK_START DESC");
			queryResults = select.executeQuery();
			int count = 0;
			while (queryResults.next()) {
				++count;
				if (count == 1) {
					// exactly 1 entry, use it
					lastSuccessDate = queryResults.getTimestamp("TASK_START");
					taskID = queryResults.getInt("TASK_ID");
				} else {
					// if more than one entry, delete remaining
					queryResults.deleteRow();
				}
			}
			
			if (count == 0)
				taskID = -1;	// use this as sentinel to decide in insert is needed rather than update

			if (lastSuccessDate == null) {
				// no entry in table, use current date
				lastSuccessDate = new Date();
			}
			
			queryResults.close();
			select.close();
			conn.close();
	}
	
	public void updateTaskEnd(tcITResourceInstanceOperationsIntf moITResourceUtility) throws tcAPIException, SQLException, tcColumnNotFoundException, tcITResourceNotFoundException, ClassNotFoundException {
		PreparedStatement update = null;
			Connection conn = getConnection(moITResourceUtility);
			Timestamp sqlCurrentStartDate = new Timestamp(currentStartDate.getTime());
			Timestamp sqlCurrentDate = new Timestamp((new Date()).getTime());
			String sqlStmt = null;
			if (taskID == -1) {
				sqlStmt = "INSERT INTO OIM_TASK_LOG (TASK_ID, TASK_NAME, TASK_START, TASK_END) VALUES (SQ_OIM_TASK_LOG_TASK_ID.NEXTVAL, ?, ?, ?)";
				update = conn.prepareStatement(sqlStmt);
				update.setString(1, MailRouting.connectorName);
				update.setTimestamp(2, sqlCurrentStartDate);
				update.setTimestamp(3, sqlCurrentDate);
			} else {
				sqlStmt = "UPDATE OIM_TASK_LOG SET TASK_START = ?, TASK_END = ? WHERE TASK_ID = ?";
				update = conn.prepareStatement(sqlStmt);
				update.setTimestamp(1, sqlCurrentStartDate);
				update.setTimestamp(2, sqlCurrentDate);
				update.setInt(3, taskID);
			}
			update.executeUpdate();
			update.close();
			conn.close();
			logger.info(EmailUsers.addLogEntry(MailRouting.connectorName, "DEBUG", "sqlCurrentStartDate:" + sqlCurrentStartDate + ": sqlCurrentDate:" + sqlCurrentDate));
	}
	
	public String getLastTaskSuccessDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return sdf.format(lastSuccessDate);
	}
	
	private Connection getConnection(tcITResourceInstanceOperationsIntf moITResourceUtility)
				throws tcAPIException, tcColumnNotFoundException, tcITResourceNotFoundException, ClassNotFoundException, SQLException {
	    Map<String, String> parameters = new HashMap<String, String>();
	    Map<String, String> resourceMap = new HashMap<String, String>();
	    resourceMap.put("IT Resources.Name", "OIM_DATABASE");
	    tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
	    long resourceKey = moResultSet.getLongValue("IT Resources.Key");

	    moResultSet = null;
	    moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
	    for (int i = 0; i < moResultSet.getRowCount(); i++) {
	      moResultSet.goToRow(i);
	      String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
	      String value = moResultSet.getStringValue("IT Resources Type Parameter Value.Value");
	      parameters.put(name, value);
	    }
	    
	    logger.info(EmailUsers.addLogEntry(MailRouting.connectorName, "INFO", "Creating database connection"));

	    Class.forName((String)parameters.get("driver"));

	    Properties props = new Properties();
	    props.put("user", parameters.get("username"));
	    props.put("password", parameters.get("password"));
	    if (parameters.get("connectionProperties") != null && !parameters.get("connectionProperties").equals("")) {
	      String[] additionalPropsArray = ((String)parameters.get("connectionProperties")).split(",");
	      for (int i = 0; i < additionalPropsArray.length; i++) {
	        String[] keyValue = additionalPropsArray[i].split("=");
	        props.setProperty(keyValue[0], keyValue[1]);
	      }
	    }

	    Connection conn = DriverManager.getConnection((String)parameters.get("url"), props);
	    return conn;  
	}
}
