package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Pattern;

import edu.duke.oit.idms.oracle.util.AttributeData;
import edu.duke.oit.idms.oracle.util.OIMAPIWrapper;

/**
 * @author shilen
 */
public class SimpleFeed {

  private String feedName = null;
  private Properties commonProperties = null;
  private Properties feedProperties = null;
  
  private AttributeData attributeData = AttributeData.getInstance();

  /**
   * this is the delimeter used in the current and previous raw data files for multi-valued attributes.
   */
  public static String multiValueFileDelimiter = "|";
  
  /**
   * this is the delimeter used in the current and previous raw data files to separate fields.
   */
  public static String fieldFileDelimeter = "\t";

  
  /**
   * @param feedName
   * @param commonProperties
   */
  public SimpleFeed(String feedName, Properties commonProperties) {
    this.feedName = feedName;
    this.commonProperties = commonProperties;
  }
  
  /**
   * execute feed
   */
  public void execute() {
    
    // get properties
    this.feedProperties = getFeedProperties();

    // get current data
    LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data = this.getCurrentData(this.feedProperties);

    // transform current data
    this.transformCurrentData(this.feedProperties, data);
    
    String epoch = "" + System.currentTimeMillis();
    String outputFilename = this.feedProperties.getProperty("feed.filename").replaceFirst("<EPOCH>", epoch);

    if (this.feedProperties.getProperty("feed.type").equals("incremental")) {
      
      // write current data to file
      this.writeCurrentDataToFile(data);

      // get previous data
      LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> previousData = this.getPreviousDataFromFile();

      // diff current with previous to get incremental
      IncrementalData incrementalData = this.getIncrementalData(previousData, data);

      // output incremental feed to file
      this.outputFeedToFile(
          outputFilename,
          this.feedProperties.getProperty("feed.multiValueDelimeter"),
          this.feedProperties.getProperty("feed.fieldDelimeter"), 
          this.feedProperties.getProperty("feed.includeHeader").equals("1") ? true : false,
          this.feedProperties.getProperty("feed.includeTrailer").equals("1") ? true : false,
          this.feedProperties.getProperty("feed.includeAttributeList").equals("1") ? true : false,
          incrementalData);
      
      incrementalData = null;
      previousData = null;
      data = null;

    } else {
      
      // output full feed to file.
      this.outputFeedToFile(
          outputFilename, 
          this.feedProperties.getProperty("feed.multiValueDelimeter"),
          this.feedProperties.getProperty("feed.fieldDelimeter"), 
          this.feedProperties.getProperty("feed.includeHeader").equals("1") ? true : false,
          this.feedProperties.getProperty("feed.includeTrailer").equals("1") ? true : false,
          this.feedProperties.getProperty("feed.includeAttributeList").equals("1") ? true : false,
          data);
      
      data = null;
    }
    
    String finalFilename = new String(outputFilename);

    if (this.feedProperties.getProperty("feed.encrypt", "0").equals("1")) {
      String encryptedFilename = this.feedProperties.getProperty("feed.encrypt.filename").replaceFirst("<EPOCH>", epoch);
      String publicKeyFilename = this.feedProperties.getProperty("feed.encrypt.publicKey");
      boolean armor = false;
      
      if (this.feedProperties.getProperty("feed.encrypt.armor", "0").equals("1")) {
        armor = true;
      }
      
      encryptFile(outputFilename, encryptedFilename, publicKeyFilename, armor);
      finalFilename = new String(encryptedFilename);
    }
    
    if (this.feedProperties.getProperty("feed.zip", "0").equals("1")) {
      String preFinalFilename = new String(finalFilename);
      finalFilename = this.feedProperties.getProperty("feed.zip.filename").replaceFirst("<EPOCH>", epoch);
      this.zipFiles(finalFilename, preFinalFilename, this.feedProperties);
    }
    
    this.transferFile(finalFilename, this.feedProperties);
    
    this.renameCurrentDirectory();
  }
  

  /**
   * @return properties
   */
  public Properties getFeedProperties() {
    try {
      Properties props = new Properties();
      String propertiesFile = System.getenv("OIM_APP_HOME") + "/conf/" + feedName + ".conf";
      
      props.load(new FileInputStream(propertiesFile));
      return props;
    } catch (IOException e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while reading properties.", e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * rename current directory to previous and keep some backups
   */
  public void renameCurrentDirectory() {
    
    // rename previous.9 to previous.10, previous.8 to previous.9, ... , previous.1 to previous.2.
    for (int i = 9; i > 0; i--) {
      int next = i + 1;
      File previousDir = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/previous." + i);
      File newPreviousDir = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/previous." + next);
      if (previousDir.exists()) {
        if (!previousDir.renameTo(newPreviousDir)) {
          String comment = feedName + ": Failed to rename " + previousDir.getAbsolutePath() + " to " + newPreviousDir.getAbsolutePath();
          ScheduledFileFeeds.LOG.error(comment);
          throw new RuntimeException(comment);
        }
      }
    }
    
    // rename current to previous.1
    File currentDir = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current");
    File previousDir = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/previous.1");
    if (!currentDir.renameTo(previousDir)) {
      String comment = feedName + ": Failed to rename " + currentDir.getAbsolutePath() + " to " + previousDir.getAbsolutePath();
      ScheduledFileFeeds.LOG.error(comment);
      throw new RuntimeException(comment);
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished renaming directories for next run.");
  }
  
  /**
   * @param props
   * @return map
   */
  @SuppressWarnings("unchecked")
  public LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> getCurrentData(Properties props) {
    
    LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data = 
      new LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>>();
    
    String whereClause = props.getProperty("feed.sql.where");
    String attributesProperty = props.getProperty("feed.attributes") + ", " + props.getProperty("feed.logicOnlyAttributes");
    String[] attributes = attributesProperty.split(",\\s*");

    Set<String> attributesUsingOIMColumnNames = new LinkedHashSet<String>();
    
    for (int i = 0; i < attributes.length; i++) {
      if (attributes[i].equalsIgnoreCase("eduPersonAffiliation")) {
        // i'm adding to a tree set first to make sure the columns are always in the same order.
        attributesUsingOIMColumnNames.addAll(new TreeSet(OIMAPIWrapper.getOIMAffiliationFieldNames()));
      } else {
        attributesUsingOIMColumnNames.add(getOIMColumnName(attributeData.getOIMAttributeName(attributes[i])));
      }
    }

    String selectClause = "";
    
    Iterator<String> iter = attributesUsingOIMColumnNames.iterator();
    while (iter.hasNext()) {
      selectClause += iter.next();
      if (iter.hasNext()) {
        selectClause += ", ";
      }
    }
    
    String sql = "select " + selectClause + " from usr where " + whereClause + " order by usr_login";
    Connection conn = this.getConnection(commonProperties);
    ScheduledFileFeeds.LOG.info(feedName + ": Retrieved database connection.");
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = conn.prepareStatement(sql);
      rs = ps.executeQuery();
      int count = 0;
      
      ScheduledFileFeeds.LOG.info(feedName + ": Now retrieving entries from database.");

      while (rs.next()) {
        count++;
        String key = rs.getString("USR_LOGIN");
        data.put(key, new LinkedHashMap<String, LinkedList<String>>());
        
        iter = attributesUsingOIMColumnNames.iterator();
        while (iter.hasNext()) {
          String columnName = iter.next();
          String fieldName = getOIMFieldName(columnName);
          String attrValue = rs.getString(columnName);
          if (attrValue == null) {
            attrValue = "";
          }
          
          if (OIMAPIWrapper.isOIMAffiliationField(fieldName)) {
            if (data.get(key).get("eduPersonAffiliation".toLowerCase()) == null) {
              data.get(key).put("eduPersonAffiliation".toLowerCase(), new LinkedList<String>());
            }
            
            if (attrValue.equals("1")) {
              data.get(key).get("eduPersonAffiliation".toLowerCase()).add(OIMAPIWrapper.getAffiliationValueFromOIMFieldName(fieldName));
            }
          } else {
            String attrName = attributeData.getLDAPAttributeName(fieldName).toLowerCase();
            data.get(key).put(attrName, new LinkedList<String>());

            if (attributeData.isMultiValued(attrName)) {
              List<String> attrValues = OIMAPIWrapper.split(attrValue);
              Iterator<String> attrValuesIter = attrValues.iterator();
              while (attrValuesIter.hasNext()) {
                data.get(key).get(attrName).add(attrValuesIter.next());
              }
            } else {
              data.get(key).get(attrName).add(attrValue);              
            }
          }
        }
      }

      ScheduledFileFeeds.LOG.info(feedName + ": Finished getting " + count + " entries.");

    } catch (Exception e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while getting current data.", e);
      throw new RuntimeException(e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        
        if (ps != null) {
          ps.close();
        }
        
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e) {
        // just warn
        ScheduledFileFeeds.LOG.warn("Error while closing result set, prepared statement, or database connection.", e);
      }
    }
    
    return data;
  }
  
  /**
   * @param oimFieldName
   * @return oim column name
   */
  public String getOIMColumnName(String oimFieldName) {
    if (oimFieldName.startsWith("USR_UDF_")) {
      return oimFieldName;
    } else if (oimFieldName.equalsIgnoreCase("Users.User ID")) {
      return "USR_LOGIN";
    } else if (oimFieldName.equalsIgnoreCase("Users.Middle Name")) {
      return "USR_MIDDLE_NAME";
    } else if (oimFieldName.equalsIgnoreCase("Users.First Name")) {
      return "USR_FIRST_NAME";
    } else if (oimFieldName.equalsIgnoreCase("Users.Last Name")) {
      return "USR_LAST_NAME";
    } else if (oimFieldName.equalsIgnoreCase("Users.Email")) {
      return "USR_EMAIL";
    } else {
      String comment = feedName + ": Exception while getting OIM column name for field " + oimFieldName;
      ScheduledFileFeeds.LOG.error(comment);
      throw new RuntimeException(comment);
    }
  }
  
  /**
   * @param oimColumnName
   * @return oim field name
   */
  public String getOIMFieldName(String oimColumnName) {
    if (oimColumnName.startsWith("USR_UDF_")) {
      return oimColumnName;
    } else if (oimColumnName.equalsIgnoreCase("USR_LOGIN")) {
      return "Users.User ID";
    } else if (oimColumnName.equalsIgnoreCase("USR_MIDDLE_NAME")) {
      return "Users.Middle Name";
    } else if (oimColumnName.equalsIgnoreCase("USR_FIRST_NAME")) {
      return "Users.First Name";
    } else if (oimColumnName.equalsIgnoreCase("USR_LAST_NAME")) {
      return "Users.Last Name";
    } else if (oimColumnName.equalsIgnoreCase("USR_EMAIL")) {
      return "Users.Email";
    } else {
      String comment = feedName + ": Exception while getting OIM field name for column " + oimColumnName;
      ScheduledFileFeeds.LOG.error(comment);
      throw new RuntimeException(comment);
    }
  }
  
  /**
   * Get database connection
   * @param commonProperties
   * @return database connection
   */
  public Connection getConnection(Properties commonProperties) {
    
    try {
      Class.forName(commonProperties.getProperty("db.driver"));
  
      Properties props = new Properties();
      props.put("user", commonProperties.getProperty("db.username"));
      props.put("password", commonProperties.getProperty("db.password"));
      if (commonProperties.getProperty("db.props") != null && !commonProperties.getProperty("db.props").equals("")) {
        String[] additionalPropsArray = commonProperties.getProperty("db.props").split(",");
        for (int i = 0; i < additionalPropsArray.length; i++) {
          String[] keyValue = additionalPropsArray[i].split("=");
          props.setProperty(keyValue[0], keyValue[1]);
        }
      }
  
      Connection conn = DriverManager.getConnection(commonProperties.getProperty("db.url"), props);
      return conn;
    } catch (Exception e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while getting database connection.", e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Transform current data before saving to file.
   * This replaces tab characters with spaces since tabs are used as the delimiter in the file.  
   * Also, removes logic only attributes.
   * You can overwrite this...
   * @param props
   * @param data
   */
  public void transformCurrentData(Properties props, LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data) {

    String logicOnlyAttributesProperty = props.getProperty("feed.logicOnlyAttributes");
    String[] attributes = logicOnlyAttributesProperty.split(",\\s*");
    
    Iterator<String> entriesIter = data.keySet().iterator();
    while (entriesIter.hasNext()) {
      LinkedHashMap<String, LinkedList<String>> entry = data.get(entriesIter.next());
      
      // remove logic only attributes
      for (int i = 0; i < attributes.length; i++) {
        entry.remove(attributes[i].toLowerCase());
      }
      
      Iterator<String> attributesIter = entry.keySet().iterator();
      while (attributesIter.hasNext()) {
        String attribute = attributesIter.next();
        LinkedList<String> values = entry.get(attribute);
        LinkedList<String> newValues = new LinkedList<String>();
        Iterator<String> valuesIter = values.iterator();
        while (valuesIter.hasNext()) {
          String value = valuesIter.next();
          value = value.replaceAll(SimpleFeed.fieldFileDelimeter, " ");
          // RGC -- duplicate embedded double-quotes for the DHTS loader to process them
          if (value.contains("\"")) {
        	  value = value.replaceAll("\"","\"\"");
          }
          newValues.add(value);
        }
        
        entry.put(attribute, newValues);
      }
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished transforming data.");
  }
  
  /**
   * Write full raw data dump to file.  This assumes one entry per line with multi-valued attributes being pipe delimited.
   * And fields are delimited by tabs.  This is used for incremental feeds only.  You can extend this...
   * @param data
   */
  public void writeCurrentDataToFile(LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data) {
    
    File outputFile = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/fullRawData.dat");
    BufferedWriter bw = null;
    
    boolean doneWritingHeader = false;

    try {      
      bw = new BufferedWriter(new FileWriter(outputFile, false));
      
      Iterator<String> entriesIter = data.keySet().iterator();
      while (entriesIter.hasNext()) {
        LinkedHashMap<String, LinkedList<String>> entry = data.get(entriesIter.next());
        
        // write header line
        if (!doneWritingHeader) {
          Iterator<String> attributesIter = entry.keySet().iterator();
          while (attributesIter.hasNext()) {
            bw.write(attributesIter.next());
            if (attributesIter.hasNext()) {
              bw.write(SimpleFeed.fieldFileDelimeter);
            }
          }
          bw.newLine();
          doneWritingHeader = true;
        }
        
        Iterator<String> attributesIter = entry.keySet().iterator();
        while (attributesIter.hasNext()) {
          String attribute = attributesIter.next();
          LinkedList<String> values = entry.get(attribute);
          String allValues = "";
          Iterator<String> valuesIter = values.iterator();
          while (valuesIter.hasNext()) {
            String value = valuesIter.next();
            allValues += value;
            
            if (valuesIter.hasNext()) {
              allValues += SimpleFeed.multiValueFileDelimiter;
            }
          }
          
          bw.write(allValues);
          
          if (attributesIter.hasNext()) {
            bw.write(SimpleFeed.fieldFileDelimeter);
          }
        }
        
        bw.newLine();
      }
    } catch (IOException e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while writing to file " + outputFile.getAbsolutePath(), e);
      throw new RuntimeException(e);
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished writing full data dump to file for incremental feed.");
  }

  /**
   * Returns the previous data.  This assumes one entry per line with multi-valued attributes being pipe delimited.
   * And fields are delimited by tabs.  You can extend this...
   * @return previous data
   */
  public LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> getPreviousDataFromFile() {
    File inputFile = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/previous.1/fullRawData.dat");
    LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> previousData =
      new LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>>();
    
    if (!inputFile.exists()) {
      return previousData;
    }
    
    BufferedReader br = null;

    try {      
      br = new BufferedReader(new FileReader(inputFile));
      String header = br.readLine();
      String[] headerValues = header.split(Pattern.quote(SimpleFeed.fieldFileDelimeter));
      int keyPosition = -1;
      for (int i = 0; i < headerValues.length; i++) {
        if (headerValues[i].equalsIgnoreCase("duDukeID")) {
          keyPosition = i;
          break;
        }
      }
      
      if (keyPosition == -1) {
        throw new RuntimeException("Unable to find duDukeID in previous data header line: " + inputFile.getAbsolutePath());
      }
      
      String line;
      while ((line = br.readLine()) != null) {
        // we have -1 as the second argument to split() to make sure we don't discard trailing empty spaces.
        String[] entryValues = line.split(Pattern.quote(SimpleFeed.fieldFileDelimeter), -1);
        String key = entryValues[keyPosition];
        previousData.put(key, new LinkedHashMap<String, LinkedList<String>>());

        for (int i = 0; i < entryValues.length; i++) {
          previousData.get(key).put(headerValues[i], new LinkedList<String>());

          if (headerValues[i].equalsIgnoreCase("eduPersonAffiliation") || attributeData.isMultiValued(headerValues[i])) {
            String[] values = entryValues[i].split(Pattern.quote(SimpleFeed.multiValueFileDelimiter));
            for (int j = 0; j < values.length; j++) {
              previousData.get(key).get(headerValues[i]).add(values[j]);
            }
          } else {
            previousData.get(key).get(headerValues[i]).add(entryValues[i]);
          }
        }
      }
    } catch (IOException e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while reading previous file " + inputFile.getAbsolutePath(), e);
      throw new RuntimeException(e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }

    ScheduledFileFeeds.LOG.info(feedName + ": Finished getting previous data from file.");

    return previousData;
  }
  
  /**
   * @param previousData
   * @param currentData
   * @return incremental data
   */
  public IncrementalData getIncrementalData(LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> previousData,
      LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> currentData) {
    
    IncrementalData incrementalData = new IncrementalData();
    

    Iterator<String> currentDataIter = currentData.keySet().iterator();
    while (currentDataIter.hasNext()) {
      String dukeid = currentDataIter.next();
      LinkedHashMap<String, LinkedList<String>> currentEntry = currentData.get(dukeid);
      LinkedHashMap<String, LinkedList<String>> previousEntry = previousData.get(dukeid);
      
      // we have an add
      if (previousEntry == null) {
        incrementalData.addNewAdd(dukeid, currentEntry);
        continue;
      }
      
      // if the same attributes don't exist, we have a modify.
      if (!currentEntry.keySet().equals(previousEntry.keySet())) {
        incrementalData.addNewModify(dukeid, currentEntry);
        continue;
      }
      
      // now we have to go through all the attribute values.
      Iterator<String> attributes = currentEntry.keySet().iterator();
      while (attributes.hasNext()) {
        String attrName = attributes.next();
        List<String> currentValues = currentEntry.get(attrName);
        List<String> previousValues = previousEntry.get(attrName);
        
        // we have a modify
        if (!currentValues.equals(previousValues)) {
          incrementalData.addNewModify(dukeid, currentEntry);
          continue;
        }
      }
      
    }
    
    
    Iterator<String> previousDataIter = previousData.keySet().iterator();
    while (previousDataIter.hasNext()) {
      String dukeid = previousDataIter.next();
      LinkedHashMap<String, LinkedList<String>> previousEntry = previousData.get(dukeid);
      LinkedHashMap<String, LinkedList<String>> currentEntry = currentData.get(dukeid);
      
      // we have a delete
      if (currentEntry == null) {
        incrementalData.addNewDelete(dukeid, previousEntry);
      }
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished calculating differences.");

    return incrementalData;
  }
  

  
  /**
   * Output full feed to file.  You can extend this...
   * @param filename
   * @param multiValueOutputDelimeter 
   * @param fieldOutputDelimeter 
   * @param includeHeader
   * @param includeTrailer
   * @param includeAttributeList
   * @param data
   */
  public void outputFeedToFile(String filename, String multiValueOutputDelimeter, String fieldOutputDelimeter, 
      boolean includeHeader, boolean includeTrailer, boolean includeAttributeList, LinkedHashMap<String, 
      LinkedHashMap<String, LinkedList<String>>> data) {
    throw new RuntimeException("not implemented.");
  }

  /**
   * Output incremental feed to file.  You can extend this...
   * @param filename
   * @param multiValueOutputDelimeter 
   * @param fieldOutputDelimeter 
   * @param includeHeader
   * @param includeTrailer
   * @param includeAttributeList
   * @param incrementalData
   */
  public void outputFeedToFile(String filename, String multiValueOutputDelimeter, String fieldOutputDelimeter, 
      boolean includeHeader, boolean includeTrailer, boolean includeAttributeList, IncrementalData incrementalData) {
    LinkedList<LinkedHashMap<String, LinkedList<String>>> allIncrementalData = 
      new LinkedList<LinkedHashMap<String, LinkedList<String>>>();
    
    // add the adds to allIncrementalData with an action of A.
    Iterator<String> dukeidsIter = incrementalData.getAdds().keySet().iterator();
    while (dukeidsIter.hasNext()) {
      String dukeid = dukeidsIter.next();
      LinkedHashMap<String, LinkedList<String>> entry = new LinkedHashMap<String, LinkedList<String>>();
      LinkedList<String> actionValues = new LinkedList<String>();
      actionValues.add("A");
      entry.put("action", actionValues);
      entry.putAll(incrementalData.getAdds().get(dukeid));
      allIncrementalData.add(entry);
    }
    
    // add the modifies to allIncrementalData with an action of M.
    dukeidsIter = incrementalData.getModifies().keySet().iterator();
    while (dukeidsIter.hasNext()) {
      String dukeid = dukeidsIter.next();
      LinkedHashMap<String, LinkedList<String>> entry = new LinkedHashMap<String, LinkedList<String>>();
      LinkedList<String> actionValues = new LinkedList<String>();
      actionValues.add("M");
      entry.put("action", actionValues);
      entry.putAll(incrementalData.getModifies().get(dukeid));
      allIncrementalData.add(entry);
    }
    
    // add the deletes to allIncrementalData with an action of D.  Remove all data except duDukeID.
    dukeidsIter = incrementalData.getDeletes().keySet().iterator();
    while (dukeidsIter.hasNext()) {
      String dukeid = dukeidsIter.next();
      LinkedHashMap<String, LinkedList<String>> entry = new LinkedHashMap<String, LinkedList<String>>();
      LinkedList<String> actionValues = new LinkedList<String>();
      actionValues.add("D");
      entry.put("action", actionValues);

      Iterator<String> attributes = incrementalData.getDeletes().get(dukeid).keySet().iterator();
      while (attributes.hasNext()) {
        String attrName = attributes.next();
        if (attrName.equalsIgnoreCase("duDukeID")) {
          entry.put(attrName, incrementalData.getDeletes().get(dukeid).get(attrName));
        } else {
          entry.put(attrName, new LinkedList<String>());
        }
      }
      allIncrementalData.add(entry);
    }

    // now write to file
    File outputFile = new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + filename);
    BufferedWriter bw = null;
    
    boolean doneWritingHeader = false;

    try {      
      bw = new BufferedWriter(new FileWriter(outputFile, false));
      
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      formatter.setCalendar(cal);
      String timestamp = formatter.format(cal.getTime()) + "Z";
      
      if (includeHeader) {
        bw.write(timestamp);
        bw.newLine();
      }
      
      Iterator<LinkedHashMap<String, LinkedList<String>>> entriesIter = allIncrementalData.iterator();
      while (entriesIter.hasNext()) {
        LinkedHashMap<String, LinkedList<String>> entry = entriesIter.next();
        
        // write header line
        if (includeAttributeList && !doneWritingHeader) {
          Iterator<String> attributesIter = entry.keySet().iterator();
          while (attributesIter.hasNext()) {
            bw.write(attributesIter.next());
            if (attributesIter.hasNext()) {
              bw.write(fieldOutputDelimeter);
            }
          }
          bw.newLine();
          doneWritingHeader = true;
        }
        
        Iterator<String> attributesIter = entry.keySet().iterator();
        while (attributesIter.hasNext()) {
          String attribute = attributesIter.next();
          LinkedList<String> values = entry.get(attribute);
          String allValues = "";
          Iterator<String> valuesIter = values.iterator();
          while (valuesIter.hasNext()) {
            String value = valuesIter.next();
            allValues += value;
            
            if (valuesIter.hasNext()) {
              allValues += multiValueOutputDelimeter;
            }
          }
          
          // if the string contains the delimeter, then surround value with quotes.
          if (allValues.contains(fieldOutputDelimeter)) {
            allValues = "\"" + allValues + "\"";
          }
          
          bw.write(allValues);
          
          if (attributesIter.hasNext()) {
            bw.write(fieldOutputDelimeter);
          }
        }
        
        bw.newLine();
      }
      
      if (includeTrailer) {
        bw.write("__________Trailer_________");
        bw.newLine();
      }
    } catch (IOException e) {
      ScheduledFileFeeds.LOG.error(feedName + ": Exception while writing to file " + outputFile.getAbsolutePath(), e);
      throw new RuntimeException(e);
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished writing incremental feed to file.");
  }
  
  

  /**
   * encrypt file
   * @param inputFilename
   * @param outputFilename
   * @param publicKeyFilename
   * @param armor
   */
  public void encryptFile(String inputFilename, String outputFilename, String publicKeyFilename, boolean armor) {
    OpenPGPFileEncryption encryption = new OpenPGPFileEncryption();
   
    // add path
    inputFilename = System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + inputFilename;
    outputFilename = System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + outputFilename;
    
    try {
      encryption.encryptFile(outputFilename, inputFilename, publicKeyFilename, armor, true);
    } catch (Exception e) {
      String comment = feedName + ": Exception while encrypted file " + inputFilename;
      ScheduledFileFeeds.LOG.error(comment, e);
      throw new RuntimeException(e);
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished encrypting file.");
  }

  
  /**
   * Zip the primary feed file along with any additional files if specified in the configuration.
   * Additional files are specified in the configuration as feed.zip.include.x=/full/path/to/file
   * where x is an incrementing number starting at 1.
   * @param outputFile 
   * @param primaryInputFile
   * @param props
   */
  public void zipFiles(String outputFile, String primaryInputFile, Properties props) {
    outputFile = System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + outputFile;
    
    ArrayList<File> list = new ArrayList<File>();
    list.add(new File(System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + primaryInputFile));
    
    int count = 1;
    while (true) {
      String nextInputFile = props.getProperty("feed.zip.include." + count);
      if (nextInputFile == null || nextInputFile.equals("")) {
        break;
      }
      
      list.add(new File(nextInputFile));
      count++;
    }
    
    try {
      Zip.execute(list.toArray(new File[0]), outputFile);
    } catch (IOException e) {
      String comment = feedName + ": Exception while creating zip file " + outputFile;
      ScheduledFileFeeds.LOG.error(comment, e);
      throw new RuntimeException(e);
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished zipping files.");
  }
  

  /**
   * @param finalFilename
   * @param props
   */
  public void transferFile(String finalFilename, Properties props) {
    String host = props.getProperty("feed.transfer.host");
    String username = props.getProperty("feed.transfer.username");
    String password = props.getProperty("feed.transfer.password");
    int port = Integer.parseInt(props.getProperty("feed.transfer.port", "22"));
    String lfile = System.getenv("OIM_APP_HOME") + "/work/" + this.feedName + "/current/" + finalFilename;
    String rfile = props.getProperty("feed.transfer.destinationDirectory") + "/" + finalFilename;
    
    try {
      Scp.scpFile(host, username, password, port, lfile, rfile);
    } catch (Exception e) {
      String comment = feedName + ": Exception while transferring file " + finalFilename;
      ScheduledFileFeeds.LOG.error(comment, e);
      throw new RuntimeException(e);
    }
    
    ScheduledFileFeeds.LOG.info(feedName + ": Finished transferring file to " + host + ".");
  }
}
