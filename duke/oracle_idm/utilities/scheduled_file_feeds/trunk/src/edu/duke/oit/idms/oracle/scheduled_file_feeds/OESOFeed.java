package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Properties;

/**
 * @author shilen
 */
public class OESOFeed extends SimpleFeed {

  /**
   * @param feedName
   * @param commonProperties
   */
  public OESOFeed(String feedName, Properties commonProperties) {
    super(feedName, commonProperties);
  }
  
  /**
   * 
   * @see edu.duke.oit.idms.oracle.scheduled_file_feeds.SimpleFeed#transformCurrentData(java.util.LinkedHashMap)
   */
  public void transformCurrentData(Properties props, LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> data) {

    // perform common transformations
    super.transformCurrentData(props, data);
  }

}
