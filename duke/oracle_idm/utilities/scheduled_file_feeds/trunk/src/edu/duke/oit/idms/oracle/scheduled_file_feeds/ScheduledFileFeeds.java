package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author shilen
 */
public class ScheduledFileFeeds {

  protected static org.apache.log4j.Logger LOG = Logger.getLogger(ScheduledFileFeeds.class);

  /**
   * @param args
   * @throws Exception 
   */
  public static void main (String args[]) {
    
    if (System.getenv("OIM_APP_HOME") == null || System.getenv("OIM_APP_HOME").equals("")) {
      System.out.println("OIM_APP_HOME is empty.");
      System.exit(1);
    }
    
    PropertyConfigurator.configure(System.getenv("OIM_APP_HOME") + "/conf/log4j.properties");
    if (args.length == 0) {
      System.out.println("Usage: java " + ScheduledFileFeeds.class.getName() + " <name of feed as referenced in the configuration file>");
      System.exit(1);
    }
    
    new ScheduledFileFeeds(args[0]);
  }
  
  /**
   * @param feedName
   */
  @SuppressWarnings("unchecked")
  public ScheduledFileFeeds(String feedName) {
      
    try {
      
      // make sure the "current" working directory exists without any files.
      File currentDir = new File(System.getenv("OIM_APP_HOME") + "/work/" + feedName + "/current");
      if (!currentDir.exists()) {
        if (!currentDir.mkdirs()) {
          String comment = feedName + ": Failed to create \"current\" directory.";
          LOG.error(comment);
          throw new RuntimeException(comment);
        }
      } else {
        File[] files = currentDir.listFiles();
        for (int i = 0; i < files.length; i++) {
          if (!files[i].delete()) {
            String comment = feedName + ": Failed to delete file " + files[i].getAbsolutePath();
            LOG.error(comment);
            throw new RuntimeException(comment);
          }
        }
      }
      
      // if previous.10 exists, delete it and all its contents
      File previous10Dir = new File(System.getenv("OIM_APP_HOME") + "/work/" + feedName + "/previous.10");
      if (previous10Dir.exists()) {
        File[] files = previous10Dir.listFiles();
        for (int i = 0; i < files.length; i++) {
          if (!files[i].delete()) {
            String comment = feedName + ": Failed to delete file " + files[i].getAbsolutePath();
            LOG.error(comment);
            throw new RuntimeException(comment);
          }
        }
        
        if (!previous10Dir.delete()) {
          String comment = feedName + ": Failed to delete file " + previous10Dir.getAbsolutePath();
          LOG.error(comment);
          throw new RuntimeException(comment);
        }
      }
      
      Properties props = new Properties();
      String propertiesFile = System.getenv("OIM_APP_HOME") + "/conf/scheduledFileFeeds.conf";
      
      props.load(new FileInputStream(propertiesFile));
      
      Class<SimpleFeed> feedClass = (Class<SimpleFeed>) Class.forName(props.getProperty(feedName + ".class"));
      Constructor constructor = feedClass.getConstructor(new Class[] { String.class, Properties.class });
      SimpleFeed feed = (SimpleFeed) constructor.newInstance(new Object[] { feedName, props });
      feed.execute();
      
    } catch (Exception e) {
      LOG.error(feedName + ": Received exception while running ScheduledFileFeeds.", e);
      throw new RuntimeException(e);
    }
  }
}
