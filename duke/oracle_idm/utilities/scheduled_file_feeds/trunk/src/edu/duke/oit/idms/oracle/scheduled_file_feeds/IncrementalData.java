package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.util.LinkedHashMap;
import java.util.LinkedList;


/**
 * @author shilen
 */
public class IncrementalData {
  private LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> adds = 
    new LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>>();
  
  private LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> deletes = 
    new LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>>();
  
  private LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> modifies = 
    new LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>>();
  
  /**
   * @param key
   * @param newAdd
   */
  public void addNewAdd(String key, LinkedHashMap<String, LinkedList<String>> newAdd) {
    adds.put(key, newAdd);
  }
  
  /**
   * @param key
   * @param newDelete
   */
  public void addNewDelete(String key, LinkedHashMap<String, LinkedList<String>> newDelete) {
    deletes.put(key, newDelete);
  }

  /**
   * @param key
   * @param newModify
   */
  public void addNewModify(String key, LinkedHashMap<String, LinkedList<String>> newModify) {
    modifies.put(key, newModify);
  }
  
  /**
   * @return adds
   */
  public LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> getAdds() {
    return this.adds;
  }
  
  /**
   * @return deletes
   */
  public LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> getDeletes() {
    return this.deletes;
  }
  
  /**
   * @return modifies
   */
  public LinkedHashMap<String, LinkedHashMap<String, LinkedList<String>>> getModifies() {
    return this.modifies;
  }
}
