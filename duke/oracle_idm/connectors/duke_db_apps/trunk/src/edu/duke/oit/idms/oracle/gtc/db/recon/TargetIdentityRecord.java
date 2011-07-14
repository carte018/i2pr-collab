package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.util.Hashtable;

import com.thortech.xl.gc.vo.runtime.TargetRecord;

/**
 * @author shilen
 */
public class TargetIdentityRecord implements TargetRecord {

  private Hashtable parentData;

  /**
   * create new instance
   */
  public TargetIdentityRecord() {
    super();
  }

  /**
   * @return hashtable
   */
  public Hashtable getChildData() {
    //return childData;
    return null;
  }

  /**
   * @param childData
   */
  public void setChildData(Hashtable childData) {
    // do nothing
  }

  /**
   * @return hashtable
   */
  public Hashtable getParentData() {
    return parentData;
  }

  /**
   * @param parentData
   */
  public void setParentData(Hashtable parentData) {
    this.parentData = parentData;
  }
}
