package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.util.ArrayList;
import java.util.Hashtable;

import com.thortech.xl.gc.vo.designtime.TargetSchema;

/**
 * @author shilen
 *
 */
public class TargetIdentityMetadata implements TargetSchema {

  private ArrayList parentFields;
  
  /**
   * create new instance
   */
  public TargetIdentityMetadata() {
    super();
  }

  /**
   * @return hashtable
   */
  public Hashtable getChildMetadata() {
    // return childMetadata;
    return null;
  }

  /**
   * @param childMetadata
   */
  public void setChildMetadata(Hashtable childMetadata) {
    // do nothing
  }

  /**
   * 
   * @return arraylist
   */
  public ArrayList getParentFields() {
    return parentFields;
  }

  /**
   * @param parentFields
   */
  public void setParentFields(ArrayList parentFields) {
    this.parentFields = parentFields;
  }
}
