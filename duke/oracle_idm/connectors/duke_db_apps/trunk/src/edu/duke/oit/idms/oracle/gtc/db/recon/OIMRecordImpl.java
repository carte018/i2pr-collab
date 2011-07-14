package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.thortech.xl.gc.vo.runtime.OIMRecord;


/**
 * @author shilen
 *
 */
public class OIMRecordImpl implements OIMRecord {

  private Map parentData;

  private Hashtable childData;

  public Map getParentData() {
    return parentData;
  }

  public Collection getChildDataNames() {
    
    Collection childDataSet = new ArrayList();
    /*
    if (childData != null) {
      for (Enumeration e = childData.keys(); e.hasMoreElements();) {
        childDataSet.add((String) e.nextElement());
      }
    }
    */
    return childDataSet;
  }

  public List getChildDataSet(String childDataSetName) {
    /*
    if (childData != null) {
      return (ArrayList) childData.get(childDataSetName);
    } else {
      return null;
    }*/
    
    return null;
  }

  public void setChildData(Hashtable childData) {
    this.childData = childData;
  }

  public void setParentData(Map parentData) {
    this.parentData = parentData;
  }
}
