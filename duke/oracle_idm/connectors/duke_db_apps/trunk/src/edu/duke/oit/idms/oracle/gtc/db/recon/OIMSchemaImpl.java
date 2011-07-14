package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.util.Hashtable;

import com.thortech.xl.gc.vo.designtime.Attribute;
import com.thortech.xl.gc.vo.designtime.OIMSchema;


/**
 * @author shilen
 *
 */
public class OIMSchemaImpl implements OIMSchema {

  private Attribute[] parentMetadata;

  public Attribute[] getChildMetadata(String childDataSetName) {
    /*
    ArrayList value = (ArrayList) childMetadata.get(childDataSetName);
    Attribute[] metaDataCh = new Attribute[value.size()];
    for (int i = 0; i < value.size(); i++) {
      Attribute a = new Attribute();
      a.setStrName((String) value.get(i));
      metaDataCh[i] = a;
    }
    return metaDataCh;
    */
    return null;
  }

  public Attribute[] getParentMetadata() {
    return parentMetadata;
  }

  public String[] getPossibleChildren() {
    /*
    ArrayList temp = new ArrayList();
    if (childMetadata != null) {
      for (Enumeration e = childMetadata.keys(); e.hasMoreElements();) {
        temp.add((String) e.nextElement());
      }
      String[] childDataSet = new String[temp.size()];
      for (int i = 0; i < temp.size(); i++) {
        childDataSet[i] = (String) temp.get(i);
      }
      return childDataSet;
    } else {
      return null;
    }
    */
    return null;
  }

  public void setParentMetadata(Attribute[] parentMetadata) {
    this.parentMetadata = parentMetadata;
  }

  public void setChildMetadata(Hashtable childMetadata) {
    // do nothing
  }

}
