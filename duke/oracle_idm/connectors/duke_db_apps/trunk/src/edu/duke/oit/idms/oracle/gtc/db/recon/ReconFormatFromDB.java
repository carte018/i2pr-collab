package edu.duke.oit.idms.oracle.gtc.db.recon;

import java.util.ArrayList;

import com.thortech.xl.gc.exception.ProviderException;
import com.thortech.xl.gc.spi.ReconFormatProvider;
import com.thortech.xl.gc.vo.designtime.Attribute;
import com.thortech.xl.gc.vo.designtime.OIMSchema;
import com.thortech.xl.gc.vo.designtime.Provider;
import com.thortech.xl.gc.vo.designtime.TargetSchema;
import com.thortech.xl.gc.vo.runtime.OIMRecord;
import com.thortech.xl.gc.vo.runtime.TargetRecord;


/**
 * @author shilen
 *
 */
public class ReconFormatFromDB implements ReconFormatProvider {

  public void initialize(Provider providerData) throws ProviderException {
    // do nothing
  }

  public OIMSchema parseMetadata(TargetSchema targetSchema) throws ProviderException {
    OIMSchemaImpl schema = new OIMSchemaImpl();
    TargetIdentityMetadata sh = (TargetIdentityMetadata) targetSchema;
    ArrayList parentMetaData = sh.getParentFields();
    Attribute[] parMetaData = new Attribute[parentMetaData.size()];
    for (int i = 0; i < parentMetaData.size(); i++) {
      Attribute a = new Attribute();
      a.setStrName((String) parentMetaData.get(i));
      parMetaData[i] = a;
    }
    schema.setParentMetadata(parMetaData);

    return schema;
  }

  public OIMRecord[] parseRecords(TargetRecord[] targetRecord) throws ProviderException {
    OIMRecord[] oimRecs = new OIMRecord[targetRecord.length];
    for (int i = 0; i < targetRecord.length; i++) {
      OIMRecordImpl rec = new OIMRecordImpl();
      TargetIdentityRecord shRec = (TargetIdentityRecord) targetRecord[i];

      rec.setParentData(shRec.getParentData());
      oimRecs[i] = rec;
    }
    return oimRecs;
  }

}
