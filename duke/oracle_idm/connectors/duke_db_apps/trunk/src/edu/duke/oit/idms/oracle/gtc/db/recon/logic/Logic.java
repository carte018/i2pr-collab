package edu.duke.oit.idms.oracle.gtc.db.recon.logic;

import java.sql.Connection;
import java.sql.SQLException;

import com.thortech.xl.gc.vo.designtime.Provider;

import edu.duke.oit.idms.oracle.gtc.db.recon.TargetIdentityRecord;

/**
 * @author shilen
 *
 */
public interface Logic {

  /**
   * Used to transform the data in the target record.
   * @param targetRecord
   * @param conn 
   * @return TargetRecord
   */
  public TargetIdentityRecord doSomething(TargetIdentityRecord targetRecord, Connection conn);
  
  /**
   * Used to perform some task that is unrelated to any individual change.
   * This is called before the target records are handed off for reconciliation.
   * @param conn
   * @param providerData 
   * @throws SQLException 
   */
  public void doSomething(Connection conn, Provider providerData) throws SQLException;
}
