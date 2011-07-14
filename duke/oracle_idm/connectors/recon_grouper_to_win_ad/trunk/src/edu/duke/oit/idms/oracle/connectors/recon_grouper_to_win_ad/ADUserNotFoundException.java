package edu.duke.oit.idms.oracle.connectors.recon_grouper_to_win_ad;

/**
 * @author shilen
 */
public class ADUserNotFoundException extends Exception {


  /**
   * 
   */
  private static final long serialVersionUID = 577919876313805299L;

  /**
  *
  */
  public ADUserNotFoundException() {
    super();
  }

  /**
   * @param msg
   * @param cause
   */
  public ADUserNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * @param msg
   */
  public ADUserNotFoundException(String msg) {
    super(msg);
  }

  /**
   * @param cause
   */
  public ADUserNotFoundException(Throwable cause) {
    super(cause);
  }

}
