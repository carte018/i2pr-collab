package edu.duke.oit.idms.oracle.exceptions;

/**
 * @author shilen
 */
public class OIMUserNotFoundException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 7853497346623970571L;

  /**
  *
  */
  public OIMUserNotFoundException() {
    super();
  }

  /**
   * @param msg
   * @param cause
   */
  public OIMUserNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * @param msg
   */
  public OIMUserNotFoundException(String msg) {
    super(msg);
  }

  /**
   * @param cause
   */
  public OIMUserNotFoundException(Throwable cause) {
    super(cause);
  }

}
