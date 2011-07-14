package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;


/**
 * This class is used to scp files using Jsch
 * This class was essentially copied from the class files used by DirXML to scp files.
 * 
 * @author shilen
 */
public class Scp {
  
  /**
   * @param host
   * @param username
   * @param password
   * @param port
   * @param lfile
   * @param rfile
   * @throws JSchException
   * @throws IOException
   */
  public static void scpFile(String host, String username, String password, int port,
      String lfile, String rfile) throws JSchException, IOException {
    JSch jsch = new JSch();
    Session session = jsch.getSession(username, host, port);
    
    if (password != null) {
      session.setPassword(password);
    }
    
    UserInfo ui= new JschUserInfo();
    session.setUserInfo(ui);
    session.connect();

    // exec 'scp -t rfile' remotely
    String command="scp -p -t "+ rfile;
    Channel channel = session.openChannel("exec");
    ((ChannelExec)channel).setCommand(command);

    // get I/O streams for remote scp
    OutputStream out = channel.getOutputStream();
    InputStream in = channel.getInputStream();

    channel.connect();

    if(checkAck(in) != 0){
      throw new RuntimeException("Non-zero return from checkAck");
    }

    // send "C0644 filesize filename", where filename should not include '/'
    int filesize = (int)(new File(lfile).length());
    command = "C0644 " + filesize + " ";
    if (lfile.lastIndexOf('/') > 0) {
      command += lfile.substring(lfile.lastIndexOf('/') + 1);
    } else{
      command += lfile;
    }
    
    command += "\n";
    out.write(command.getBytes());
    out.flush();

    if (checkAck(in) != 0){
      throw new RuntimeException("Non-zero return from checkAck");
    }

    // send a content of lfile
    FileInputStream fis = new FileInputStream(lfile);
    byte[] buf = new byte[1024];
    while (true) {
      int len = fis.read(buf, 0, buf.length);
      if(len <= 0) {
        break;
      }
      
      out.write(buf, 0, len);
      out.flush();
    }

    // send '\0'
    buf[0] = 0;
    out.write(buf, 0, 1);
    out.flush();

    if (checkAck(in) != 0){
      throw new RuntimeException("Non-zero return from checkAck");
    }
    
    try {
      session.disconnect();
    } catch (Exception e) {
      // just log a warn
      ScheduledFileFeeds.LOG.warn("Error while disconnecting from " + host, e);
    }
  }
  
  private static int checkAck(InputStream in) throws IOException {
    int b=in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if(b==0) return b;
    if(b==-1) return b;

    if(b==1 || b==2){
      StringBuffer sb=new StringBuffer();
      int c;
      do {
                c=in.read();
                sb.append((char)c);
      }
      while(c!='\n');
      if(b==1){ // error
                System.out.print(sb.toString());
      }
      if(b==2){ // fatal error
                System.out.print(sb.toString());
      }
    }
    return b;
  }


  /**
   * @author shilen
   */
  public static class JschUserInfo implements UserInfo {

    private String password;
    private String passPhrase;

    public String getPassword() {
      return password;
    }

    public String getPassphrase() {
      return passPhrase;
    }

    public boolean promptPassword(String message) {
      return true;
    }

    public boolean promptPassphrase(String message) {
      return true;
    }

    public boolean promptYesNo(String message) {
      return true;
    }

    public void showMessage(String message) {
      // do nothing
    }
  }

}
