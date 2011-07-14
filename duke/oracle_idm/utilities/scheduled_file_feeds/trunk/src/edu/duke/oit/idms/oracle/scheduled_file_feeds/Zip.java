package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class is used to zip files.
 * This class was essentially copied from the class files used by DirXML to zip files.
 * @author shilen
 */
public class Zip {
  
  /**
   * @param files
   * @param outputFile
   * @throws IOException 
   */
  public static void execute(File[] files, String outputFile) throws IOException {
      
    int bufferSize = getFileSize(files);
    
    ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
    out.setLevel(Deflater.DEFAULT_COMPRESSION);
    byte data[] = new byte[bufferSize];

    for (int i = 0; i < files.length; i++) {
      FileInputStream fi = new FileInputStream(files[i]);
      BufferedInputStream origin = new BufferedInputStream(fi, bufferSize);
      ZipEntry entry = new ZipEntry(files[i].getName());
      out.putNextEntry(entry);
      int count;
      while ((count = origin.read(data, 0, bufferSize)) != -1) {
        out.write(data, 0, count);
      }
      origin.close();
    }
    out.close();
  }

  /**
   * @param files
   * @return file size
   */
  private static int getFileSize(File[] files) {
    int fileSize = 0;
    for (int index = 0; index < files.length; index++) {
      fileSize += files[index].length();
    }

    return fileSize;
  }
}
