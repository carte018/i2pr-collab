package edu.duke.oit.idms.oracle.scheduled_file_feeds;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Iterator;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;

/**
 * This class is used to encrypt files using open pgp.
 * This class was essentially copied from the class files used by DirXML to encrypt files.
 * 
 * @author shilen
 */
public class OpenPGPFileEncryption {

  /**
   * 
   */
  public OpenPGPFileEncryption() {
    Security.addProvider(new BouncyCastleProvider());
  }

  /**
   * @param keyIn
   * @return public key
   * @throws FileNotFoundException
   * @throws IOException
   * @throws PGPException
   */
  private PGPPublicKey readPublicKey(String keyIn) throws FileNotFoundException,
      IOException, PGPException {
    
    InputStream in = PGPUtil.getDecoderStream(new FileInputStream(keyIn));
    PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(in);
    PGPPublicKey key = null;

    Iterator<?> rIt = pgpPub.getKeyRings();

    while (key == null && rIt.hasNext()) {
      PGPPublicKeyRing kRing = (PGPPublicKeyRing) rIt.next();
      Iterator<?> kIt = kRing.getPublicKeys();

      while (kIt.hasNext()) {
        PGPPublicKey k = (PGPPublicKey) kIt.next();
        if (k.isEncryptionKey()) {
          key = k;
          break;
        }
      }
    }

    if (key == null) {
      throw new RuntimeException("Can't find encryption key in key ring: " + keyIn);
    }

    return key;
  }

  /**
   * @param outFilename
   * @param inFilename
   * @param keyIn
   * @param armor
   * @param withIntegrityCheck
   * @throws IOException
   * @throws PGPException
   * @throws NoSuchProviderException
   */
  public void encryptFile(String outFilename, String inFilename, String keyIn,
      boolean armor, boolean withIntegrityCheck) throws IOException, PGPException,
      NoSuchProviderException {

    OutputStream out = new FileOutputStream(new File(outFilename));
    PGPPublicKey encKey = readPublicKey(keyIn);

    if (armor) {
      out = new ArmoredOutputStream(out);
    }

    ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(
        PGPCompressedData.ZIP);

    PGPUtil.writeFileToLiteralData(comData.open(bOut), PGPLiteralData.BINARY, new File(inFilename));

    comData.close();

    PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(PGPEncryptedData.CAST5,
        withIntegrityCheck, new SecureRandom(), "BC");

    cPk.addMethod(encKey);

    byte[] bytes = bOut.toByteArray();
    OutputStream cOut = cPk.open(out, bytes.length);

    cOut.write(bytes);
    cPk.close();
    out.close();
  }

}
