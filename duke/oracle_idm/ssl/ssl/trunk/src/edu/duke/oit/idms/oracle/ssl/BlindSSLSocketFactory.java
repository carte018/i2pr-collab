package edu.duke.oit.idms.oracle.ssl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;


import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


public class BlindSSLSocketFactory extends SocketFactory {
	   private static SocketFactory blindFactory = null;

	   static {
	       TrustManager[] blindTrustMan = new TrustManager[] { new X509TrustManager() {
	           public X509Certificate[] getAcceptedIssuers() { return null; }
	           public void checkClientTrusted(X509Certificate[] c, String a) { }
	           public void checkServerTrusted(X509Certificate[] c, String a) { }
	       } };

	       try {
	           SSLContext sc = SSLContext.getInstance("SSL");
	           sc.init(null, blindTrustMan, new java.security.SecureRandom());
	           blindFactory = sc.getSocketFactory();
	       } catch (GeneralSecurityException e) {
	           // handle this error??
	       }
	   }

	   /**
	    * @see javax.net.SocketFactory#getDefault()
	    */
	   public static SocketFactory getDefault() {
	       return new BlindSSLSocketFactory();
	   }


	   /**
	    * @see javax.net.SocketFactory#createSocket(java.lang.String, int)
	    */
	   public Socket createSocket(String arg0, int arg1) throws IOException,
	           UnknownHostException {
	       return blindFactory.createSocket(arg0, arg1);
	   }

	   /**
	    * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int)
	    */
	   public Socket createSocket(InetAddress arg0, int arg1) throws IOException {
	       return blindFactory.createSocket(arg0, arg1);
	   }

	   /**
	    * @see javax.net.SocketFactory#createSocket(java.lang.String, int,
	    *      java.net.InetAddress, int)
	    */
	   public Socket createSocket(String arg0, int arg1, InetAddress arg2, int arg3)
	           throws IOException, UnknownHostException {
	       return blindFactory.createSocket(arg0, arg1, arg2, arg3);
	   }

	   /**
	    * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int,
	    *      java.net.InetAddress, int)
	    */
	   public Socket createSocket(InetAddress arg0, int arg1, InetAddress arg2,
	           int arg3) throws IOException {
	       return blindFactory.createSocket(arg0, arg1, arg2, arg3);
	   }

	}

