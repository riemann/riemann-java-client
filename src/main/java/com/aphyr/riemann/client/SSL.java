package com.aphyr.riemann.client;

import java.io.*;
import java.net.InetSocketAddress;
import java.security.cert.*;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.*;
import java.security.UnrecoverableKeyException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Scanner;
import javax.net.ssl.*;
import javax.xml.bind.DatatypeConverter;

public class SSL {
  // You know, a mandatory password stored in memory so we can... encrypt...
  // data stored in memory.
  public static char[] keyStorePassword =
    "GheesBetDyPhuvwotNolofamLydMues9".toCharArray();
  
  // The X.509 certificate factory
  public static CertificateFactory X509CertFactory() throws
    CertificateException {
    return CertificateFactory.getInstance("X.509");
  }

  // An RSA key factory
  public static KeyFactory RSAKeyFactory() throws NoSuchAlgorithmException {
    return KeyFactory.getInstance("RSA");
  }

  // Parses a base64-encoded string to a byte array
  public static byte[] base64toBinary(final String string) {
    return DatatypeConverter.parseBase64Binary(string);
  }

  // Turns a filename into an inputstream
  public static FileInputStream inputStream(final String fileName) throws
    FileNotFoundException {
    return new FileInputStream(new File(fileName));
  }

  // Reads a filename into a string
  public static String slurp(final String file) throws FileNotFoundException {
    return new Scanner(new File(file)).useDelimiter("\\Z").next();
  }

  // Loads an X.509 certificate from a file.
  public static X509Certificate loadCertificate(final String file) throws
    IOException, CertificateException {
    FileInputStream stream = null;
    try {
      stream = inputStream(file);
      return (X509Certificate) X509CertFactory().generateCertificate(stream);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  // Loads a public key from a .crt file.
  public static PublicKey publicKey(final String file) throws IOException,
         CertificateException {
    return loadCertificate(file).getPublicKey();
  }

  // Loads a private key from a PKCS8 file.
  public static PrivateKey privateKey(final String file) throws
    FileNotFoundException, NoSuchAlgorithmException, InvalidKeySpecException {
    final Pattern p = Pattern.compile(
      "^-----BEGIN ?.*? PRIVATE KEY-----$(.+)^-----END ?.*? PRIVATE KEY-----$",
      Pattern.MULTILINE | Pattern.DOTALL);

    final Matcher matcher = p.matcher(slurp(file));
    matcher.find();
    return RSAKeyFactory().generatePrivate(
        new PKCS8EncodedKeySpec(
          base64toBinary(
            matcher.group(1))));
  }

  // Makes a KeyStore from a PKCS8 private key file, a public cert file, and the
  // signing CA certificate.
  public static KeyStore keyStore(final String keyFile,
                                  final String certFile,
                                  final String caCertFile) throws
    FileNotFoundException, IOException, KeyStoreException,
    NoSuchAlgorithmException, InvalidKeySpecException, CertificateException {
    final Key key = privateKey(keyFile);
    final Certificate cert = loadCertificate(certFile);
    final Certificate caCert = loadCertificate(caCertFile);
    final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);
    // alias, private key, password, certificate chain
    keyStore.setKeyEntry("key", key, keyStorePassword,
        new Certificate[] { cert });
    return keyStore;
  }

  // Makes a trust store, suitable for backing a TrustManager, out of a CA cert
  // file.
  public static KeyStore trustStore(final String caCertFile) throws
    KeyStoreException, IOException, NoSuchAlgorithmException,
    CertificateException {
    final KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    keyStore.setCertificateEntry("cacert", loadCertificate(caCertFile));
    return keyStore;
  }

  // An X.509 trust manager for a KeyStore.
  public static TrustManager trustManager(final KeyStore keyStore) throws
    NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException {
    final TrustManagerFactory factory =
      TrustManagerFactory.getInstance("PKIX", "SunJSSE");
    synchronized(factory) {
      factory.init(keyStore);
      for (TrustManager tm : factory.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          return tm;
        }
      }
      return null;
    }
  }

  // An X.509 key manager for a KeyStore
  public static KeyManager keyManager(final KeyStore keyStore) throws
    NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException,
    UnrecoverableKeyException {
    final KeyManagerFactory factory =
      KeyManagerFactory.getInstance("PKIX", "SunJSSE");
    synchronized(factory) {
      factory.init(keyStore, keyStorePassword);
      for (KeyManager km : factory.getKeyManagers()) {
        if (km instanceof X509KeyManager) {
          return km;
        }
      }
      return null;
    }
  }

  // Builds an SSL Context from a PKCS8 key file, a certificate file, and a
  // trusted CA certificate used to verify peers.
  public static SSLContext sslContext(final String keyFile,
                           final String certFile,
                           final String caCertFile) throws
    KeyManagementException, NoSuchAlgorithmException, FileNotFoundException,
    KeyStoreException, IOException, InvalidKeySpecException,
    CertificateException, NoSuchProviderException, UnrecoverableKeyException {

    final KeyManager keyManager = keyManager(
                                    keyStore(keyFile, certFile, caCertFile));
    final TrustManager trustManager = trustManager(trustStore(caCertFile));
    final SSLContext context = SSLContext.getInstance("TLS");
    context.init(new KeyManager[] { keyManager },
                 new TrustManager[] { trustManager },
                 null);
    return context;
  }

  public static SSLContext uncheckedSSLContext(final String keyFile, final String certFile, final String caCertFile) {
    // hack hack hack
    try {
      return sslContext(keyFile, certFile, caCertFile);
    } catch (KeyManagementException e) {
      throw new RuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (KeyStoreException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException(e);
    } catch (CertificateException e) {
      throw new RuntimeException(e);
    } catch (NoSuchProviderException e) {
      throw new RuntimeException(e);
    } catch (UnrecoverableKeyException e) {
      throw new RuntimeException(e);
    }
  }
}
