package io.confluent.ksql.security.ssl;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import org.bouncycastle.jsse.BCSSLSocket;
import org.mockito.Mockito;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.security.Security;

class HostSslSocketFactoryTest {
  private static final String BCFIPS_PROVIDER_NAME = "BCFIPS";
  private static final String BCFIPS_JSSE_PROVIDER_NAME = "BCJSSE";

  @Test
  void testInterceptAndSetHost() throws IOException {

    Security.addProvider(new BouncyCastleFipsProvider());
    Security.addProvider(new BouncyCastleJsseProvider());

    SSLSocketFactory mockSslSocketFactory = Mockito.mock(SSLSocketFactory.class);
    SSLSocketFactory hostSslSocketFactory = new HostSslSocketFactory(mockSslSocketFactory, "peerHost");

    Socket mockSocket = Mockito.mock(Socket.class, withSettings().extraInterfaces(BCSSLSocket.class));
    when(mockSslSocketFactory.createSocket()).thenReturn(mockSocket);

    hostSslSocketFactory.createSocket();

    verify((BCSSLSocket) mockSocket).setHost("peerHost");

    Security.removeProvider(BCFIPS_PROVIDER_NAME);
    Security.removeProvider(BCFIPS_JSSE_PROVIDER_NAME);

  }
}