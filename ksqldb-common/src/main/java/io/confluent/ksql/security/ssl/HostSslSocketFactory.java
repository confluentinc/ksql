/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.security.ssl;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Provider;
import java.security.Security;
import javax.net.ssl.SSLSocketFactory;
import org.bouncycastle.jsse.BCSSLSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a wrapper class on top of {@link SSLSocketFactory} to address
 * issue where host name is not set on {@link BCSSLSocket} when creating a socket.
 */
public class HostSslSocketFactory extends SSLSocketFactory {

  private static final Logger log = LoggerFactory.getLogger(HostSslSocketFactory.class);

  private final SSLSocketFactory sslSocketFactory;
  private final String peerHost;

  private static final String BC_FIPS_SSL_PROVIDER = "BCJSSE";
  private static final String BC_FIPS_SECURITY_PROVIDER = "BCFIPS";

  public HostSslSocketFactory(final SSLSocketFactory sslSocketFactory, final String peerHost) {
    this.sslSocketFactory = sslSocketFactory;
    this.peerHost = peerHost;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return sslSocketFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return sslSocketFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(final Socket socket,
                             final String host, final int port, final boolean autoClose)
          throws IOException {
    return interceptAndSetHost(sslSocketFactory.createSocket(socket, host, port, autoClose));
  }

  @Override
  public Socket createSocket() throws IOException {
    final Socket socket = sslSocketFactory.createSocket();
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(final String host, final int port) throws IOException {
    final Socket socket = sslSocketFactory.createSocket(host, port);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(final String host,
                             final int port, final InetAddress localAddress, final int localPort)
          throws IOException {
    final Socket socket = sslSocketFactory.createSocket(host, port, localAddress, localPort);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(final InetAddress address, final int port) throws IOException {
    final Socket socket = sslSocketFactory.createSocket(address, port);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(final InetAddress address,
                             final int port, final InetAddress remoteAddress,
                             final int remotePort) throws IOException {
    final Socket socket = sslSocketFactory.createSocket(address, port, remoteAddress, remotePort);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(final Socket socket,
                             final InputStream inputStream, final boolean autoClose)
          throws IOException {
    return interceptAndSetHost(sslSocketFactory.createSocket(socket, inputStream, autoClose));
  }

  private Socket interceptAndSetHost(final Socket socket) {
    if (peerHost != null && isFipsDeployment() && socket instanceof BCSSLSocket) {
      final BCSSLSocket bcsslSocket = (BCSSLSocket) socket;
      if (!Strings.isNullOrEmpty(peerHost)) {
        log.debug("Setting hostname on Bouncy Castle SSL socket: {}", peerHost);
        bcsslSocket.setHost(peerHost);
      }
    }
    return socket;
  }

  public static boolean isFipsDeployment() {
    final Provider bcFipsProvider = Security.getProvider(BC_FIPS_SECURITY_PROVIDER);
    final Provider bcFipsJsseProvider = Security.getProvider(BC_FIPS_SSL_PROVIDER);
    return (bcFipsProvider != null) && (bcFipsJsseProvider != null);
  }

}
