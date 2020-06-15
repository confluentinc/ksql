package io.confluent.ksql.rest.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class TestProxy {

  public static void main(String[] args) {
    TestProxy.run("localhost", 8089, "localhost", 8088, () -> false);
  }

  public static void runInBackground(String host, int port, String remoteHost, int remotePort, Supplier<Boolean> pause) {
    Executor executor = Executors.newSingleThreadExecutor();
    executor.execute(() -> {
      run(host, port, remoteHost, remotePort, pause);
    });
  }

  public static void run(String host, int port, String remoteHost, int remotePort, Supplier<Boolean> pause) {
    InetSocketAddress listenAddress = new InetSocketAddress(host, port);
    Map<SocketChannel, SocketChannel> map = new HashMap<>();
    try (
        Selector selector = Selector.open();
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
    ) {
      serverChannel.configureBlocking(false);
      serverChannel.socket().bind(listenAddress);
      serverChannel.register(selector, SelectionKey.OP_ACCEPT);

      ByteBuffer buffer = ByteBuffer.allocate(5120);

      while (true) {

        if (pause.get()) {
          try {
            Thread.sleep(200);
            continue;
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        selector.select();

        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
        while (keys.hasNext()) {
          SelectionKey key = keys.next();
          keys.remove();

          if (!key.isValid()) {
            continue;
          }

          if (key.isAcceptable()) {
            ServerSocketChannel newServerChannel = (ServerSocketChannel) key.channel();
            SocketChannel channel = newServerChannel.accept();
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ);

            InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
            SocketChannel remoteChannel = SocketChannel.open(address);
            remoteChannel.configureBlocking(false);
            remoteChannel.register(selector, SelectionKey.OP_READ);
            map.put(channel, remoteChannel);
            map.put(remoteChannel, channel);
          } else if (key.isReadable()) {
            SocketChannel channel = (SocketChannel) key.channel();

            buffer.clear();
            int numRead = channel.read(buffer);
            if (numRead == -1) {
              Socket socket = channel.socket();
              SocketAddress remoteAddr = socket.getRemoteSocketAddress();
              System.out.println("Connection closed by client: " + remoteAddr);
              closeAll(key, channel, map);
              continue;
            }

//            String str = new String(buffer.array(), 0, numRead);
//            System.out.println("Msg: " + str);
            SocketChannel outgoingSocketChannel = map.get(channel);
            try {
              int position = buffer.position();
              buffer.limit(position);
              buffer.position(0);
              while (buffer.position() < buffer.limit()) {
                outgoingSocketChannel.write(buffer);
                Thread.sleep(10);
              }
            } catch (IOException e) {
              closeAll(key, channel, map);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    } catch (IOException e){
      e.printStackTrace();
    }
  }

  private static void closeAll(SelectionKey key, SocketChannel channel, Map<SocketChannel, SocketChannel> map) {
    SocketChannel other = map.get(channel);
    closeAll(channel);
    closeAll(other);
    key.cancel();
    map.remove(channel);
    map.remove(other);
  }

  private static void closeAll(SocketChannel channel) {
    Socket socket = channel.socket();
    try {
      socket.close();
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
