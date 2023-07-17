/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.perf;

import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.security.KsqlDefaultSecurityExtension;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple tool for measuring performance of our API.
 * <p>
 * Implement the actual test in a subclass by overriding the abstract methods.
 * <p>
 * This tool is a simple rough and ready tool that does not pretend to be a fully fledged
 * performance testing tool. It's really useful for quickly running perf tests in your IDE to get a
 * rough idea of performance and do a first pass of performance tuning parameters - e.g. reactive
 * streams buffer sizes.
 */
public abstract class BasePerfRunner {

  private Endpoints endpoints;
  private final AtomicInteger counter = new AtomicInteger();
  private long totalTime;
  private int totalCount;
  private int numWarmupRuns;
  private int numRuns;
  private long runMs;

  protected Vertx vertx;
  protected WebClient client;
  protected Server server;

  protected volatile Throwable throwable;

  /**
   * Implement to configure your test
   */
  protected abstract void configure();

  /**
   * Implement this with the clean up logic of your run
   *
   * @throws Exception
   */
  protected abstract void endRun() throws Exception;

  /**
   * Implement this with the actual run logic of your test
   *
   * @param ms - how long it should run for in ms
   * @throws Exception
   */
  protected abstract void run(long ms) throws Exception;

  protected void count() {
    counter.incrementAndGet();
  }

  protected BasePerfRunner setNumWarmupRuns(int runs) {
    this.numWarmupRuns = runs;
    return this;
  }

  protected BasePerfRunner setNumRuns(int runs) {
    this.numRuns = runs;
    return this;
  }

  protected BasePerfRunner setRunMs(long runMs) {
    this.runMs = runMs;
    return this;
  }

  protected BasePerfRunner setEndpoints(Endpoints endpoints) {
    this.endpoints = endpoints;
    return this;
  }

  protected KsqlRestConfig createServerConfig() {
    final Map<String, Object> config = new HashMap<>();
    config.put("ksql.apiserver.listen.host", "localhost");
    config.put("ksql.apiserver.listen.port", 8089);
    config.put("ksql.apiserver.tls.enabled", false);

    return new KsqlRestConfig(config);
  }

  protected void errorOccurred(Throwable throwable) {
    this.throwable = throwable;
  }

  protected final void go() {
    try {
      configure();
      setUp();
      System.out.println("Warming up for " + numWarmupRuns + " iterations");
      for (int i = 0; i < numWarmupRuns; i++) {
        System.out.println("Warming up iteration " + (i + 1));
        doRun();
      }
      totalTime = 0;
      totalCount = 0;
      System.out.println("Running for " + numRuns + " iterations");
      for (int i = 0; i < numRuns; i++) {
        System.out.println("Run iteration " + (i + 1));
        doRun();
      }
      System.out
          .println(String.format("Mean rate is %.0f counts/sec", calcRate(totalTime, totalCount)));
      tearDown();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private double calcRate(long duration, int count) {
    return 1000 * (double) count / duration;
  }

  private void doRun() throws Throwable {
    counter.set(0);

    long start = System.currentTimeMillis();

    run(runMs);

    long duration = System.currentTimeMillis() - start;

    int sent = counter.get();

    double rate = calcRate(duration, sent);

    if (throwable != null) {
      throw throwable;
    }

    System.out.println(String.format("Rate is %.0f counts/sec", rate));

    totalTime += duration;
    totalCount += sent;

    endRun();
  }

  private void setUp() {
    vertx = Vertx.vertx();
    KsqlRestConfig serverConfig = createServerConfig();
    final ServerState serverState = new ServerState();
    serverState.setReady();
    server = new Server(vertx, serverConfig, endpoints, new KsqlDefaultSecurityExtension(),
        Optional.empty(), serverState, Optional.empty());
    server.start();
    client = createClient();
  }

  private void tearDown() {
    server.stop();
    client.close();
    vertx.close();
  }

  private WebClientOptions createClientOptions() {
    return new WebClientOptions()
        .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false);
  }

  private WebClient createClient() {
    return WebClient.create(vertx, createClientOptions());
  }

}
