/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.entity.StreamedRowMatchers.matchersRowsAnyOrder;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.extractQueryId;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequestWithResponse;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makePullQueryRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.UserDataProviderBig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.impl.Http1xClientConnection;
import io.vertx.core.http.impl.HttpClientResponseImpl;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.net.impl.ConnectionBase;
import io.vertx.core.net.impl.VertxHandler;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.zookeeper.ZooKeeperClientException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AllArguments;
import net.bytebuddy.asm.Advice.OnMethodEnter;
import net.bytebuddy.asm.Advice.OnMethodExit;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.Implementation.Context.Default.Factory;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({IntegrationTest.class})
public class PullQueryLimitHARoutingTest {

    public static final Logger LOG = LoggerFactory.getLogger(PullQueryLimitHARoutingTest.class);

    private static final String USER_TOPIC = "user_topic_";
    private static final String USERS_STREAM = "users";
    private static final UserDataProviderBig USER_PROVIDER = new UserDataProviderBig();
    private static final int TOTAL_RECORDS = USER_PROVIDER.getNumRecords();
    private static final int HEADER = 1;
    private static final int LIMIT_REACHED_MESSAGE = 1;
    private static final int COMPLETE_MESSAGE = 1;
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
    private static final int BASE_TIME = 1_000_000;
    private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
    private String output;
    private String queryId;
    private String topic;

    private static final Optional<BasicCredentials> USER_CREDS = Optional.empty();

    private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
            LogicalSchema.builder()
                    .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
                    .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
                    .build(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
    );

    private static final Map<String, Object> COMMON_CONFIG = ImmutableMap.<String, Object>builder()
            .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
            .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
            .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 500)
            .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
            .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
            .put(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
            .put(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
            .put(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
            .put(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
            .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
            .build();

    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_0 = new HighAvailabilityTestUtil.Shutoffs();
    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_1 = new HighAvailabilityTestUtil.Shutoffs();
    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_2 = new HighAvailabilityTestUtil.Shutoffs();

    private static final int INT_PORT_0 = TestUtils.findFreeLocalPort();
    private static final int INT_PORT_1 = TestUtils.findFreeLocalPort();
    private static final int INT_PORT_2 = TestUtils.findFreeLocalPort();
    private static final KsqlHostInfoEntity HOST0 = new KsqlHostInfoEntity("localhost", INT_PORT_0);
    private static final KsqlHostInfoEntity HOST1 = new KsqlHostInfoEntity("localhost", INT_PORT_1);
    private static final KsqlHostInfoEntity HOST2 = new KsqlHostInfoEntity("localhost", INT_PORT_2);

    @Rule
    public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
            .builder(TEST_HARNESS::kafkaBootstrapServers)
            .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
            .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
            .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
            .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
            .withFaultyKsqlClient(APP_SHUTOFFS_0::getKsqlOutgoing)
            .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer0.class.getName())
            .withProperties(COMMON_CONFIG)
            .build();

    @Rule
    public final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
            .builder(TEST_HARNESS::kafkaBootstrapServers)
            .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
            .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
            .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_1)
            .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_1)
            .withFaultyKsqlClient(APP_SHUTOFFS_1::getKsqlOutgoing)
            .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer1.class.getName())
            .withProperties(COMMON_CONFIG)
            .build();

    @Rule
    public final TestKsqlRestApp REST_APP_2 = TestKsqlRestApp
            .builder(TEST_HARNESS::kafkaBootstrapServers)
            .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
            .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
            .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_2)
            .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_2)
            .withFaultyKsqlClient(APP_SHUTOFFS_2::getKsqlOutgoing)
            .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer2.class.getName())
            .withProperties(COMMON_CONFIG)
            .build();

    public final TestApp TEST_APP_0 = new TestApp(HOST0, REST_APP_0, APP_SHUTOFFS_0);
    public final TestApp TEST_APP_1 = new TestApp(HOST1, REST_APP_1, APP_SHUTOFFS_1);
    public final TestApp TEST_APP_2 = new TestApp(HOST2, REST_APP_2, APP_SHUTOFFS_2);

    public final List<TestApp> ALL_TEST_APPS = ImmutableList.of(TEST_APP_0, TEST_APP_1, TEST_APP_2);

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
            .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
            .around(TEST_HARNESS)
            .around(TMP);

    @Rule
    public final Timeout timeout = Timeout.builder()
            .withTimeout(4, TimeUnit.MINUTES)
            .withLookingForStuckThread(true)
            .build();

    public static class VertxHandlerAdvice {

      @OnMethodEnter
      public static void foo(
          @Advice.Argument(0)ChannelHandlerContext chctx,
          @Advice.Argument(1) Object msg
      ) {
        LOG.info("{} VertxHandler Received Message: {}", chctx.toString(), msg);
      }

    }

    public static class CCAdvice {

      @OnMethodEnter
      public static void foo(
          @Advice.FieldValue("chctx") ChannelHandlerContext chctx,
          @Advice.Argument(0) Object stream
      ) throws Exception {
        final Field endHandlerF = stream.getClass().getDeclaredField("endHandler");
        endHandlerF.setAccessible(true);

        final Field queueF = stream.getClass().getDeclaredField("queue");
        queueF.setAccessible(true);

        final Field headF = stream.getClass().getDeclaredField("request");
        headF.setAccessible(true);

        LOG.info("{} Http1xClientConnection hit handleResponseEnd() with endHandler "
            + "set on stream {} with {}null head",
            chctx.toString(),
            endHandlerF.get(stream),
            headF.get(stream) != null ? "non-" : "");
      }

    }

  public static class HttpEventHandlerAdvice {

    @OnMethodEnter
    public static void foo(
        @Advice.Argument(0) Handler<Void> handler,
        @Advice.FieldValue("endHandler") Handler<Void> endHandler
    ) {
      final String handlerClazz = handler == null
          ? "null" : handler.getClass().getSimpleName();
      final String endHandlerClazz = endHandler == null
          ? "null" : endHandler.getClass().getSimpleName();

      if (handlerClazz.contains("RecordParserImpl") || endHandlerClazz.contains("RecordParserImpl")) {
        LOG.info(
            "Replacing handler class {} with handler of class {} at trace {}",
            endHandlerClazz,
            handlerClazz,
            ExceptionUtils.getStackTrace(new Throwable())
        );
      }
    }
  }

  public static class HttpClientResponseAdvice {

    @OnMethodEnter
    public static void foo(
        @Advice.FieldValue("conn") HttpConnection conn,
        @Advice.FieldValue("eventHandler") Object eventHandler
    ) throws Exception {
      final Field endHandlerF = eventHandler.getClass().getDeclaredField("endHandler");
      endHandlerF.setAccessible(true);
      final Object endHandler = endHandlerF.get(eventHandler);

      LOG.info(
          "{} HttpClientResponse handleEnd() with handler object {} class {}. Stack trace: {}",
          ((Http1xClientConnection) conn).channelHandlerContext(),
          endHandler,
          endHandler != null ? endHandler.getClass() : null,
          ExceptionUtils.getStackTrace(new Throwable())
      );
    }

  }

  public static class StreamImplAdvice {

      @OnMethodEnter
      public static void foo(
          @Advice.FieldValue("conn") Object conn,
          @Advice.FieldValue("endHandler") Object endHandler,
          @Advice.Origin String origin,
          @Advice.AllArguments Object[] args
      ) {
        String trace = "";
        if (origin.contains("endHandler")) {
          trace = ExceptionUtils.getStackTrace(new Throwable());
        }

        LOG.info(
            "{} {}({}) called. Field value for endHandler: {}{}",
            ((ConnectionBase) conn).channelHandlerContext(),
            origin,
            Arrays.toString(args),
            endHandler,
            trace.isEmpty() ? "" : " from: " + trace
        );
      }
  }

  public static class EventLoopIntercept {

      @RuntimeType
      public static void intercept(
          @Argument(1) Object arg,
          @Argument(2) Handler<Object> task,
          @This EventLoopContext context
      ) throws Exception {
        EventLoop eventLoop = context.nettyEventLoop();
        if (eventLoop.inEventLoop()) {
          task.handle(arg);
        } else {
          final Field streamF = task.getClass().getDeclaredField("arg$1");
          streamF.setAccessible(true);

          final Object stream = streamF.get(task);
          final Field connF = stream.getClass().getDeclaredField("conn");
          connF.setAccessible(true);

          final Field threadF = SingleThreadEventExecutor.class.getDeclaredField("thread");
          threadF.setAccessible(true);
          final Thread thread = (Thread) threadF.get(eventLoop);

          final ConnectionBase conn = (ConnectionBase) connF.get(stream);
          LOG.info("{} Enqueued EventLoopLambda onto loop with thread {} with task: {}",
              conn.channelHandlerContext(), thread.getName(), task);
          eventLoop.execute(new EventLoopLambda(arg, task, conn));
        }
      }
  }

  public static class EventLoopLambda implements Runnable {
      public final Object arg;
      public final Handler task;
      public final ConnectionBase conn;

    public EventLoopLambda(
        final Object arg,
        final Handler task,
        final ConnectionBase conn) {
      this.arg = arg;
      this.task = task;
      this.conn = conn;
    }

    @Override
    public void run() {
      task.handle(arg);
    }
  }

  public static class EventExecAdvice {

      @OnMethodExit
      public static void foo(
          @Advice.Return Runnable task
      ) {
        if (task instanceof EventLoopLambda) {
          LOG.info("{} Pulled EventLoopLambda from loop with thread {} with task: {}",
              ((EventLoopLambda) task).conn.channelHandlerContext(),
              Thread.currentThread().getName(),
              ((EventLoopLambda) task).task);
        }
      }

  }

  @Before
    public void setUp() throws ClassNotFoundException {

      //  private void handleResponseEnd(Stream stream, LastHttpContent trailer) {

        new ByteBuddy()
            .with(Factory.INSTANCE)
            .redefine(VertxHandler.class)
            .visit(Advice.to(VertxHandlerAdvice.class).on(ElementMatchers.named("channelRead")))
            .make()
            .load(VertxHandler.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());

        new ByteBuddy()
            .with(Factory.INSTANCE)
            .redefine(Http1xClientConnection.class)
            .visit(Advice.to(CCAdvice.class).on(ElementMatchers.named("handleResponseEnd")))
            .make()
            .load(Http1xClientConnection.class.getClassLoader(),
                ClassReloadingStrategy.fromInstalledAgent());

      new ByteBuddy()
          .with(Factory.INSTANCE)
          .redefine(HttpClientResponseImpl.class)
          .visit(Advice.to(HttpClientResponseAdvice.class).on(ElementMatchers.named("handleEnd")))
          .make()
          .load(HttpClientResponseImpl.class.getClassLoader(),
              ClassReloadingStrategy.fromInstalledAgent());

      new ByteBuddy()
          .with(Factory.INSTANCE)
          .redefine(HttpClientResponseImpl.class.getClassLoader()
              .loadClass("io.vertx.core.http.impl.HttpEventHandler"))
          .visit(Advice.to(HttpEventHandlerAdvice.class).on(ElementMatchers.named("endHandler")))
          .make()
          .load(HttpClientResponseImpl.class.getClassLoader(),
              ClassReloadingStrategy.fromInstalledAgent());

      new ByteBuddy()
          .with(Factory.INSTANCE)
          .redefine(Http1xClientConnection.class.getClassLoader()
              .loadClass("io.vertx.core.http.impl.Http1xClientConnection$StreamImpl"))
          .visit(Advice.to(StreamImplAdvice.class).on(
              ElementMatchers.nameContainsIgnoreCase("handle")
                  .and(ElementMatchers.not(ElementMatchers.nameContainsIgnoreCase("chunk")))))
          .make()
          .load(Http1xClientConnection.class.getClassLoader(),
              ClassReloadingStrategy.fromInstalledAgent());

    new ByteBuddy()
        .with(Factory.INSTANCE)
        .redefine(EventLoopContext.class)
        .method(ElementMatchers.named("execute").and(ElementMatchers.takesArguments(3)))
        .intercept(MethodDelegation.to(EventLoopIntercept.class))
        .make()
        .load(EventLoopContext.class.getClassLoader(),
            ClassReloadingStrategy.fromInstalledAgent());

    new ByteBuddy()
        .with(Factory.INSTANCE)
        .redefine(SingleThreadEventExecutor.class)
        .visit(Advice.to(EventExecAdvice.class).on(ElementMatchers.named("pollTaskFrom")))
        .make()
        .load(SingleThreadEventExecutor.class.getClassLoader(),
            ClassReloadingStrategy.fromInstalledAgent());

        //Create topic with 4 partition to control who is active and standby
        topic = USER_TOPIC + KsqlIdentifierTestUtil.uniqueIdentifierName();
        TEST_HARNESS.ensureTopics(4, topic);

        TEST_HARNESS.produceRows(
                topic,
                USER_PROVIDER,
                FormatFactory.KAFKA,
                FormatFactory.JSON,
                timestampSupplier::getAndIncrement
        );

        //Create stream
        makeAdminRequest(
                REST_APP_0,
                "CREATE STREAM " + USERS_STREAM
                        + " (" + USER_PROVIDER.ksqlSchemaString(false) + ")"
                        + " WITH ("
                        + "   kafka_topic='" + topic + "', "
                        + "   value_format='JSON');",
                USER_CREDS
        );
        //Create table
        output = KsqlIdentifierTestUtil.uniqueIdentifierName();
        List<KsqlEntity> res = makeAdminRequestWithResponse(
                REST_APP_0,
                "CREATE TABLE " + output + " AS"
                        + " SELECT " + USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
                        + " GROUP BY " + USER_PROVIDER.key() + ";",
                USER_CREDS
        );
        queryId = extractQueryId(res.get(0).toString());
        queryId = queryId.substring(0, queryId.length() - 1);
        waitForTableRows();

        waitForStreamsMetadataToInitialize(
                REST_APP_0, ImmutableList.of(HOST0, HOST1, HOST2), USER_CREDS);
    }

    @After
    public void cleanUp() {
        REST_APP_0.closePersistentQueries(USER_CREDS);
        REST_APP_0.dropSourcesExcept(USER_CREDS);
        APP_SHUTOFFS_0.reset();
        APP_SHUTOFFS_1.reset();
        APP_SHUTOFFS_2.reset();
    }

    @Test
    public void shouldReturnLimitRowsMultiHostSetupTable() {
        // Given:
        final int numLimitRows = 300;
        final int limitSubsetSize = HEADER + numLimitRows;

        // check for lags reported for every app
        for (TestApp testApp : ALL_TEST_APPS) {
            waitForRemoteServerToChangeStatus(TEST_APP_0.getApp(),
                    testApp.getHost(),
                    HighAvailabilityTestUtil.lagsReported(testApp.getHost(), Optional.of(10L),
                            10),
                    USER_CREDS);
        }

        final String sqlTableScan = "SELECT * FROM " + output + ";";
        final String sqlLimit = "SELECT * FROM " + output + " LIMIT " + numLimitRows + " ;";

        //issue table scan first
        final List<StreamedRow> rows_0 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlTableScan, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS));

        // issue pull query with limit
        final List<StreamedRow> rows_1 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(limitSubsetSize));

        // Partition off one test app
        TEST_APP_1.getShutoffs().shutOffAll();

        waitForRemoteServerToChangeStatus(
                TEST_APP_0.getApp(),
                TEST_APP_2.getHost(),
                HighAvailabilityTestUtil::remoteServerIsUp,
                USER_CREDS);
        waitForRemoteServerToChangeStatus(
                TEST_APP_0.getApp(),
                TEST_APP_1.getHost(),
                HighAvailabilityTestUtil::remoteServerIsDown,
                USER_CREDS);

        // issue table scan after partitioning an app
        final List<StreamedRow> rows_2 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlTableScan, null, USER_CREDS);

        // check that we get all rows back
        assertThat(rows_2, hasSize(HEADER + TOTAL_RECORDS));
        assertThat(rows_0, is(matchersRowsAnyOrder(rows_2)));

        // issue pull query with limit after partitioning an app
        final List<StreamedRow> rows_3 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_3, hasSize(limitSubsetSize));
    }

    @Test
    public void shouldReturnLimitRowsMultiHostSetupStream() {
        // Given:
        final int numLimitRows = 200;
        final int limitSubsetSize = HEADER + numLimitRows + LIMIT_REACHED_MESSAGE;

        // check for lags reported for every app
        for (TestApp testApp : ALL_TEST_APPS) {
            waitForRemoteServerToChangeStatus(TEST_APP_0.getApp(),
                    testApp.getHost(),
                    HighAvailabilityTestUtil.lagsReported(testApp.getHost(), Optional.of(10L),
                            10),
                    USER_CREDS);
        }

        final String sqlStreamPull = "SELECT * FROM " + USERS_STREAM + ";";
        final String sqlLimit = "SELECT * FROM " + USERS_STREAM + " LIMIT " + numLimitRows + " ;";

        //scan the entire stream first
        final List<StreamedRow> rows_0 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlStreamPull, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS + COMPLETE_MESSAGE));

        //issue pull query on stream with limit
        final List<StreamedRow> rows_1 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(limitSubsetSize));

        // Partition off an app
        TEST_APP_1.getShutoffs().shutOffAll();

        waitForRemoteServerToChangeStatus(
                TEST_APP_0.getApp(),
                TEST_APP_2.getHost(),
                HighAvailabilityTestUtil::remoteServerIsUp,
                USER_CREDS);
        waitForRemoteServerToChangeStatus(
                TEST_APP_0.getApp(),
                TEST_APP_1.getHost(),
                HighAvailabilityTestUtil::remoteServerIsDown,
                USER_CREDS);


        // issue stream scan after partitioning an app
        final List<StreamedRow> rows_2 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlStreamPull, null, USER_CREDS);

        // check that we get all rows back
        assertThat(rows_2, hasSize(HEADER + TOTAL_RECORDS + COMPLETE_MESSAGE));
        assertThat(rows_0, is(matchersRowsAnyOrder(rows_2)));

        // issue pull query with limit after partitioning an app
        final List<StreamedRow> rows_3 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_3, hasSize(limitSubsetSize));
    }

    private void waitForTableRows() {
        TEST_HARNESS.verifyAvailableUniqueRows(
                output.toUpperCase(),
                USER_PROVIDER.data().size(),
                FormatFactory.KAFKA,
                FormatFactory.JSON,
                AGGREGATE_SCHEMA
        );
    }

    private static String getNewStateDir() {
        try {
            return TMP.newFolder().getAbsolutePath();
        } catch (final IOException e) {
            throw new AssertionError("Failed to create new state dir", e);
        }
    }

    private static class TestApp {

        private final KsqlHostInfoEntity host;
        private final TestKsqlRestApp app;
        private final HighAvailabilityTestUtil.Shutoffs shutoffs;

        public TestApp(KsqlHostInfoEntity host, TestKsqlRestApp app, HighAvailabilityTestUtil.Shutoffs shutoffs) {
            this.host = host;
            this.app = app;
            this.shutoffs = shutoffs;
        }

        public KsqlHostInfoEntity getHost() {
            return host;
        }

        public TestKsqlRestApp getApp() {
            return app;
        }

        public HighAvailabilityTestUtil.Shutoffs getShutoffs() {
            return shutoffs;
        }
    }
}
