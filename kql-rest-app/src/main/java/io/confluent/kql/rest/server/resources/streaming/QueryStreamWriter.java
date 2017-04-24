package io.confluent.kql.rest.server.resources.streaming;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.planner.plan.AggregateNode;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.rest.server.resources.KQLExceptionMapper;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.util.SerDeUtil;
import io.confluent.kql.util.WindowedSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final KQLEngine kqlEngine;
  private final String streamName;
  private final long disconnectCheckInterval;
  private final Map<String, Object> streamsProperties;
  private final KStreamBuilder streamBuilder;
  private final KStream<?, GenericRow> queryKStream;
  private final KafkaStreams querySourceStreams;
  private final List<Field> columns;
  private AtomicReference<Throwable> streamsException;

  public QueryStreamWriter(
      KQLEngine kqlEngine,
      String streamName,
      long disconnectCheckInterval,
      Map<String, Object> streamsProperties,
      String queryString
  ) throws Exception {

    // TODO: Look into this, figure out why the exception is thrown in cases where it appears it shouldn't be
//            if (querySpecification.getInto().isPresent()) {
//                Relation into = querySpecification.getInto().get();
//                if (into instanceof Table) {
//                    throw new Exception("INTO clause currently not supported for REST app queries");
//                }
//            }
    streamsException = new AtomicReference<>(null);

    this.kqlEngine = kqlEngine;
    this.streamName = streamName;
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.streamsProperties = streamsProperties;

    // TODO: Find a better way to do this
    String redirectedQuery = String.format("CREATE STREAM %s AS %s;", streamName, queryString);

    // Want to create a new application ID in order to ensure that the stream is read from the beginning
    QueryMetadata queryMetadata = kqlEngine.runMultipleQueries(true, redirectedQuery).get(0);

    KQLStructuredDataOutputNode outputNode = (KQLStructuredDataOutputNode) queryMetadata.getQueryOutputNode();
    KQLTopic kqlTopic = outputNode.getKqlTopic();

    querySourceStreams = queryMetadata.getQueryKafkaStreams();
    querySourceStreams.setUncaughtExceptionHandler(new StreamsExceptionHandler());

    streamBuilder = new KStreamBuilder();

    Serde keySerde = outputNode.getSource() instanceof AggregateNode ? new WindowedSerde() : Serdes.String();

    queryKStream = streamBuilder.stream(
        keySerde,
        SerDeUtil.getRowSerDe(kqlTopic.getKqlTopicSerDe(), outputNode.getSchema()),
        kqlTopic.getKafkaTopicName()
    );

    columns = outputNode.getSchema().fields();
  }

  @Override
  public void write(OutputStream output) throws IOException {
    AtomicBoolean rowsWritten = new AtomicBoolean(false);

    queryKStream.foreach(new QueryRowWriter<>(output, streamsException, columns, rowsWritten));

    Map<String, Object> streamedQueryProperties = new HashMap<>(streamsProperties);
    streamedQueryProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, streamName);
    streamedQueryProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaStreams queryStreams = new KafkaStreams(streamBuilder, new StreamsConfig(streamedQueryProperties));
    queryStreams.setUncaughtExceptionHandler(new StreamsExceptionHandler());
    queryStreams.start();

    try {
      // TODO: Think about the control flow here--have to detect user-terminated connection somehow but also handle multi-threaded-ness of Kafka Streams
      while (true) {
        Thread.sleep(disconnectCheckInterval);
        Throwable exception = streamsException.get();
        if (exception != null) {
          throw exception;
        }
        // If no new rows have been written, the user may have terminated the connection without us knowing.
        // Check by trying to write a single newline.
        if (!rowsWritten.getAndSet(false)) {
          synchronized (output) {
            output.write("\n".getBytes());
            output.flush();
          }
        }
      }
    } catch (EOFException exception) {
      // The user has terminated the connection; we can stop writing
    } catch (Throwable exception) {
      log.error("Exception occurred while writing to connection stream: ", exception);
      output.write(("\n" + KQLExceptionMapper.stackTraceJson(exception).toString() + "\n").getBytes());
    }

    log.info("Closing and cleaning up KafkaStreams instances for streamed query");
    queryStreams.close();
    queryStreams.cleanUp();
    querySourceStreams.close();
    querySourceStreams.cleanUp();
    log.info("KafkaStreams instances have all been closed and cleaned up");

    kqlEngine.getMetaStore().deleteSource(streamName);
    log.info("Temporary stream has been removed from metastore");
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
      streamsException.compareAndSet(null, exception);
    }
  }
}
