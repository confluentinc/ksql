/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.resources;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.planner.plan.AggregateNode;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.rest.StatementParser;
import io.confluent.kql.rest.TopicUtil;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.util.SerDeUtil;
import io.confluent.kql.util.WindowedSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class StreamedQueryResource {
  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KQLEngine kqlEngine;
  private final TopicUtil topicUtil;
  private final String nodeId;
  private final StatementParser statementParser;
  private final AtomicInteger queriesCreated;
  private final long disconnectCheckInterval;
  private final Map<String, Object> streamsProperties;

  public StreamedQueryResource(
      KQLEngine kqlEngine,
      TopicUtil topicUtil,
      String nodeId,
      StatementParser statementParser,
      long disconnectCheckInterval,
      Map<String, Object> streamsProperties) {
    this(kqlEngine, topicUtil, nodeId, statementParser, disconnectCheckInterval, streamsProperties, 0);
  }

  public StreamedQueryResource(
      KQLEngine kqlEngine,
      TopicUtil topicUtil,
      String nodeId,
      StatementParser statementParser,
      long disconnectCheckInterval,
      Map<String, Object> streamsProperties,
      int queriesCreated
  ) {
    this.kqlEngine = kqlEngine;
    this.topicUtil = topicUtil;
    this.nodeId = nodeId;
    this.statementParser = statementParser;
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.streamsProperties = streamsProperties;
    this.queriesCreated = new AtomicInteger(queriesCreated);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response performQuery(KQLJsonRequest request) {
    try {
      String kql = Objects.requireNonNull(request.getKql(), "\"kql\" field must be given");
      Statement statement = statementParser.parseSingleStatement(kql);
      if (statement instanceof Query) {
        QueryStream queryStream = new QueryStream(kql);
        return Response.ok().entity(queryStream).type(MediaType.APPLICATION_JSON).build();
      } else {
        throw new Exception(String.format("Statement type `%s' not supported for this resource", statement.getClass().getName()));
      }
    } catch (Exception exception) {
      return KQLErrorResponse.stackTraceResponse(exception);
    }
  }

  private class QueryStream implements StreamingOutput {
    private final KStreamBuilder streamBuilder;
    private final KStream<?, GenericRow> queryKStream;
    private final KafkaStreams querySourceStreams;
    private final List<Field> columns;
    private final String intoTable;
    private AtomicReference<Throwable> streamsException;

    public QueryStream(String queryString) throws Exception {

      // TODO: Look into this, figure out why the exception is thrown in cases where it appears it shouldn't be
//            if (querySpecification.getInto().isPresent()) {
//                Relation into = querySpecification.getInto().get();
//                if (into instanceof Table) {
//                    throw new Exception("INTO clause currently not supported for REST app queries");
//                }
//            }
      streamsException = new AtomicReference<>(null);

      String queryId = String.format("%s_streamed_query_%d", nodeId, queriesCreated.incrementAndGet()).toUpperCase();
      intoTable = queryId;
      topicUtil.ensureTopicExists(queryId);

      log.info(String.format("Assigning query ID %s to streamed query \"%s\"", queryId, queryString));

      // TODO: Find a better way to do this
      String redirectedQuery = String.format("CREATE STREAM %s AS %s;", queryId, queryString);

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
      queryKStream.foreach(new QueryRowWriter<>(output));

      Map<String, Object> streamedQueryProperties = new HashMap<>(streamsProperties);
      streamedQueryProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, intoTable);
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
          // TODO: Consider not writing the newline if it's known that rows have been written recently
          synchronized (output) {
            output.write("\n".getBytes());
            output.flush();
          }
        }
      } catch (EOFException exception) {
        // The user has terminated the connection; we can stop writing
      } catch (Throwable exception) {
        log.error("Exception occurred while writing to connection stream: ", exception);
        output.write(("\n" + KQLErrorResponse.stackTraceJson(exception).toString() + "\n").getBytes());
      }

      log.info("Closing and cleaning up KafkaStreams instances for streamed query");
      queryStreams.close();
      queryStreams.cleanUp();
      querySourceStreams.close();
      querySourceStreams.cleanUp();
      log.info("KafkaStreams instances have all been closed and cleaned up");

      kqlEngine.getMetaStore().deleteSource(intoTable);
      log.info("Temporary stream has been removed from metastore");
    }

    private class QueryRowWriter<K> implements ForeachAction<K, GenericRow> {
      private final OutputStream output;

      public QueryRowWriter(OutputStream output) {
        this.output = output;
      }

      @Override
      public void apply(K key, GenericRow row) {
        String keyString;
        if (key instanceof Windowed) {
          keyString = ((Windowed) key).key().toString();
        } else {
          keyString = key.toString();
        }

        try {
          write(keyString, row);
        } catch (Exception exception) {
          streamsException.compareAndSet(null, exception);
        }
      }

      private void write(String key, GenericRow genericRow) throws Exception {
        JsonObjectBuilder values = Json.createObjectBuilder();
        List<Object> rowValues = genericRow.getColumns();

        if (rowValues.size() != columns.size()) {
          throw new Exception(String.format(
              "Lengths of columns and rowValues differ: %d vs %d, respectively",
              columns.size(),
              rowValues.size()
          ));
        }

        for (int i = 0; i < rowValues.size(); i++) {
          addRowValue(values, columns.get(i), rowValues.get(i));
        }

        JsonObjectBuilder row = Json.createObjectBuilder();
        row.add("values", values.build());
        row.add("key", key);

        JsonObjectBuilder message = Json.createObjectBuilder().add("row", row.build());

        synchronized (output) {
          output.write((message.build().toString() + "\n").getBytes());
          output.flush();
        }
      }

      private void addRowValue(JsonObjectBuilder row, Field columnField, Object value) throws Exception {
        String fieldName = columnField.name();
        if (value == null) {
          row.addNull(fieldName);
          return;
        }
        switch (columnField.schema().type()) {
          case FLOAT64:
            row.add(fieldName, (double) value);
            return;
          case FLOAT32:
            row.add(fieldName, (float) value);
            return;
          case INT8:
            row.add(fieldName, (byte) value);
            return;
          case INT16:
            row.add(fieldName, (short) value);
            return;
          case INT32:
            row.add(fieldName, (int) value);
            return;
          case INT64:
            row.add(fieldName, (long) value);
            return;
          case STRING:
            row.add(fieldName, (String) value);
            return;
          default:
            throw new Exception(String.format(
                "Cannot handle Field schema of type `%s' yet",
                columnField.schema().type()
            ));
        }
      }
    }

    private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
      @Override
      public void uncaughtException(Thread thread, Throwable exception) {
        streamsException.compareAndSet(null, exception);
      }
    }
  }
}
