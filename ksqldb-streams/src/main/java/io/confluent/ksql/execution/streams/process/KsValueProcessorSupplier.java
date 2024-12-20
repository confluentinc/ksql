package io.confluent.ksql.execution.streams.process;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.process.KsqlProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Supplies {@link KsValueProcessor} instances for processing values in a Kafka Streams pipeline.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsValueProcessorSupplier<K, R>
    implements ProcessorSupplier<K, GenericRow, K, R> {

  private final KsqlProcessor<K, R> delegate;

  public KsValueProcessorSupplier(final KsqlProcessor<K, R> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
  }

  @Override
  public Processor<K, GenericRow, K, R> get() {
    return new KsValueProcessor<>(delegate);
  }
}
