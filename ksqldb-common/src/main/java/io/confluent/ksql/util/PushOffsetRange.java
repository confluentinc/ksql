package io.confluent.ksql.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Base64;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PushOffsetRange {
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModule(new Jdk8Module());
  }

  private int version;
  // If set, the start of the offset range associated with this batch of data.
  private final Optional<PushOffsetVector> startOffsets;
  // The offsets associated with this row/batch of rows, or the end of the offset range if start
  // offsets is set.
  private final PushOffsetVector endOffsets;

  @JsonCreator
  public PushOffsetRange(
      final @JsonProperty(value = "startOffsets") Optional<PushOffsetVector> startOffsets,
      final @JsonProperty(value = "endOffsets", required = true) PushOffsetVector endOffsets,
      final @JsonProperty(value = "version", required = true) int version
  ) {
    this.startOffsets = startOffsets;
    this.endOffsets = endOffsets;
    this.version = version;
  }

  public PushOffsetRange(
      final Optional<PushOffsetVector> startOffsets,
      final PushOffsetVector endOffsets
  ) {
    this.startOffsets = startOffsets;
    this.endOffsets = endOffsets;
    this.version = 0;
  }

  /**
   * If this represents a range of offsets, the starting offsets of the range.
   */
  public Optional<PushOffsetVector> getStartOffsets() {
    return startOffsets;
  }

  /**
   * The offsets associated with this set of rows, or the end of a range, if start is also set.
   */
  public PushOffsetVector getEndOffsets() {
    return endOffsets;
  }

  public String token() {
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(this);
      return Base64.getEncoder().encodeToString(bytes);
    } catch (Exception e) {
      throw new KsqlException("Couldn't encode push offset range token", e);
    }
  }

  public static PushOffsetRange fromToken(final String token) {
    try {
      final byte[] bytes = Base64.getDecoder().decode(token);
      return OBJECT_MAPPER.readValue(bytes, PushOffsetRange.class);
    } catch (Exception e) {
      throw new KsqlException("Couldn't decode push offset range token", e);
    }
  }

}
