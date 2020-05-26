package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupOffsets {
    private final String groupId;
    private final String kafkaTopic;
    private final List<ConsumerOffset> offsets;

    @JsonCreator
    public ConsumerGroupOffsets(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("offsets") List<ConsumerOffset> offsets
    ) {
        this.groupId = groupId;
        this.kafkaTopic = kafkaTopic;
        this.offsets = offsets;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public List<ConsumerOffset> getOffsets() {
        return offsets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerGroupOffsets that = (ConsumerGroupOffsets) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(kafkaTopic, that.kafkaTopic) &&
            Objects.equals(offsets, that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, kafkaTopic, offsets);
    }
}
