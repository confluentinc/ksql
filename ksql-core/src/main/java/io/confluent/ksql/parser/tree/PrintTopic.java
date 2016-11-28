package io.confluent.ksql.parser.tree;


import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PrintTopic extends Statement
{
    private final QualifiedName topic;

    private final LongLiteral intervalValue;

    public PrintTopic(QualifiedName topic, LongLiteral intervalValue)
    {
        this(Optional.empty(), topic, intervalValue);
    }

    public PrintTopic(NodeLocation location, QualifiedName topic, LongLiteral intervalValue)
    {
        this(Optional.of(location), topic, intervalValue);
    }

    private PrintTopic(Optional<NodeLocation> location, QualifiedName topic, LongLiteral intervalValue)
    {
        super(location);
        this.topic = requireNonNull(topic, "table is null");
        this.intervalValue = intervalValue;
    }

    public QualifiedName getTopic() {
        return topic;
    }

    public LongLiteral getIntervalValue() {
        return intervalValue;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(topic);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PrintTopic o = (PrintTopic) obj;
        return Objects.equals(topic, o.topic);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("topic", topic)
                .toString();
    }
}
