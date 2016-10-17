package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetProperty extends Statement {

    private final Optional<String> propertyName;
    private final String propertyValue;

    public SetProperty(Optional<String> propertyName, String propertyValue)
    {
        this(Optional.empty(), propertyName, propertyValue);
    }

    public SetProperty(NodeLocation location, Optional<String> propertyName, String propertyValue)
    {
        this(Optional.of(location), propertyName, propertyValue);
    }

    private SetProperty(Optional<NodeLocation> location, Optional<String> propertyName, String propertyValue)
    {
        super(location);
        requireNonNull(propertyName, "catalog is null");
        requireNonNull(propertyValue, "propertyValue is null");
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
    }


    @Override
    public int hashCode()
    {
        return Objects.hash(propertyName, propertyValue);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SetProperty setProperty = (SetProperty) o;

        if (!propertyName.equals(setProperty.propertyName)) {
            return false;
        }
        if (!propertyValue.equals(setProperty.propertyValue)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }
}
