package io.confluent.ksql.physical.pull.operators;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.physical.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.common.operators.UnaryPhysicalOperator;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryLimitNode;
import java.util.List;
import java.util.Objects;

public class LimitOperator extends AbstractPhysicalOperator
        implements UnaryPhysicalOperator {

    private AbstractPhysicalOperator child;
    private final QueryLimitNode logicalNode;
    private final ProcessingLogger logger;
    private final int limit;
    private int rowsReturned;

    public LimitOperator(
            final QueryLimitNode logicalNode,
            final ProcessingLogger logger
    ) {
        this(logicalNode, logger,
                logicalNode.getLimit()
        );
    }

    @VisibleForTesting
    LimitOperator(
            final QueryLimitNode logicalNode,
            final ProcessingLogger logger,
            final int limit
    ) {
        this.logger = Objects.requireNonNull(logger, "logger");
        this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
        this.limit = limit;
        this.rowsReturned = 0;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Object next() {
        if (rowsReturned >= limit) {
            return null;
        }
        Object row = child.next();
        if (row == null) {
            return null;
        }
        rowsReturned += 1;
        return row;
    }

    @Override
    public void close() {
        child.close();
    }

    @Override
    public PlanNode getLogicalNode() {
        return logicalNode;
    }

    @Override
    public void addChild(AbstractPhysicalOperator child) {
        if (this.child != null) {
            throw new UnsupportedOperationException("The limit operator already has a child.");
        }
        Objects.requireNonNull(child, "child");
        this.child = child;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public AbstractPhysicalOperator getChild() {
        return child;
    }

    @Override
    public AbstractPhysicalOperator getChild(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<AbstractPhysicalOperator> getChildren() {
        throw new UnsupportedOperationException();
    }
}
