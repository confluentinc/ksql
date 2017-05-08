/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.LinkedList;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StackableAstVisitor<R, C>
    extends AstVisitor<R, StackableAstVisitor.StackableAstVisitorContext<C>> {

  public R process(Node node, StackableAstVisitorContext<C> context) {
    context.push(node);
    try {
      return node.accept(this, context);
    } finally {
      context.pop();
    }
  }

  public static class StackableAstVisitorContext<C> {

    private final LinkedList<Node> stack = new LinkedList<>();
    private final C context;

    public StackableAstVisitorContext(C context) {
      this.context = requireNonNull(context, "context is null");
    }

    public C getContext() {
      return context;
    }

    private void pop() {
      stack.pop();
    }

    void push(Node node) {
      stack.push(node);
    }

    public Optional<Node> getPreviousNode() {
      if (stack.size() > 1) {
        return Optional.of(stack.get(1));
      }
      return Optional.empty();
    }
  }
}
