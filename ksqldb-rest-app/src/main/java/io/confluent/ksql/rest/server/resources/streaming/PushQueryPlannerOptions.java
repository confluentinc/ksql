package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.planner.QueryPlannerOptions;

public class PushQueryPlannerOptions implements QueryPlannerOptions {

  @Override
  public boolean getTableScansEnabled() {
    return true;
  }

  @Override
  public boolean getInterpreterEnabled() {
    return true;
  }
}
