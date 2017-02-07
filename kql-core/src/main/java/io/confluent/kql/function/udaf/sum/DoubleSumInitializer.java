/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function.udaf.sum;

import io.confluent.kql.physical.GenericRow;

import org.apache.kafka.streams.kstream.Initializer;

public class DoubleSumInitializer implements Initializer<GenericRow> {

  GenericRow resultGenericRow;
  int aggColumnIndexInResult;
  Object aggColumnInitialValueInResult;

  public DoubleSumInitializer(GenericRow resultGenericRow, int aggColumnIndexInResult,
                              Object aggColumnInitialValueInResult) {
    this.resultGenericRow = resultGenericRow;
    this.aggColumnIndexInResult = aggColumnIndexInResult;
    this.aggColumnInitialValueInResult = aggColumnInitialValueInResult;
  }

  @Override
  public GenericRow apply() {
    resultGenericRow.getColumns().set(aggColumnIndexInResult, aggColumnInitialValueInResult);
    return resultGenericRow;
  }
}