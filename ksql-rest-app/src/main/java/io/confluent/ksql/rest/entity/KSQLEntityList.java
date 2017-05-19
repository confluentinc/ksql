/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import java.util.ArrayList;

/**
 * Utility class to prevent type erasure from stripping annotation information from KSQLEntity instances in a list
 */
public class KSQLEntityList extends ArrayList<KSQLEntity> {
}
