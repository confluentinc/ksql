/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner.plan;

/**
 * Means nothing could be extracted about the keys.  Either the expression is too complex or
 * doesn't make reference to keys, so we consider there to be no bound key constraint.
 * Obviously, we'll still have to evaluate the expression for correctness on some overly
 * permissive set of rows (e.g. table scan), but we cannot use a key as a hint for fetching the
 * data.
 */
public class NonKeyConstraint implements LookupConstraint {
}
