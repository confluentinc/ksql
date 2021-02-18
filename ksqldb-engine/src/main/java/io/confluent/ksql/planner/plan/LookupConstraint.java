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
 * The top level interface which represents information extracted from a given disjunct
 * from the DNF representation of the where clause expression.  Generally, implementing classes will
 * be used as hints from the logical planning layer to the physical layer about how to fetch the
 * associated data.  Namely, use of keys.
 */
public interface LookupConstraint {
}
