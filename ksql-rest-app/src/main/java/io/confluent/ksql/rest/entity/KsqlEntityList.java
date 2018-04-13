/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility class to prevent type erasure from stripping annotation information from KsqlEntity
 * instances in a list
 */
@JsonSubTypes({})
public class KsqlEntityList extends ArrayList<KsqlEntity> {
  public KsqlEntityList() {
  }

  public KsqlEntityList(Collection<? extends KsqlEntity> c) {
    super(c);
  }
}
