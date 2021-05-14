/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.support.metrics.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UuidTest {
  @Test
  public void generatesValidType4UUID() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String generatedUUID = uuid.getUuid();

    // Then
    assertTrue(generatedUUID.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
  }

  @Test
  public void stringRepresentationIsIdenticalToGeneratedUUID() {
    // Given
    Uuid uuid = new Uuid();

    // When/Then
    assertEquals(uuid.getUuid(), uuid.toString());
  }

  @Test
  public void uuidDoesNotChangeBetweenRuns() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String firstUuid = uuid.getUuid();
    String secondUuid = uuid.getUuid();

    // Then
    assertEquals(secondUuid, firstUuid);
  }
}
