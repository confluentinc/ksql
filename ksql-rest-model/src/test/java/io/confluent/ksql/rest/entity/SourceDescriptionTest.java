/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.google.common.testing.EqualsTester;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class SourceDescriptionTest {

    private static final String SOME_STRING = "some string";
    private static final int SOME_INT = 3;
    private static final boolean SOME_BOOL = true;
 
    @Mock
    private RunningQuery runningQuery;

    @Mock
    private FieldInfo fieldInfo;

    @Test
    public void shouldImplementHashCodeAndEqualsProperty() {
      new EqualsTester()
          .addEqualityGroup(
              new SourceDescription(
                  SOME_STRING, Collections.singletonList(runningQuery), Collections.singletonList(runningQuery),
                  Collections.singletonList(fieldInfo), SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                  SOME_STRING, SOME_BOOL, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT, SOME_STRING),
              new SourceDescription(
                  SOME_STRING, Collections.singletonList(runningQuery), Collections.singletonList(runningQuery),
                  Collections.singletonList(fieldInfo), SOME_STRING, SOME_STRING, SOME_STRING, SOME_STRING,
                  SOME_STRING, SOME_BOOL, SOME_STRING, SOME_STRING, SOME_INT, SOME_INT, SOME_STRING)
          )
          .testEquals();
    }
  }
