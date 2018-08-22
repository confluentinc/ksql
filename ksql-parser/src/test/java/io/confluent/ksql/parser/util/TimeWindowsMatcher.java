/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.parser.util;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * Hopefully temporary matcher to work around the fact that TimeWindows has had its equals and
 * hashcode removed.
 */
public final class TimeWindowsMatcher {

  public static TimeWindows timeWindows(final TimeWindows expected){
    EasyMock.reportMatcher(new IArgumentMatcher() {
      @Override
      public boolean matches(final Object argument) {
        if (!(argument instanceof TimeWindows)) {
          return false;
        }

        final TimeWindows actual = (TimeWindows)argument;
        return actual.equals(expected);
      }

      @Override
      public void appendTo(final StringBuffer buffer) {
        buffer.append("timeWindow(\"").append(expected).append("\")");
      }
    });
    return null;
  }

  private TimeWindowsMatcher(){}
}
