/*
 * Copyright 2020 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class KsqlMediaTypeTest {

  public static class Common {

    @Test
    public void shouldHaveUpToDateLatest() {
      final int maxVersion = Arrays.stream(KsqlMediaType.values())
          .mapToInt(KsqlMediaType::getVersion)
          .max()
          .orElseThrow(IllegalStateException::new);

      assertThat(KsqlMediaType.LATEST_FORMAT.getVersion(), is(maxVersion));
    }
  }

  @RunWith(Parameterized.class)
  public static class PerValue {

    @Parameterized.Parameters(name = "{0}")
    public static KsqlMediaType[] getMediaTypes() {
      return KsqlMediaType.values();
    }

    @Parameterized.Parameter
    public KsqlMediaType mediaType;

    @Test
    public void shouldParse() {
      assertThat(KsqlMediaType.parse(mediaType.mediaType()), is(mediaType));
    }

    @Test
    public void shouldGetValueOf() {
      final String format = mediaType.mediaType().split("\\+")[1];
      assertThat(KsqlMediaType.valueOf(format, mediaType.getVersion()), is(mediaType));
    }
  }
}