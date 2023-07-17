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

package io.confluent.ksql.rest.server.restore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

public class RestoreOptionsTest {
  private static final String[] REQUIRED_ARGS =
      new String[]{"--config-file", "config file", "backup file"};

  @Test
  public void shouldUseDefaultValuesOnNonRequiredParameters() throws IOException {
    // When
    RestoreOptions restoreOptions = RestoreOptions.parse(REQUIRED_ARGS);

    // Then
    assertThat(restoreOptions.isAutomaticYes(), is(false));
    assertThat(restoreOptions.getBackupFile().getPath(), is("backup file"));
    assertThat(restoreOptions.getConfigFile().getPath(), is("config file"));
  }

  @Test
  public void shouldSetAutomaticYesOptionIfSupplied() throws IOException {
    // When
    RestoreOptions restoreOptions = RestoreOptions.parse(ArrayUtils.addAll(REQUIRED_ARGS, "--yes"));

    // Then
    assertThat(restoreOptions.isAutomaticYes(), is(true));
  }
}