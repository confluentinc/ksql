/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.version.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.support.metrics.submitters.ResponseHandler;
import java.io.IOException;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KsqlVersionCheckerResponseHandler implements ResponseHandler {

  private static final Logger DEFAULT_LOGGER = LogManager.getLogger(KsqlVersionChecker.class);

  private final Logger log;

  KsqlVersionCheckerResponseHandler() {
    this(DEFAULT_LOGGER);
  }

  @VisibleForTesting
  KsqlVersionCheckerResponseHandler(final Logger log) {
    this.log = log;
  }

  @Override
  public void handle(final ClassicHttpResponse response) {
    final int statusCode = response.getCode();
    try {
      if (statusCode == HttpStatus.SC_OK && response.getEntity().getContent() != null) {

        final String content = EntityUtils.toString(response.getEntity());
        if (content.length() > 0) {
          log.warn(content);
        }
      }
    } catch (final ParseException | IOException e) {
      log.error("Error while parsing the Version check response ", e);
    }
  }
}
