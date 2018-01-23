/*
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
 */

package io.confluent.ksql.version.metrics;


import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import io.confluent.support.metrics.submitters.ResponseHandler;

public class KsqlVersionCheckerResponseHandler implements ResponseHandler {

  private static final Logger log = LoggerFactory.getLogger(KsqlVersionChecker.class);

  @Override
  public void handle(HttpResponse response) {
    int statusCode = response.getStatusLine().getStatusCode();

    try(InputStream inputStream = response.getEntity().getContent();
        BufferedReader br = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      if (statusCode == HttpStatus.SC_OK && inputStream != null) {
        StringBuilder content = new StringBuilder();
        String line;
        while (null != (line = br.readLine())) {
          content.append(line);
        }
        if (content.length() > 0) {
          log.warn(content.toString());
        }
      }
    } catch (IOException e) {
      log.error("Error while parsing the Version check response ", e);
    }
  }
}
