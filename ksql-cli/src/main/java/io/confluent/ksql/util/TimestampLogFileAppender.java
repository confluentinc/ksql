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
 **/

package io.confluent.ksql.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.FileAppender;

public class TimestampLogFileAppender extends FileAppender {

  @Override
  public void setFile(final String fileName) {
    if (fileName.contains("%timestamp")) {
      final Date d = new Date();
      final SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss");
      super.setFile(fileName.replaceAll("%timestamp", format.format(d)));
    } else {
      super.setFile(fileName);
    }
  }

}