/*
 * Copyright 2022 Confluent Inc.
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

package io.vertx.core.http.impl;

import io.vertx.core.Handler;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.OnMethodEnter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpEventHandlerAdvice {

  public static final Logger LOG = LoggerFactory.getLogger(HttpEventHandlerAdvice.class);

  @OnMethodEnter
  public static void foo(
      @Advice.Argument(0) Handler<Void> handler,
      @Advice.FieldValue("endHandler") Handler<Void> endHandler
  ) {
    final String handlerClazz = handler == null
        ? "null" : handler.getClass().getSimpleName();
    final String endHandlerClazz = endHandler == null
        ? "null" : endHandler.getClass().getSimpleName();

    if (handlerClazz.contains("RecordParserImpl") || endHandlerClazz.contains("RecordParserImpl")) {
      LOG.info(
          "Replacing handler class {} with handler of class {} at trace {}",
          endHandlerClazz,
          handlerClazz,
          ExceptionUtils.getStackTrace(new Throwable())
      );
    }
  }

}
