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

import io.vertx.core.http.HttpConnection;
import java.lang.reflect.Field;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.OnMethodEnter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientResponseAdvice {

  public static final Logger LOG = LoggerFactory.getLogger(HttpClientResponseAdvice.class);

  @OnMethodEnter
  public static void foo(
      @Advice.FieldValue("conn") HttpConnection conn,
      @Advice.FieldValue("eventHandler") HttpEventHandler eventHandler
  ) throws Exception {
    final Field endHandlerF = HttpEventHandler.class.getDeclaredField("endHandler");
    endHandlerF.setAccessible(true);
    final Object endHandler = endHandlerF.get(eventHandler);

    LOG.info(
        "{} HttpClientResponse handleEnd() with handler object {} class {}. Stack trace: {}",
        ((Http1xClientConnection) conn).channelHandlerContext(),
        endHandler,
        endHandler != null ? endHandler.getClass() : null,
        ExceptionUtils.getStackTrace(new Throwable())
    );
  }

}
