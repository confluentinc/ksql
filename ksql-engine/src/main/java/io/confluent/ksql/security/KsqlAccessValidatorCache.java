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

package io.confluent.ksql.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.util.KsqlConfig;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.common.acl.AclOperation;

/**
 * An implementation of {@link KsqlAccessValidator} that provides authorization checks
 * from a Cache.
 */
@ThreadSafe
public class KsqlAccessValidatorCache implements KsqlAccessValidator {
  private static final boolean ALLOWED = true;
  private static final boolean DENIED = false;

  static class CacheKey {
    private static final String UNKNOWN_USER = "";

    private final KsqlSecurityContext securityContext;
    private final String topicName;
    private final AclOperation operation;

    CacheKey(KsqlSecurityContext securityContext, String topicName, AclOperation operation) {
      this.securityContext = securityContext;
      this.topicName = topicName;
      this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof CacheKey)) {
        return false;
      }

      CacheKey other = (CacheKey)o;
      return getuserName(securityContext).equals(getuserName(other.securityContext))
          && topicName.equals(other.topicName)
          && operation.code() == other.operation.code();
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          getuserName(securityContext),
          topicName,
          operation.code()
      );
    }

    private String getuserName(KsqlSecurityContext securityContext) {
      return (securityContext.getUserPrincipal().isPresent())
          ? securityContext.getUserPrincipal().get().getName()
          : UNKNOWN_USER;
    }
  }

  static class CacheValue {
    private final boolean response;
    private final RuntimeException exception;

    CacheValue(boolean response, RuntimeException exception) {
      this.response = response;
      this.exception = exception;
    }
  }

  private final LoadingCache<CacheKey, CacheValue> cache;
  private final KsqlAccessValidator backendValidator;

  public KsqlAccessValidatorCache(KsqlConfig ksqlConfig, KsqlAccessValidator backendValidator) {
    this(ksqlConfig, backendValidator, Ticker.systemTicker());
  }

  @VisibleForTesting
  KsqlAccessValidatorCache(
      KsqlConfig ksqlConfig,
      KsqlAccessValidator backendValidator,
      Ticker cacheTicker
  ) {
    this.backendValidator = backendValidator;

    long expiryTime = ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME);
    long maxEntries = ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES);

    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(expiryTime, TimeUnit.SECONDS)
        .maximumSize(maxEntries)
        .ticker(cacheTicker)
        .build(buildCacheLoader());
  }

  private CacheLoader<CacheKey, CacheValue> buildCacheLoader() {
    return new CacheLoader<CacheKey, CacheValue>() {
      @Override
      public CacheValue load(CacheKey cacheKey) {
        try {
          backendValidator.checkAccess(
              cacheKey.securityContext,
              cacheKey.topicName,
              cacheKey.operation
          );
        } catch (KsqlTopicAuthorizationException e) {
          return new CacheValue(DENIED, e);
        }

        return new CacheValue(ALLOWED, null);
      }
    };
  }

  @Override
  public void checkAccess(
      KsqlSecurityContext securityContext,
      String topicName,
      AclOperation operation
  ) {
    CacheKey cacheKey = new CacheKey(securityContext, topicName, operation);
    CacheValue cacheValue = cache.getUnchecked(cacheKey);
    if (cacheValue.response == DENIED) {
      throw cacheValue.exception;
    }
  }
}
