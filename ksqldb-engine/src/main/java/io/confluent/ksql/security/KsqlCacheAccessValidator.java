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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.common.acl.AclOperation;

/**
 * An implementation of {@link KsqlAccessValidator} that provides authorization checks
 * from a memory cache.
 */
@ThreadSafe
public class KsqlCacheAccessValidator implements KsqlAccessValidator {
  private static final boolean ALLOW_ACCESS = true;

  static class CacheKey {
    private static final String UNKNOWN_USER = "";

    private final KsqlSecurityContext securityContext;
    private final String topicName;
    private final AclOperation operation;

    CacheKey(
        final KsqlSecurityContext securityContext,
        final String topicName,
        final AclOperation operation
    ) {
      this.securityContext = securityContext;
      this.topicName = topicName;
      this.operation = operation;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == null || !(o instanceof CacheKey)) {
        return false;
      }

      final CacheKey other = (CacheKey)o;
      return getUserName(securityContext).equals(getUserName(other.securityContext))
          && topicName.equals(other.topicName)
          && operation.code() == other.operation.code();
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          getUserName(securityContext),
          topicName,
          operation.code()
      );
    }

    private String getUserName(final KsqlSecurityContext securityContext) {
      return (securityContext.getUserPrincipal().isPresent())
          ? securityContext.getUserPrincipal().get().getName()
          : UNKNOWN_USER;
    }
  }

  static class CacheValue {
    private final boolean allowAccess;
    private final Optional<RuntimeException> denialReason;

    CacheValue(final boolean allowAccess, final Optional<RuntimeException> denialReason) {
      this.allowAccess = allowAccess;
      this.denialReason = denialReason;
    }
  }

  private final LoadingCache<CacheKey, CacheValue> cache;
  private final KsqlAccessValidator backendValidator;

  public KsqlCacheAccessValidator(
      final KsqlConfig ksqlConfig,
      final KsqlAccessValidator backendValidator
  ) {
    this(ksqlConfig, backendValidator, Ticker.systemTicker());
  }

  @VisibleForTesting
  KsqlCacheAccessValidator(
      final KsqlConfig ksqlConfig,
      final KsqlAccessValidator backendValidator,
      final Ticker cacheTicker
  ) {
    this.backendValidator = backendValidator;

    final long expiryTime = ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS);
    final long maxEntries = ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES);

    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(expiryTime, TimeUnit.SECONDS)
        .maximumSize(maxEntries)
        .ticker(cacheTicker)
        .build(buildCacheLoader());
  }

  private CacheLoader<CacheKey, CacheValue> buildCacheLoader() {
    return new CacheLoader<CacheKey, CacheValue>() {
      @Override
      public CacheValue load(final CacheKey cacheKey) {
        try {
          backendValidator.checkAccess(
              cacheKey.securityContext,
              cacheKey.topicName,
              cacheKey.operation
          );
        } catch (KsqlTopicAuthorizationException e) {
          return new CacheValue(!ALLOW_ACCESS, Optional.of(e));
        }

        return new CacheValue(ALLOW_ACCESS, Optional.empty());
      }
    };
  }

  @Override
  public void checkAccess(
      final KsqlSecurityContext securityContext,
      final String topicName,
      final AclOperation operation
  ) {
    final CacheKey cacheKey = new CacheKey(securityContext, topicName, operation);
    final CacheValue cacheValue = cache.getUnchecked(cacheKey);
    if (!cacheValue.allowAccess) {
      throw cacheValue.denialReason.get();
    }
  }
}
