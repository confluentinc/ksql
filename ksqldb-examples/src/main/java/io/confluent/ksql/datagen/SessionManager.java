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

package io.confluent.ksql.datagen;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SessionManager {

  private Duration maxSessionDuration = Duration.ofSeconds(30);
  private int maxSessions = 5;

  public void setMaxSessionDurationSeconds(final int maxSessionDurationSeconds) {
    this.maxSessionDuration = Duration.ofSeconds(maxSessionDurationSeconds);
  }

  public void setMaxSessionDuration(final Duration duration) {
    this.maxSessionDuration = Objects.requireNonNull(duration, "duration");
  }

  public void setMaxSessions(final int maxSessions) {
    this.maxSessions = maxSessions;
  }

  public boolean isActive(final String sessionId) {
    return activeSessions.containsKey(sessionId);
  }

  public boolean isExpired(final String sessionId) {
    if (activeSessions.containsKey(sessionId)) {
      final SessionObject sessionObject = activeSessions.get(sessionId);
      return sessionObject.isExpired();
    }
    return expiredSessions.containsKey(sessionId);
  }

  public boolean isActiveAndExpire(final String sessionId) {
    final boolean b = isActive(sessionId);
    if (b) {
      final SessionObject sessionObject = activeSessions.get(sessionId);
      if (sessionObject.isExpired()) {
        final SessionObject removed = activeSessions.remove(sessionId);
        expiredSessions.put(sessionId, removed);
        return false;
      }

    }
    return b;
  }


  public void newSession(final String sessionToken) {
    if (activeSessions.containsKey(sessionToken)) {
      throw new RuntimeException("Session" + sessionToken + " already exists");
    }
    activeSessions.putIfAbsent(sessionToken, new SessionObject(maxSessionDuration));
  }

  public boolean isExpiredSession(final String sessionId) {
    return expiredSessions.containsKey(sessionId);
  }

  public String recycleOldestExpired() {
    Map.Entry<String, SessionObject> oldest = null;
    for (final Map.Entry<String, SessionObject> entry : expiredSessions.entrySet()) {
      if (oldest == null || (entry.getValue().createdMs < oldest.getValue().createdMs)) {
        oldest = entry;
      }
    }
    if (oldest != null) {
      expiredSessions.remove(oldest.getKey());
      return oldest.getKey();
    }
    return null;
  }

  public String getRandomActiveToken() {
    final int randomIndex = (int) (Math.random() * activeSessions.size());
    return new ArrayList<>(activeSessions.keySet()).get(randomIndex);
  }

  public String getActiveSessionThatHasExpired() {
    return activeSessions.entrySet().stream()
        .filter(entry -> entry.getValue().isExpired())
        .findFirst()
        .map(entry -> {
          expiredSessions.put(entry.getKey(), activeSessions.remove(entry.getKey()));
          return entry.getKey();
        }).orElse(null);

  }

  public String getToken(final String s) {
    if (activeSessions.containsKey(s)) {
      return s;
    }

    // MaxedOut = then reuse active key
    if (activeSessions.size() == maxSessions) {
      final int randomIndex = (int) (Math.random() * activeSessions.size());
      return new ArrayList<>(activeSessions.keySet()).get(randomIndex);
    }

    // we have a new sessionId,  =- if it is expired then we will allow reuse
    if (expiredSessions.containsKey(s)) {
      expiredSessions.remove(s);
      return s;
    }

    return s;
  }

  public int getActiveSessionCount() {
    return activeSessions.size();
  }

  public String randomActiveSession() {
    if (activeSessions.size() == 0) {
      return null;
    }
    return activeSessions.keySet().iterator().next();
  }

  public int getMaxSessions() {
    return maxSessions;
  }

  public static class SessionObject {

    private final long createdMs = System.currentTimeMillis();
    private final Duration sessionDuration;

    public SessionObject(final Duration duration) {
      this.sessionDuration = Duration.ofMillis(duration.toMillis());
    }

    public boolean isExpired() {
      return age().toMillis() > sessionDuration.toMillis();
    }

    private Duration age() {
      return Duration.ofMillis(System.currentTimeMillis() - createdMs);
    }

    @Override
    public String toString() {
      return "Session:" + new Date(createdMs);
    }
  }

  Map<String, SessionObject> expiredSessions = new HashMap<>();
  Map<String, SessionObject> activeSessions = new HashMap<>();

}
