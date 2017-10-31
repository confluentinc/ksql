/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.datagen;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SessionManager {

  private int maxSessionDurationSeconds = 30;
  private int maxSessions = 5;

  public void setMaxSessionDurationSeconds(int maxSessionDurationSeconds) {
    this.maxSessionDurationSeconds = maxSessionDurationSeconds;
  }

  public void setMaxSessions(int maxSessions) {
    this.maxSessions = maxSessions;
  }

  public boolean isActive(String sessionId) {
    return activeSessions.containsKey(sessionId);
  }

  public boolean isExpired(String sessionId) {
    if (activeSessions.containsKey(sessionId)) {
      SessionObject sessionObject = activeSessions.get(sessionId);
      return sessionObject.isExpired();
    }
    return expiredSessions.containsKey(sessionId);
  }

  public boolean isActiveAndExpire(String sessionId) {
    boolean b = isActive(sessionId);
    if (b) {
      SessionObject sessionObject = activeSessions.get(sessionId);
      if (sessionObject.isExpired()) {
        System.out.println("***Expired:" + sessionId);
        SessionObject removed = activeSessions.remove(sessionId);
        expiredSessions.put(sessionId, removed);
        return false;
      }

    }
    return b;
  }


  public void newSession(String sessionToken) {
    if (activeSessions.containsKey(sessionToken)) {
      throw new RuntimeException("Session" + sessionToken + " already exists");
    }
    activeSessions.putIfAbsent(sessionToken, new SessionObject(maxSessionDurationSeconds));
  }

  public boolean isExpiredSession(String sessionId) {
    return expiredSessions.containsKey(sessionId);
  }

  public String recycleOldestExpired() {
    Map.Entry<String, SessionObject> oldest = null;
    for (Map.Entry<String, SessionObject> entry : expiredSessions.entrySet()) {
      if (oldest == null || (entry.getValue().created < oldest.getValue().created)) {
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
    int randomIndex = (int) (Math.random() * activeSessions.size());
    return new ArrayList<String>(activeSessions.keySet()).get(randomIndex);
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

  public String getToken(String s) {
    if (activeSessions.containsKey(s)) {
      return s;
    }

    // MaxedOut = then reuse active key
    if (activeSessions.size() == maxSessions) {
      int randomIndex = (int) (Math.random() * activeSessions.size());
      return new ArrayList<String>(activeSessions.keySet()).get(randomIndex);
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

    public SessionObject(int duration) {
      this.sessionDurationSecs = duration;
    }

    long created = System.currentTimeMillis();
    private long sessionDurationSecs = 300;

    public boolean isExpired() {
      return (System.currentTimeMillis() - created) / 1000 > sessionDurationSecs;
    }

    @Override
    public String toString() {
      return "Session:" + new Date(created).toString();
    }
  }

  Map<String, SessionObject> expiredSessions = new HashMap<String, SessionObject>();
  Map<String, SessionObject> activeSessions = new HashMap<String, SessionObject>();

}
