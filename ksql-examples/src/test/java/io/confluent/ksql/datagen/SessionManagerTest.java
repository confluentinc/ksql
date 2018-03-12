/**
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

package io.confluent.ksql.datagen;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class SessionManagerTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void sessionShouldForceTokenReUseWhenMaxedOut() throws InterruptedException {

        SessionManager sm = new SessionManager();

        sm.setMaxSessionDurationSeconds(1);
        sm.setMaxSessions(5);

        Set<String> expectedSet = new HashSet<String>(Arrays.asList( "0", "1", "2", "3", "4"));

        /**
         * FillActiveSessions
         */
        for (int i = 0; i < 5; i ++) {

            String token = sm.getToken(Integer.toString(i));
            assertTrue("Got Token:" + token, expectedSet.contains(token));
            sm.newSession(token);
        }
    }

    @Test
    public void sessionShouldExpireAndReuse() throws InterruptedException {

        SessionManager sm = new SessionManager();

        sm.setMaxSessionDurationSeconds(1);
        sm.setMaxSessions(5);

        /**
         * FillActiveSessions
         */
        for (int i = 0; i < 5; i ++) {
            sm.newSession(Integer.toString(i));
        }

        /**
         * Expire them all
         */
        Thread.sleep(2 * 1000);


        /**
         *  reuse tokens
         */
        for (int i = 0; i < 5; i ++) {

            // force expiration & check
            boolean active = sm.isActiveAndExpire(Integer.toString(i));
            assertFalse(active);

            // want to re-use the oldest-existing session if we havent seen this before
            boolean isRecycled = sm.isExpiredSession(Integer.toString(i));

            assertTrue("Should be recycled session: " + i, isRecycled);

            String oldest = sm.recycleOldestExpired();

            assertNotNull(oldest);

            sm.newSession(Integer.toString(i));
        }

    }



    @Test
    public void isReturningOldestExpiredSession() throws InterruptedException {

        SessionManager sm = new SessionManager();
        sm.setMaxSessionDurationSeconds(1);
        sm.newSession("1");
        Thread.sleep(200);
        sm.newSession("2");
        Thread.sleep(2500);

        sm.isActiveAndExpire("1");
        sm.isActiveAndExpire("2");


        assertEquals("1", sm.recycleOldestExpired());

    }




    @Test
    public void isActiveThenAddSession() throws InterruptedException {

        SessionManager sm = new SessionManager();
        String sessionToken = "not-active";
        assertFalse(sm.isActiveAndExpire(sessionToken));
        sm.newSession(sessionToken);
        assertTrue(sm.isActiveAndExpire(sessionToken));

    }

    @Test
    public void doesSessionExpire() throws InterruptedException {

        SessionManager sm = new SessionManager();
        sm.setMaxSessionDurationSeconds(1);
        String sessionToken = "active";
        sm.newSession(sessionToken);
        assertTrue(sm.isActiveAndExpire(sessionToken));

        Thread.sleep(2 * 1000);

        assertFalse(sm.isActiveAndExpire(sessionToken));
    }


}