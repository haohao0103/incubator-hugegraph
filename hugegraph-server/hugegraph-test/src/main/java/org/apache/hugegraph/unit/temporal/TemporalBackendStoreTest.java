/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.temporal;

import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.TemporalBackendStore;
import org.apache.hugegraph.temporal.store.TemporalErrorCode;
import org.apache.hugegraph.temporal.store.TemporalStoreException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Capability contract of {@link TemporalBackendStore}.
 *
 * A backend opts in by implementing the sub-interface; every other backend
 * keeps its ordinary path unchanged and temporal writes must receive an
 * explicit {@code TEMPORAL_UNSUPPORTED_VERSION}, never a silent scan or
 * downgrade (design ruling §1.4 / §5, Phase 0 §2.5).
 */
public class TemporalBackendStoreTest {

    @Test
    public void shouldRejectNonTemporalStore() {
        BackendStore plain = mock(BackendStore.class);
        when(plain.store()).thenReturn("memory");

        assertFalse(TemporalBackendStore.isTemporal(plain));
        try {
            TemporalBackendStore.require(plain);
            fail("Expected TEMPORAL_UNSUPPORTED_VERSION for a non-temporal store");
        } catch (TemporalStoreException e) {
            assertEquals(TemporalErrorCode.TEMPORAL_UNSUPPORTED_VERSION, e.code());
        }
    }

    @Test
    public void shouldAcceptTemporalStore() {
        TemporalBackendStore temporal = mock(TemporalBackendStore.class);

        assertTrue(TemporalBackendStore.isTemporal(temporal));
        assertSame(temporal, TemporalBackendStore.require(temporal));
    }

    @Test
    public void shouldRejectNullStore() {
        assertFalse(TemporalBackendStore.isTemporal(null));
        try {
            TemporalBackendStore.require(null);
            fail("Expected TEMPORAL_UNSUPPORTED_VERSION for a null store");
        } catch (TemporalStoreException e) {
            assertEquals(TemporalErrorCode.TEMPORAL_UNSUPPORTED_VERSION, e.code());
        }
    }
}
