/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.store.temporal;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Temporal write feature flag (design ruling §5.1).
 *
 * Temporal writes may only be enabled after all Store nodes have been upgraded
 * to a version supporting the registered wire op + codec version and the
 * capability handshake has succeeded. While the flag is off, the temporal RPC
 * entry point must reject requests explicitly instead of submitting them.
 */
public final class TemporalFeatureFlag {

    /**
     * System property read once at class initialization so Store nodes can be
     * started with -Dhugegraph.temporal.enabled=true after all nodes support
     * the registered wire op + codec version and the capability handshake
     * succeeded. Test environments may also flip enable()/disable() directly.
     */
    private static final AtomicBoolean ENABLED = new AtomicBoolean(
            Boolean.parseBoolean(
                    System.getProperty("hugegraph.temporal.enabled", "false")));

    private TemporalFeatureFlag() {
    }

    public static boolean isEnabled() {
        return ENABLED.get();
    }

    /**
     * Enable temporal writes. Production must gate this behind an all-nodes
     * upgrade + capability handshake. Test environments may call it directly
     * to open the temporal RPC entry point.
     */
    public static void enable() {
        ENABLED.set(true);
    }

    public static void disable() {
        ENABLED.set(false);
    }
}
