/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.util;

import java.util.HashMap;
import java.util.Map;

import com.alipay.sofa.jraft.Status;

public enum HgRaftError {
    UNKNOWN(-1, "unknown"),
    OK(0, "OK"),
    NOT_LEADER(20000, "This partition is not leader"),
    WAIT_LEADER_TIMEOUT(20001, "Waiting for leader timeout"),
    NOT_LOCAL(20002, "This partition is not local"),
    CLUSTER_NOT_READY(20003, "The cluster is not ready, please check active stores number!"),

    TASK_CONTINUE(21000, "Task is continue"),
    TASK_ERROR(21001, "Task is error, need to retry"),

    // Temporal apply-time business rejections (Slice 1 L1b fix). These are NOT
    // partition faults: they represent a legitimate, deterministic rejection of
    // a temporal mutation (interval conflict / unsupported wire version) decided
    // identically on every replica by the state machine. Carrying a dedicated
    // code keeps them out of the getErrorResponse() default branch
    // ("Unmatchable errorStatus") and lets the client distinguish a lost
    // conflict race from a genuine unknown failure.
    TEMPORAL_CONFLICT(22000, "temporal fact interval conflict"),
    TEMPORAL_UNSUPPORTED_VERSION(22001, "temporal unsupported wire version"),
    TEMPORAL_CLOSED_INTERVAL_CONFLICT(22002, "temporal closed interval conflict"),
    END(30000, "HgStore error is end");

    private static final Map<Integer, HgRaftError> RAFT_ERROR_MAP = new HashMap<>();

    static {
        for (final HgRaftError error : HgRaftError.values()) {
            RAFT_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    private final int value;

    private final String msg;

    HgRaftError(final int value, final String msg) {
        this.value = value;
        this.msg = msg;
    }

    public static HgRaftError forNumber(final int value) {
        return RAFT_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public final int getNumber() {
        return this.value;
    }

    public final String getMsg() {
        return this.msg;
    }

    public Status toStatus() {
        return new Status(value, msg);
    }
}
