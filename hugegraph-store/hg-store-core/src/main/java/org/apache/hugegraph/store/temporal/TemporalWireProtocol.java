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

/**
 * Versioned registration of the temporal Raft task wire protocol.
 *
 * Design ruling §5.1: logical task name, wire op, task codec version and schema
 * version must be registered as a group in Store capability metadata. Old nodes
 * that do not recognize the wire op must fail explicitly with
 * TEMPORAL_UNSUPPORTED_VERSION, never apply the entry as an ordinary task.
 */
public final class TemporalWireProtocol {

    /** Logical task name. */
    public static final String LOGICAL_NAME = "temporal_mutation";

    /** Wire op byte carried as the first byte of the Raft log entry. */
    public static final byte WIRE_OP = TemporalMutationHandler.TEMPORAL_MUTATION;

    /** Bundle codec version. */
    public static final int CODEC_VERSION = TemporalMutationBundle.CODEC_VERSION;

    /** Temporal schema version carried by mutations. */
    public static final int SCHEMA_VERSION = 1;

    private TemporalWireProtocol() {
    }

    public static String describe() {
        return "logical=" + LOGICAL_NAME +
               ", wireOp=0x" + Integer.toHexString(WIRE_OP & 0xFF) +
               ", codecVersion=" + CODEC_VERSION +
               ", schemaVersion=" + SCHEMA_VERSION;
    }
}
