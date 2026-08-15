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
package org.apache.hugegraph.temporal;

import org.apache.hugegraph.util.E;

public final class TemporalMutation {

    public enum Operation {
        APPEND,
        UPSERT,
        CLOSE,
        DELETE
    }

    private final String mutationId;
    private final Operation operation;
    private final TemporalInterval interval;
    private final String idempotencyKey;

    public TemporalMutation(String mutationId, Operation operation,
                            TemporalInterval interval, String idempotencyKey) {
        E.checkArgument(mutationId != null && !mutationId.isEmpty(),
                        "The temporal mutation id can't be null or empty");
        E.checkNotNull(operation, "The temporal mutation operation can't be null");
        E.checkNotNull(interval, "The temporal mutation interval can't be null");
        E.checkArgument(idempotencyKey != null && !idempotencyKey.isEmpty(),
                        "The temporal idempotency key can't be null or empty");
        this.mutationId = mutationId;
        this.operation = operation;
        this.interval = interval;
        this.idempotencyKey = idempotencyKey;
    }

    public String mutationId() {
        return this.mutationId;
    }

    public Operation operation() {
        return this.operation;
    }

    public TemporalInterval interval() {
        return this.interval;
    }

    public String idempotencyKey() {
        return this.idempotencyKey;
    }
}
