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

package org.apache.hugegraph.temporal.store;

import java.util.Objects;

/**
 * (graph, temporal_label, entity_id, fact_key) - the placement unit that never
 * crosses a Region. Identity is the raw canonical fact key, never the hash.
 */
public final class ColocationGroup {

    private final String graphId;
    private final String temporalLabel;
    private final String entityId;
    private final TemporalFactKey factKey;

    public ColocationGroup(String graphId, String temporalLabel,
                           String entityId, TemporalFactKey factKey) {
        this.graphId = Objects.requireNonNull(graphId);
        this.temporalLabel = Objects.requireNonNull(temporalLabel);
        this.entityId = Objects.requireNonNull(entityId);
        this.factKey = Objects.requireNonNull(factKey);
    }

    public String graphId() {
        return this.graphId;
    }

    public String temporalLabel() {
        return this.temporalLabel;
    }

    public String entityId() {
        return this.entityId;
    }

    public TemporalFactKey factKey() {
        return this.factKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColocationGroup)) {
            return false;
        }
        ColocationGroup other = (ColocationGroup) o;
        return this.graphId.equals(other.graphId) &&
               this.temporalLabel.equals(other.temporalLabel) &&
               this.entityId.equals(other.entityId) &&
               this.factKey.equals(other.factKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.graphId, this.temporalLabel,
                            this.entityId, this.factKey);
    }

    @Override
    public String toString() {
        return "(" + this.graphId + ", " + this.temporalLabel + ", " +
               this.entityId + ", " + this.factKey + ")";
    }
}
