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

import java.util.Objects;

import org.apache.hugegraph.util.E;

public final class TemporalInterval {

    private final String entityId;
    private final String temporalLabel;
    private final String factKey;
    private final long validFrom;
    private final Long validTo;
    private final TemporalGranularity granularity;

    public TemporalInterval(String entityId,
                            String temporalLabel, String factKey,
                            long validFrom, Long validTo,
                            TemporalGranularity granularity) {
        E.checkArgument(entityId != null && !entityId.isEmpty(),
                        "The temporal entity id can't be null or empty");
        E.checkArgument(temporalLabel != null && !temporalLabel.isEmpty(),
                        "The temporal label can't be null or empty");
        E.checkArgument(factKey != null && !factKey.isEmpty(),
                        "The temporal fact key can't be null or empty");
        E.checkArgument(validTo == null || validFrom < validTo,
                        "The temporal interval must satisfy valid_from < valid_to");
        E.checkNotNull(granularity, "The temporal granularity can't be null");
        this.entityId = entityId;
        this.temporalLabel = temporalLabel;
        this.factKey = factKey;
        this.validFrom = validFrom;
        this.validTo = validTo;
        this.granularity = granularity;
    }

    public String entityId() {
        return this.entityId;
    }

    public String temporalLabel() {
        return this.temporalLabel;
    }

    public String factKey() {
        return this.factKey;
    }

    public long validFrom() {
        return this.validFrom;
    }

    public Long validTo() {
        return this.validTo;
    }

    public TemporalGranularity granularity() {
        return this.granularity;
    }

    public boolean contains(long time) {
        return this.validFrom <= time &&
               (this.validTo == null || time < this.validTo);
    }

    public boolean overlaps(TemporalInterval other) {
        E.checkNotNull(other, "The temporal interval can't be null");
        return this.entityId.equals(other.entityId) &&
               this.temporalLabel.equals(other.temporalLabel) &&
               this.factKey.equals(other.factKey) &&
               this.validFrom < endOf(other) &&
               other.validFrom < endOf(this);
    }

    private static long endOf(TemporalInterval interval) {
        return interval.validTo == null ? Long.MAX_VALUE : interval.validTo;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TemporalInterval)) {
            return false;
        }
        TemporalInterval other = (TemporalInterval) obj;
        return this.validFrom == other.validFrom &&
               Objects.equals(this.entityId, other.entityId) &&
               Objects.equals(this.temporalLabel, other.temporalLabel) &&
               Objects.equals(this.factKey, other.factKey) &&
               Objects.equals(this.validTo, other.validTo) &&
               this.granularity == other.granularity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.entityId,
                            this.temporalLabel, this.factKey,
                            this.validFrom, this.validTo, this.granularity);
    }
}
