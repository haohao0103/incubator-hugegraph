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

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One fact-scoped temporal interval read back from a temporal backend.
 *
 * The fact key is carried as its canonical bytes; the caller already holds the
 * {@link TemporalFactKey} (it issued the query), so the entity label, temporal
 * label and granularity are not duplicated here. valid_to == null means the
 * interval is still open.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
                getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE)
public final class TemporalIntervalResult {

    @JsonProperty("fact_key")
    private final byte[] factKey;
    @JsonProperty("valid_from")
    private final long validFrom;
    @JsonProperty("valid_to")
    private final Long validTo;
    @JsonProperty("committed_revision")
    private final long committedRevision;

    public TemporalIntervalResult(byte[] factKey, long validFrom, Long validTo,
                                  long committedRevision) {
        this.factKey = factKey == null ? new byte[0] : factKey.clone();
        this.validFrom = validFrom;
        this.validTo = validTo;
        this.committedRevision = committedRevision;
    }

    public byte[] factKey() {
        return this.factKey.clone();
    }

    public long validFrom() {
        return this.validFrom;
    }

    /** null means open (still valid). */
    public Long validTo() {
        return this.validTo;
    }

    public boolean open() {
        return this.validTo == null;
    }

    public long committedRevision() {
        return this.committedRevision;
    }

    @Override
    public String toString() {
        return "TemporalIntervalResult{factKey=" + Arrays.toString(this.factKey) +
               ", validFrom=" + this.validFrom +
               ", validTo=" + (this.validTo == null ? "open" : this.validTo) +
               ", committedRevision=" + this.committedRevision + '}';
    }
}
