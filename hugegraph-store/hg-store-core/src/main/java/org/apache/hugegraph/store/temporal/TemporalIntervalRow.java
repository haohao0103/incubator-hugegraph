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

import java.nio.charset.StandardCharsets;

/**
 * A single temporal interval row read back from the Store history table.
 *
 * This is the Store-side read DTO; it is intentionally independent from the
 * Server core model so the Store can answer fact-scoped queries without
 * depending on hugegraph-core.
 */
public final class TemporalIntervalRow {

    private final byte[] factKey;
    private final long validFrom;
    /** {@code null} means open (still valid). */
    private final Long validTo;
    private final long committedRevision;

    public TemporalIntervalRow(byte[] factKey, long validFrom, Long validTo,
                               long committedRevision) {
        this.factKey = factKey.clone();
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
        return "TemporalIntervalRow{factKey=" +
               new String(this.factKey, StandardCharsets.UTF_8) +
               ", validFrom=" + this.validFrom +
               ", validTo=" + (this.validTo == null ? "open" : this.validTo) +
               ", committedRevision=" + this.committedRevision + '}';
    }
}
