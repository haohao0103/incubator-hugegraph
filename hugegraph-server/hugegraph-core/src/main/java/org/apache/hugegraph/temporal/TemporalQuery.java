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

public final class TemporalQuery {

    public enum Type {
        AS_OF,
        BETWEEN,
        OVERLAP
    }

    private final Type type;
    private final long from;
    private final Long to;
    // Phase 4 (additive, optional): a client-side page cap and an opaque resume
    // cursor. limit <= 0 means "no client cap"; an empty pageToken means "first
    // page". Both default to the pre-Phase-4 values, so the as_of/between/overlap
    // factories and every existing caller are unchanged.
    private final long limit;
    private final byte[] pageToken;

    private TemporalQuery(Type type, long from, Long to) {
        this(type, from, to, 0L, null);
    }

    private TemporalQuery(Type type, long from, Long to, long limit, byte[] pageToken) {
        E.checkNotNull(type, "The temporal query type can't be null");
        if (type == Type.AS_OF) {
            E.checkArgument(to == null, "The as_of query can't have a to time");
        } else {
            E.checkNotNull(to, "The temporal range query to time can't be null");
            E.checkArgument(from < to,
                            "The temporal query must satisfy from < to");
        }
        E.checkArgument(limit >= 0, "The temporal query limit can't be negative");
        this.type = type;
        this.from = from;
        this.to = to;
        this.limit = limit;
        this.pageToken = pageToken == null ? new byte[0] : pageToken.clone();
    }

    public static TemporalQuery asOf(long time) {
        return new TemporalQuery(Type.AS_OF, time, null);
    }

    public static TemporalQuery between(long from, long to) {
        return new TemporalQuery(Type.BETWEEN, from, to);
    }

    public static TemporalQuery overlap(long from, long to) {
        return new TemporalQuery(Type.OVERLAP, from, to);
    }

    /**
     * Return a copy of this query capped to {@code limit} intervals per page and
     * resuming from the opaque {@code pageToken} cursor (empty = first page).
     * Pagination applies to the range types (between / overlap); as_of resolves
     * to at most one interval, so the cap is a no-op there.
     */
    public TemporalQuery withPage(long limit, byte[] pageToken) {
        return new TemporalQuery(this.type, this.from, this.to, limit, pageToken);
    }

    public Type type() {
        return this.type;
    }

    public long from() {
        return this.from;
    }

    public Long to() {
        return this.to;
    }

    /** Client-side page cap; {@code <= 0} means no cap. */
    public long limit() {
        return this.limit;
    }

    /** Opaque resume cursor; empty means the first page. */
    public byte[] pageToken() {
        return this.pageToken.clone();
    }

    /** True when a resume cursor is present (a page after the first). */
    public boolean hasPageToken() {
        return this.pageToken.length > 0;
    }

    public boolean matches(TemporalInterval interval) {
        E.checkNotNull(interval, "The temporal interval can't be null");
        if (this.type == Type.AS_OF) {
            return interval.contains(this.from);
        }
        long intervalTo = interval.validTo() == null ? Long.MAX_VALUE : interval.validTo();
        return interval.validFrom() < this.to && this.from < intervalTo;
    }
}
