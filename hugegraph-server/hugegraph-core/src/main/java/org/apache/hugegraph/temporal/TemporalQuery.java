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

    private TemporalQuery(Type type, long from, Long to) {
        E.checkNotNull(type, "The temporal query type can't be null");
        if (type == Type.AS_OF) {
            E.checkArgument(to == null, "The as_of query can't have a to time");
        } else {
            E.checkNotNull(to, "The temporal range query to time can't be null");
            E.checkArgument(from < to,
                            "The temporal query must satisfy from < to");
        }
        this.type = type;
        this.from = from;
        this.to = to;
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

    public Type type() {
        return this.type;
    }

    public long from() {
        return this.from;
    }

    public Long to() {
        return this.to;
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
