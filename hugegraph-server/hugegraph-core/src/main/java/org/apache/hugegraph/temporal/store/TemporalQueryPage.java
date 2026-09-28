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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * One page of a fact-scoped temporal read (Phase 4).
 *
 * Wraps the intervals of this page together with the opaque cursor of the next
 * page (empty when the result set is complete) and the number of physical rows
 * the Store scanned to produce the page. The scanned-row count is the
 * measurement hook for the frozen "scanned rows &lt;= 10x returned rows" gate
 * (design doc §5.4); carrying it up to the REST layer keeps the amplification of
 * an index-hit query observable end to end instead of only in Store logs. The
 * next-page cursor is opaque to every layer above the Store: it round-trips
 * Store -&gt; Server -&gt; REST (base64) -&gt; client and back unchanged.
 */
public final class TemporalQueryPage {

    private final List<TemporalIntervalResult> intervals;
    private final byte[] nextPageToken;
    private final long scannedRows;

    public TemporalQueryPage(List<TemporalIntervalResult> intervals,
                             byte[] nextPageToken, long scannedRows) {
        this.intervals = intervals == null
                         ? Collections.emptyList()
                         : Collections.unmodifiableList(new ArrayList<>(intervals));
        this.nextPageToken = nextPageToken == null ? new byte[0] : nextPageToken.clone();
        this.scannedRows = scannedRows;
    }

    /** A single complete page: no resume cursor and no scan accounting. */
    public static TemporalQueryPage of(List<TemporalIntervalResult> intervals) {
        return new TemporalQueryPage(intervals, new byte[0], 0L);
    }

    /** The intervals of this page, ascending by {@code valid_from}. */
    public List<TemporalIntervalResult> intervals() {
        return this.intervals;
    }

    /** Opaque cursor for the next page; empty when the result set is complete. */
    public byte[] nextPageToken() {
        return this.nextPageToken.clone();
    }

    /** True when more pages remain after this one. */
    public boolean hasMore() {
        return this.nextPageToken.length > 0;
    }

    /** Physical rows the Store scanned to produce this page. */
    public long scannedRows() {
        return this.scannedRows;
    }

    @Override
    public String toString() {
        return "TemporalQueryPage{intervals=" + this.intervals.size() +
               ", nextPageToken=" + Arrays.toString(this.nextPageToken) +
               ", scannedRows=" + this.scannedRows + '}';
    }
}
