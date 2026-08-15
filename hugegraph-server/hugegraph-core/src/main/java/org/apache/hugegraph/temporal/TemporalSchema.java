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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.util.E;

public final class TemporalSchema {

    private final String name;
    private final String entityLabel;
    private final String temporalLabel;
    private final Set<String> factKeys;
    // Ordered fact dimension names, fixed by schema (design ruling §2.1). Empty
    // for a schema created before the dimension-order reconciliation (RFC §2).
    private final List<String> dimensionOrder;

    public TemporalSchema(String name, String entityLabel, String temporalLabel,
                          Set<String> factKeys) {
        this(name, entityLabel, temporalLabel, factKeys, Collections.emptyList());
    }

    public TemporalSchema(String name, String entityLabel, String temporalLabel,
                          Set<String> factKeys, List<String> dimensionOrder) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The temporal schema name can't be null or empty");
        E.checkArgument(entityLabel != null && !entityLabel.isEmpty(),
                        "The temporal entity label can't be null or empty");
        E.checkArgument(temporalLabel != null && !temporalLabel.isEmpty(),
                        "The temporal label can't be null or empty");
        E.checkNotNull(factKeys, "The temporal fact keys can't be null");
        E.checkArgument(!factKeys.isEmpty(),
                        "The temporal fact keys can't be empty");
        for (String key : factKeys) {
            E.checkArgument(key != null && !key.isEmpty(),
                            "The temporal fact key can't be null or empty");
        }
        E.checkNotNull(dimensionOrder, "The fact dimension order can't be null");
        this.name = name;
        this.entityLabel = entityLabel;
        this.temporalLabel = temporalLabel;
        this.factKeys = Collections.unmodifiableSet(new LinkedHashSet<>(factKeys));
        this.dimensionOrder = Collections.unmodifiableList(
                new ArrayList<>(dimensionOrder));
    }

    public String name() {
        return this.name;
    }

    public String entityLabel() {
        return this.entityLabel;
    }

    public String temporalLabel() {
        return this.temporalLabel;
    }

    public Set<String> factKeys() {
        return this.factKeys;
    }

    /** Ordered fact dimension names (canonical); empty before reconciliation. */
    public List<String> dimensionOrder() {
        return this.dimensionOrder;
    }

    /**
     * Rebuild the canonical fact key from the schema dimension order (RFC §2).
     * The server never trusts a client supplied canonical encoding.
     */
    public TemporalFactKey factKey(Map<String, String> values) {
        if (this.dimensionOrder.isEmpty()) {
            throw new IllegalStateException(
                    "temporal schema '" + this.name +
                    "' has no fact dimension order; it can't rebuild a fact key");
        }
        return TemporalFactKey.of(this.dimensionOrder, values);
    }

    public void check(TemporalInterval interval) {
        E.checkNotNull(interval, "The temporal interval can't be null");
        E.checkArgument(this.temporalLabel.equals(interval.temporalLabel()),
                        "The temporal label doesn't match the temporal schema");
        E.checkArgument(this.factKeys.contains(interval.factKey()),
                        "The temporal fact key doesn't belong to the temporal schema");
    }
}
