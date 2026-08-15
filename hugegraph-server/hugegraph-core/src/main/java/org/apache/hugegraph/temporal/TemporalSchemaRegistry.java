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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.util.E;

/**
 * In-memory registry of {@link TemporalSchema} by schema name.
 *
 * It is the Server-side authority for fact-key reconstruction: the REST layer
 * looks up the schema's fact dimension order here, never from a client supplied
 * order (design ruling §2.1). It is intentionally independent from the backend
 * schema transaction; persistence of temporal schema is a later slice.
 */
public final class TemporalSchemaRegistry {

    private final Map<String, TemporalSchema> schemas = new ConcurrentHashMap<>();

    public void register(TemporalSchema schema) {
        E.checkNotNull(schema, "schema");
        this.schemas.put(schema.name(), schema);
    }

    public TemporalSchema get(String name) {
        return name == null ? null : this.schemas.get(name);
    }

    public boolean contains(String name) {
        return name != null && this.schemas.containsKey(name);
    }

    /** The registered schema whose temporal label matches, or null. */
    public TemporalSchema findByTemporalLabel(String temporalLabel) {
        for (TemporalSchema schema : this.schemas.values()) {
            if (schema.temporalLabel().equals(temporalLabel)) {
                return schema;
            }
        }
        return null;
    }

    public List<TemporalSchema> all() {
        return new ArrayList<>(this.schemas.values());
    }
}
