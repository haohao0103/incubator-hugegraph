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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Carries one registered {@link TemporalErrorCode} plus structured diagnostics.
 * Diagnostics are mandatory for placement and capacity rejections so that the
 * client can see the colocation group, the observed metrics and the configured
 * threshold instead of an opaque failure.
 */
public class TemporalStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final TemporalErrorCode code;
    private final Map<String, Object> diagnostics;

    public TemporalStoreException(TemporalErrorCode code, String message) {
        this(code, message, Collections.emptyMap());
    }

    public TemporalStoreException(TemporalErrorCode code, String message,
                                  Map<String, Object> diagnostics) {
        super(code.name() + ": " + message);
        this.code = code;
        this.diagnostics = Collections.unmodifiableMap(
                new LinkedHashMap<>(diagnostics));
    }

    public TemporalErrorCode code() {
        return this.code;
    }

    public Map<String, Object> diagnostics() {
        return this.diagnostics;
    }
}
