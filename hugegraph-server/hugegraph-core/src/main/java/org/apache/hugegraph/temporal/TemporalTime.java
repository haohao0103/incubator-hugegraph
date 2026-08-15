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

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.apache.hugegraph.util.E;

public final class TemporalTime {

    private TemporalTime() {
    }

    public static long parse(Object value) {
        E.checkNotNull(value, "The temporal value can't be null");
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        E.checkArgument(value instanceof String,
                        "The temporal value must be an ISO-8601 string or epoch millis");
        String text = (String) value;
        // Epoch millis expressed as a numeric string (e.g. REST query param).
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            // fall through to ISO-8601 UTC
        }
        E.checkArgument(text.endsWith("Z"),
                        "The temporal value must be an ISO-8601 UTC string or epoch millis");
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "The temporal value must be an ISO-8601 UTC string or epoch millis", e);
        }
    }

    public static String format(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }
}
