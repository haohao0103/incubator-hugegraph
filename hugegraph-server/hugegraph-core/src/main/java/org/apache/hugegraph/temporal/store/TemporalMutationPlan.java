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
import java.util.Collections;
import java.util.List;

/**
 * The four-view mutation plan of one temporal mutation.
 *
 * This is the frozen Server&lt;-&gt;Store data-plane contract (design ruling §3.2
 * step 2). The Store ignores each view's value and writes {@code revision ||
 * payload} uniformly, so a plan carries only the view NAME and KEY; the
 * payload, interval and identity travel once on the bundle level.
 */
public final class TemporalMutationPlan {

    private final TemporalWrite.Request request;
    private final String tieBreaker;
    private final List<ViewKey> views;

    TemporalMutationPlan(TemporalWrite.Request request, String tieBreaker,
                         List<ViewKey> views) {
        this.request = request;
        this.tieBreaker = tieBreaker;
        this.views = Collections.unmodifiableList(new ArrayList<>(views));
    }

    public TemporalWrite.Request request() {
        return this.request;
    }

    public String tieBreaker() {
        return this.tieBreaker;
    }

    /** History / current / open-index (only when open) / temporal-index. */
    public List<ViewKey> views() {
        return this.views;
    }

    public ViewKey view(String viewName) {
        for (ViewKey view : this.views) {
            if (view.view().equals(viewName)) {
                return view;
            }
        }
        return null;
    }

    public static final class ViewKey {

        private final String view;
        private final byte[] key;

        ViewKey(String view, byte[] key) {
            this.view = view;
            this.key = key.clone();
        }

        public String view() {
            return this.view;
        }

        public byte[] key() {
            return this.key.clone();
        }
    }
}
