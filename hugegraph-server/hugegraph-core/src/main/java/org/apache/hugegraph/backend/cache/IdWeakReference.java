/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.cache;


import org.apache.hugegraph.backend.id.Id;

import java.lang.ref.WeakReference;
import java.util.Objects;

public class IdWeakReference<I extends Comparable<Id>> extends WeakReference<Id> {
    private final int hashCode;

    public IdWeakReference(Id referent) {
        super(referent);
        this.hashCode = referent.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IdWeakReference<Comparable<Id>> that = (IdWeakReference<Comparable<Id>>) obj;
        return Objects.equals(this.get(), that.get());
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
