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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Versioned, length-prefixed codec for Store-side temporal bundles. */
public final class TemporalMutationBundleCodec {

    private TemporalMutationBundleCodec() {
    }

    public static byte[] encode(TemporalMutationBundle bundle) throws IOException {
        if (bundle == null) {
            throw new NullPointerException("bundle");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(TemporalMutationBundle.CODEC_VERSION);
        out.writeByte(bundle.operation().code());
        writeText(out, bundle.graph());
        writeText(out, bundle.temporalLabel());
        writeText(out, bundle.entityId());
        writeBytes(out, bundle.factKey());
        writeText(out, bundle.mutationId());
        out.writeInt(bundle.schemaVersion());
        out.writeLong(bundle.validFrom());
        out.writeLong(bundle.validTo());
        out.writeBoolean(bundle.open());
        writeBytes(out, bundle.payload());
        out.writeInt(bundle.views().size());
        for (TemporalMutationBundle.ViewMutation view : bundle.views()) {
            writeText(out, view.name());
            writeBytes(out, view.key());
            writeBytes(out, view.value());
            out.writeLong(view.committedRevision());
        }
        out.flush();
        return bytes.toByteArray();
    }

    public static TemporalMutationBundle decode(byte[] encoded) throws IOException {
        if (encoded == null) {
            throw new NullPointerException("encoded");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
        int version = in.readInt();
        if (version != TemporalMutationBundle.CODEC_VERSION) {
            throw new IOException("unsupported temporal bundle version: " + version);
        }
        TemporalMutationBundle.Operation operation =
                TemporalMutationBundle.Operation.fromCode(in.readUnsignedByte());
        String graph = readText(in);
        String label = readText(in);
        String entity = readText(in);
        byte[] factKey = readBytes(in);
        String mutationId = readText(in);
        int schemaVersion = in.readInt();
        long validFrom = in.readLong();
        long validTo = in.readLong();
        boolean open = in.readBoolean();
        byte[] payload = readBytes(in);
        int viewCount = in.readInt();
        if (viewCount < 0 || viewCount > 4) {
            throw new IOException("invalid temporal view count: " + viewCount);
        }
        List<TemporalMutationBundle.ViewMutation> views = new ArrayList<>(viewCount);
        for (int i = 0; i < viewCount; i++) {
            views.add(new TemporalMutationBundle.ViewMutation(readText(in),
                                                               readBytes(in),
                                                               readBytes(in),
                                                               in.readLong()));
        }
        if (in.available() != 0) {
            throw new IOException("trailing bytes in temporal bundle: " + in.available());
        }
        return new TemporalMutationBundle(operation, graph, label, entity, factKey,
                                          mutationId, schemaVersion, validFrom, validTo,
                                          open, payload, views);
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readText(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            throw new IOException("truncated temporal bundle", e);
        }
        if (length < 0 || length > in.available()) {
            throw new IOException("invalid temporal bundle field length: " + length);
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }
}
