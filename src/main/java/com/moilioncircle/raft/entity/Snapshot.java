/*
 * Copyright 2016-2017 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.raft.entity;

import com.google.protobuf.ByteString;
import com.moilioncircle.raft.entity.proto.RaftProtos;
import com.moilioncircle.raft.util.Strings;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Snapshot {
    private byte[] data = new byte[0];
    private SnapshotMetadata metadata = new SnapshotMetadata();

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(SnapshotMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    public static RaftProtos.Snapshot build(Snapshot snap) {
        RaftProtos.Snapshot.Builder builder = RaftProtos.Snapshot.newBuilder();
        if (snap.getData() != null) {
            builder.setData(ByteString.copyFrom(snap.getData()));
        }
        if (snap.getMetadata() != null) {
            builder.setMetadata(SnapshotMetadata.build(snap.getMetadata()));
        }
        return builder.build();
    }

    public static Snapshot valueOf(RaftProtos.Snapshot snap) {
        Snapshot r = new Snapshot();
        if (snap.getData() != null) {
            r.setData(snap.getData().toByteArray());
        }
        if (snap.getMetadata() != null) {
            r.setMetadata(SnapshotMetadata.valueOf(snap.getMetadata()));
        }
        return r;
    }
}
