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
import com.moilioncircle.raft.entity.proto.RaftProto;
import com.moilioncircle.raft.util.Strings;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Snapshot {
    private byte[] data;
    private SnapshotMetadata metadata;

    public Snapshot() {
        this.metadata = new SnapshotMetadata();
    }

    public Snapshot(byte[] data, SnapshotMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }

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

    public static RaftProto.Snapshot build(Snapshot snap) {
        RaftProto.Snapshot.Builder builder = RaftProto.Snapshot.newBuilder();
        builder.setData(ByteString.copyFrom(snap.getData()));
        builder.setMetadata(SnapshotMetadata.build(snap.getMetadata()));
        return builder.build();
    }

    public static Snapshot valueOf(RaftProto.Snapshot snap) {
        return new Snapshot(snap.getData().toByteArray(), SnapshotMetadata.valueOf(snap.getMetadata()));
    }
}
