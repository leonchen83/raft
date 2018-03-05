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

import com.moilioncircle.raft.entity.proto.RaftProto;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class SnapshotMetadata {
    private ConfState confState;
    private long index;
    private long term;

    public SnapshotMetadata() {
        this.confState = new ConfState();
    }

    public SnapshotMetadata(ConfState confState, long index, long term) {
        this.confState = confState;
        this.index = index;
        this.term = term;
    }

    public ConfState getConfState() {
        return confState;
    }

    public void setConfState(ConfState confState) {
        this.confState = confState;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    @Override
    public String toString() {
        return "SnapshotMetadata{" +
                "confState=" + confState +
                ", index=" + index +
                ", term=" + term +
                '}';
    }

    public static RaftProto.SnapshotMetadata build(SnapshotMetadata meta) {
        RaftProto.SnapshotMetadata.Builder builder = RaftProto.SnapshotMetadata.newBuilder();
        builder.setConfState(ConfState.build(meta.getConfState()));
        builder.setIndex(meta.getIndex());
        builder.setTerm(meta.getTerm());
        return builder.build();
    }

    public static SnapshotMetadata valueOf(RaftProto.SnapshotMetadata meta) {
        return new SnapshotMetadata(ConfState.valueOf(meta.getConfState()), meta.getIndex(), meta.getTerm());
    }
}
