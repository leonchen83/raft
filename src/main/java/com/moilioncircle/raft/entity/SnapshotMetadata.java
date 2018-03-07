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

import com.moilioncircle.raft.entity.proto.RaftProtos;
import com.moilioncircle.raft.util.Strings;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class SnapshotMetadata {
    private ConfState confState = new ConfState();
    private long index;
    private long term;

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
        return Strings.buildEx(this);
    }

    public static RaftProtos.SnapshotMetadata build(SnapshotMetadata meta) {
        RaftProtos.SnapshotMetadata.Builder builder = RaftProtos.SnapshotMetadata.newBuilder();
        if (meta.getConfState() != null) {
            builder.setConfState(ConfState.build(meta.getConfState()));
        }
        builder.setIndex(meta.getIndex());
        builder.setTerm(meta.getTerm());
        return builder.build();
    }

    public static SnapshotMetadata valueOf(RaftProtos.SnapshotMetadata meta) {
        SnapshotMetadata r = new SnapshotMetadata();
        if (meta.getConfState() != null) {
            r.setConfState(ConfState.valueOf(meta.getConfState()));
        }
        r.setIndex(meta.getIndex());
        r.setTerm(meta.getTerm());
        return r;
    }
}
