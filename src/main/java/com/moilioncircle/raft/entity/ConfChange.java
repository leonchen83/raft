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
public class ConfChange {
    private long id;
    private ConfChangeType type;
    private long nodeID;
    private byte[] context;

    public ConfChange() {}

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ConfChangeType getType() {
        return type;
    }

    public void setType(ConfChangeType type) {
        this.type = type;
    }

    public long getNodeID() {
        return nodeID;
    }

    public void setNodeID(long nodeID) {
        this.nodeID = nodeID;
    }

    public byte[] getContext() {
        return context;
    }

    public void setContext(byte[] context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    public static RaftProtos.ConfChange build(ConfChange change) {
        RaftProtos.ConfChange.Builder builder = RaftProtos.ConfChange.newBuilder();
        builder.setId(change.getId());
        if (change.getType() != null) {
            builder.setType(ConfChangeType.build(change.getType()));
        }
        builder.setNodeID(change.getNodeID());
        if (change.getContext() != null) {
            builder.setContext(ByteString.copyFrom(change.getContext()));
        }
        return builder.build();
    }

    public static ConfChange valueOf(RaftProtos.ConfChange change) {
        ConfChange r = new ConfChange();
        r.setId(change.getId());
        if (change.getType() != null) {
            r.setType(ConfChangeType.valueOf(change.getType()));
        }
        r.setNodeID(change.getNodeID());
        if (!change.getContext().isEmpty()) {
            r.setContext(change.getContext().toByteArray());
        }
        return r;
    }
}
