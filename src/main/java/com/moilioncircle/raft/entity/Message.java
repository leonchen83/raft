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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Message {
    private MessageType type;
    private long to;
    private long from;
    private long term;
    private long logTerm;
    private long index;
    private List<Entry> entries;
    private long commit;
    private Snapshot snapshot;
    private boolean reject;
    private long rejectHint;
    private byte[] context;

    public Message() {
        this.entries = new ArrayList<>();
        this.snapshot = new Snapshot();
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getLogTerm() {
        return logTerm;
    }

    public void setLogTerm(long logTerm) {
        this.logTerm = logTerm;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public long getCommit() {
        return commit;
    }

    public void setCommit(long commit) {
        this.commit = commit;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public boolean getReject() {
        return reject;
    }

    public void setReject(boolean reject) {
        this.reject = reject;
    }

    public long getRejectHint() {
        return rejectHint;
    }

    public void setRejectHint(long rejectHint) {
        this.rejectHint = rejectHint;
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

    public static RaftProtos.Message build(Message message) {
        RaftProtos.Message.Builder builder = RaftProtos.Message.newBuilder();
        if (message.getType() != null) {
            builder.setType(MessageType.build(message.getType()));
        }
        builder.setTo(message.getTo());
        builder.setFrom(message.getFrom());
        builder.setTerm(message.getTerm());
        builder.setLogTerm(message.getLogTerm());
        builder.setIndex(message.getIndex());
        if (message.getEntries() != null) {
            builder.addAllEntries(Entry.build(message.getEntries()));
        }
        builder.setCommit(message.getCommit());
        if (message.getSnapshot() != null) {
            builder.setSnapshot(Snapshot.build(message.getSnapshot()));
        }
        builder.setReject(message.getReject());
        builder.setRejectHint(message.getRejectHint());
        if (message.getContext() != null) {
            builder.setContext(ByteString.copyFrom(message.getContext()));
        }
        return builder.build();
    }

    public static Message valueOf(RaftProtos.Message message) {
        Message r = new Message();
        if (message.getType() != null) {
            r.setType(MessageType.valueOf(message.getType()));
        }
        r.setTo(message.getTo());
        r.setFrom(message.getFrom());
        r.setTerm(message.getTerm());
        r.setLogTerm(message.getLogTerm());
        r.setIndex(message.getIndex());
        if (message.getEntriesList() != null) {
            r.setEntries(Entry.valueOf(message.getEntriesList()));
        }
        r.setCommit(message.getCommit());
        if (message.getSnapshot() != null) {
            r.setSnapshot(Snapshot.valueOf(message.getSnapshot()));
        }
        r.setReject(message.getReject());
        r.setRejectHint(message.getRejectHint());
        if (!message.getContext().isEmpty()) {
            r.setContext(message.getContext().toByteArray());
        }
        return r;
    }
}
