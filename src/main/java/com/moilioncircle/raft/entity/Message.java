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

import java.util.ArrayList;
import java.util.Arrays;
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

    public Message(MessageType type, long to, long from, long term, long logTerm, long index, List<Entry> entries, long commit, Snapshot snapshot, boolean reject, long rejectHint, byte[] context) {
        this.type = type;
        this.to = to;
        this.from = from;
        this.term = term;
        this.logTerm = logTerm;
        this.index = index;
        this.entries = entries;
        this.commit = commit;
        this.snapshot = snapshot;
        this.reject = reject;
        this.rejectHint = rejectHint;
        this.context = context;
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

    public boolean isReject() {
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
        return "Message{" +
                "type=" + type +
                ", to=" + to +
                ", from=" + from +
                ", term=" + term +
                ", logTerm=" + logTerm +
                ", index=" + index +
                ", entries=" + entries +
                ", commit=" + commit +
                ", snapshot=" + snapshot +
                ", reject=" + reject +
                ", rejectHint=" + rejectHint +
                ", context=" + Arrays.toString(context) +
                '}';
    }

    public static RaftProto.Message build(Message message) {
        RaftProto.Message.Builder builder = RaftProto.Message.newBuilder();
        builder.setType(MessageType.build(message.getType()));
        builder.setTo(message.getTo());
        builder.setFrom(message.getFrom());
        builder.setTerm(message.getTerm());
        builder.setLogTerm(message.getLogTerm());
        builder.setIndex(message.getIndex());
        builder.addAllEntries(Entry.build(message.getEntries()));
        builder.setCommit(message.getCommit());
        builder.setSnapshot(Snapshot.build(message.getSnapshot()));
        builder.setReject(message.isReject());
        builder.setRejectHint(message.getRejectHint());
        builder.setContext(ByteString.copyFrom(message.getContext()));
        return builder.build();
    }

    public static Message valueOf(RaftProto.Message message) {
        return new Message(
                MessageType.valueOf(message.getType()),
                message.getTo(),
                message.getFrom(),
                message.getTerm(),
                message.getLogTerm(),
                message.getIndex(),
                Entry.valueOf(message.getEntriesList()),
                message.getCommit(),
                Snapshot.valueOf(message.getSnapshot()),
                message.getReject(),
                message.getRejectHint(),
                message.getContext().toByteArray());
    }
}
