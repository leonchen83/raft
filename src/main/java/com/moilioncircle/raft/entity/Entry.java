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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Entry {
    private long term;
    private long index;
    private EntryType type;
    private byte[] data;

    public Entry() {}

    public Entry(long term, long index, EntryType type, byte[] data) {
        this.term = term;
        this.index = index;
        this.type = type;
        this.data = data;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public EntryType getType() {
        return type;
    }

    public void setType(EntryType type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    public static RaftProto.Entry build(Entry entry) {
        RaftProto.Entry.Builder builder = RaftProto.Entry.newBuilder();
        builder.setTerm(entry.getTerm());
        builder.setIndex(entry.getIndex());
        builder.setData(ByteString.copyFrom(entry.getData()));
        builder.setType(EntryType.build(entry.getType()));
        return builder.build();
    }

    public static List<RaftProto.Entry> build(List<Entry> entries) {
        List<RaftProto.Entry> list = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            list.add(build(entry));
        }
        return list;
    }

    public static List<Entry> valueOf(List<RaftProto.Entry> entries) {
        List<Entry> list = new ArrayList<>(entries.size());
        for (RaftProto.Entry entry : entries) {
            list.add(valueOf(entry));
        }
        return list;
    }

    public static Entry valueOf(RaftProto.Entry entry) {
        return new Entry(entry.getTerm(), entry.getIndex(), EntryType.valueOf(entry.getType()), entry.getData().toByteArray());
    }
}
