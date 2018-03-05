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
 * @author Baoyi Chen
 * @since 1.0.0
 */
public enum EntryType {
    EntryNormal(0),
    EntryConfChange(1),
    UNRECOGNIZED(-1);

    int value;

    EntryType(int value) {
        this.value = value;
    }

    public static RaftProto.EntryType build(EntryType type) {
        return RaftProto.EntryType.forNumber(type.value);
    }

    public static EntryType valueOf(RaftProto.EntryType type) {
        switch (type.getNumber()) {
            case 0:
                return EntryNormal;
            case 1:
                return EntryConfChange;
            case -1:
                return UNRECOGNIZED;
            default:
                return UNRECOGNIZED;
        }
    }
}
