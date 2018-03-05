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
public enum ConfChangeType {
    ConfChangeAddNode(0),
    ConfChangeRemoveNode(1),
    ConfChangeUpdateNode(2),
    ConfChangeAddLearnerNode(3),
    UNRECOGNIZED(-1);

    int value;

    ConfChangeType(int value) {
        this.value = value;
    }

    public static RaftProto.ConfChangeType build(ConfChangeType type) {
        return RaftProto.ConfChangeType.forNumber(type.value);
    }

    public static ConfChangeType valueOf(RaftProto.ConfChangeType type) {
        switch (type.getNumber()) {
            case 0:
                return ConfChangeAddNode;
            case 1:
                return ConfChangeRemoveNode;
            case 2:
                return ConfChangeUpdateNode;
            case 3:
                return ConfChangeAddLearnerNode;
            case -1:
                return UNRECOGNIZED;
            default:
                return UNRECOGNIZED;
        }
    }
}
