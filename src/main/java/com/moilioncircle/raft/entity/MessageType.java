/*
 * Copyright 2016-2017 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License")),
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
public enum MessageType {
    MsgHup(0),
    MsgBeat(1),
    MsgProp(2),
    MsgApp(3),
    MsgAppResp(4),
    MsgVote(5),
    MsgVoteResp(6),
    MsgSnap(7),
    MsgHeartbeat(8),
    MsgHeartbeatResp(9),
    MsgUnreachable(10),
    MsgSnapStatus(11),
    MsgCheckQuorum(12),
    MsgTransferLeader(13),
    MsgTimeoutNow(14),
    MsgReadIndex(15),
    MsgReadIndexResp(16),
    MsgPreVote(17),
    MsgPreVoteResp(18),
    UNRECOGNIZED(-1);

    int value;

    MessageType(int value) {
        this.value = value;
    }

    public static RaftProto.MessageType build(MessageType type) {
        return RaftProto.MessageType.forNumber(type.value);
    }

    public static MessageType valueOf(RaftProto.MessageType type) {
        switch (type.getNumber()) {
            case 0:
                return MsgHup;
            case 1:
                return MsgBeat;
            case 2:
                return MsgProp;
            case 3:
                return MsgApp;
            case 4:
                return MsgAppResp;
            case 5:
                return MsgVote;
            case 6:
                return MsgVoteResp;
            case 7:
                return MsgSnap;
            case 8:
                return MsgHeartbeat;
            case 9:
                return MsgHeartbeatResp;
            case 10:
                return MsgUnreachable;
            case 11:
                return MsgSnapStatus;
            case 12:
                return MsgCheckQuorum;
            case 13:
                return MsgTransferLeader;
            case 14:
                return MsgTimeoutNow;
            case 15:
                return MsgReadIndex;
            case 16:
                return MsgReadIndexResp;
            case 17:
                return MsgPreVote;
            case 18:
                return MsgPreVoteResp;
            case -1:
                return UNRECOGNIZED;
            default:
                return UNRECOGNIZED;
        }
    }

    public static boolean isLocalMsg(MessageType msgt) {
        return msgt == MsgHup || msgt == MsgBeat || msgt == MsgUnreachable ||
            msgt == MsgSnapStatus || msgt == MsgCheckQuorum;
    }

    public static boolean isResponseMsg(MessageType msgt) {
        return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp ||
            msgt == MsgUnreachable || msgt == MsgPreVoteResp;
    }

    public static MessageType voteRespMsgType(MessageType msgt) {
        switch (msgt) {
            case MsgVote:
                return MsgVoteResp;
            case MsgPreVote:
                return MsgPreVoteResp;
            default:
                throw new AssertionError("not a vote message: " + msgt);
        }
    }
}
