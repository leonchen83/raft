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
public class HardState {
    private long term;
    private long vote;
    private long commit;

    public HardState() {}

    public HardState(long term, long vote, long commit) {
        this.term = term;
        this.vote = vote;
        this.commit = commit;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getVote() {
        return vote;
    }

    public void setVote(long vote) {
        this.vote = vote;
    }

    public long getCommit() {
        return commit;
    }

    public void setCommit(long commit) {
        this.commit = commit;
    }

    @Override
    public String toString() {
        return "HardState{" +
                "term=" + term +
                ", vote=" + vote +
                ", commit=" + commit +
                '}';
    }

    public static RaftProto.HardState build(HardState state) {
        RaftProto.HardState.Builder builder = RaftProto.HardState.newBuilder();
        builder.setTerm(state.getTerm());
        builder.setVote(state.getVote());
        builder.setCommit(state.getCommit());
        return builder.build();
    }

    public static HardState valueOf(RaftProto.HardState state) {
        return new HardState(state.getTerm(), state.getVote(), state.getCommit());
    }
}
