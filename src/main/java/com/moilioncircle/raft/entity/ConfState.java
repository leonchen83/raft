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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ConfState {
    private List<Long> nodes;
    private List<Long> learners;

    public ConfState() {
        this.nodes = new ArrayList<>();
        this.learners = new ArrayList<>();
    }

    public List<Long> getNodes() {
        return nodes;
    }

    public void setNodes(List<Long> nodes) {
        this.nodes = nodes;
    }

    public List<Long> getLearners() {
        return learners;
    }

    public void setLearners(List<Long> learners) {
        this.learners = learners;
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    public static RaftProtos.ConfState build(ConfState state) {
        RaftProtos.ConfState.Builder builder = RaftProtos.ConfState.newBuilder();
        if (state.nodes != null) {
            builder.addAllNodes(state.nodes);
        }
        if (state.learners != null) {
            builder.addAllLearners(state.learners);
        }
        return builder.build();
    }

    public static ConfState valueOf(RaftProtos.ConfState state) {
        ConfState r = new ConfState();
        if (state.getNodesList() != null) {
            r.setNodes(state.getNodesList());
        }
        if (state.getLearnersList() != null) {
            r.setLearners(state.getLearnersList());
        }
        return r;
    }
}
