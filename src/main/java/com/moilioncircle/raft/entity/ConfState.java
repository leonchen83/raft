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

    public ConfState(List<Long> nodes, List<Long> learners) {
        this.nodes = nodes;
        this.learners = learners;
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
        return "ConfState{" +
                "nodes=" + nodes +
                ", learners=" + learners +
                '}';
    }

    public static RaftProto.ConfState build(ConfState state) {
        RaftProto.ConfState.Builder builder = RaftProto.ConfState.newBuilder();
        builder.addAllNodes(state.nodes);
        builder.addAllLearners(state.learners);
        return builder.build();
    }

    public static ConfState valueOf(RaftProto.ConfState state) {
        return new ConfState(state.getNodesList(), state.getLearnersList());
    }
}
