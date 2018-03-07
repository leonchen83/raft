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

package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.Entry;

import java.util.List;

import static com.moilioncircle.raft.Raft.noLimit;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class TestUtil {
    public static Entry newEntry(long term, long index) {
        Entry entry = new Entry();
        entry.setTerm(term);
        entry.setIndex(index);
        return entry;
    }

    public static Raft.Config newTestConfig(long id, List<Long> peers, int election, int heartbeat, Storage storage) {
        Raft.Config config = new Raft.Config();
        config.id = id;
        config.peers = peers;
        config.electionTick = election;
        config.heartbeatTick = heartbeat;
        config.storage = storage;
        config.maxSizePerMsg = noLimit;
        config.maxInflightMsgs = 256;
        return config;
    }
}
