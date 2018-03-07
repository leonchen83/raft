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

import com.moilioncircle.raft.Raft.Config;
import com.moilioncircle.raft.Raft.SoftState;
import com.moilioncircle.raft.ReadOnly.ReadState;
import com.moilioncircle.raft.entity.ConfChange;
import com.moilioncircle.raft.entity.ConfState;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.entity.Message;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.util.Strings;

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.raft.Errors.ERR_STEP_LOCAL_MSG;
import static com.moilioncircle.raft.Errors.ERR_STEP_PEER_NOT_FOUND;
import static com.moilioncircle.raft.Raft.None;
import static com.moilioncircle.raft.RawNode.Ready.isEmptyHardState;
import static com.moilioncircle.raft.RawNode.Ready.isEmptySnap;
import static com.moilioncircle.raft.RawNode.Ready.isHardStateEqual;
import static com.moilioncircle.raft.RawNode.SnapshotStatus.Failure;
import static com.moilioncircle.raft.Status.getStatus;
import static com.moilioncircle.raft.entity.ConfChangeType.ConfChangeAddNode;
import static com.moilioncircle.raft.entity.EntryType.EntryConfChange;
import static com.moilioncircle.raft.entity.MessageType.MsgHup;
import static com.moilioncircle.raft.entity.MessageType.MsgProp;
import static com.moilioncircle.raft.entity.MessageType.MsgReadIndex;
import static com.moilioncircle.raft.entity.MessageType.MsgSnapStatus;
import static com.moilioncircle.raft.entity.MessageType.MsgTransferLeader;
import static com.moilioncircle.raft.entity.MessageType.MsgUnreachable;
import static com.moilioncircle.raft.entity.MessageType.isLocalMsg;
import static com.moilioncircle.raft.entity.MessageType.isResponseMsg;

/**
 * RawNode is a thread-unsafe Node.
 * The methods of this struct correspond to the methods of Node and are described
 * more fully there.
 */
public class RawNode {

    public enum SnapshotStatus {
        Finish,
        Failure,
    }

    public Raft raft;
    public SoftState prevSoftSt;
    public HardState prevHardSt;

    public void commitReady(Ready rd) {
        if (rd.softState != null) {
            prevSoftSt = rd.softState;
        }
        if (!isEmptyHardState(rd.hardState)) {
            prevHardSt = rd.hardState;
        }
        if (prevHardSt.getCommit() != 0) {
            /*
             * In most cases, prevHardSt and rd.HardState will be the same
             * because when there are new entries to apply we just sent a
             * HardState with an updated Commit value. However, on initial
             * startup the two are different because we don't send a HardState
             * until something changes, but we do send any un-applied but
             * committed entries (and previously-committed entries may be
             * incorporated into the snapshot, even if rd.CommittedEntries is
             * empty). Therefore we mark all committed entries as applied
             * whether they were included in rd.HardState or not.
             */
            raft.raftLog.appliedTo(prevHardSt.getCommit());
        }
        if (rd.entries.size() > 0) {
            Entry e = rd.entries.get(rd.entries.size() - 1);
            raft.raftLog.stableTo(e.getIndex(), e.getTerm());
        }
        if (!isEmptySnap(rd.snapshot)) {
            raft.raftLog.stableSnapTo(rd.snapshot.getMetadata().getIndex());
        }
        if (rd.readStates.size() != 0) {
            raft.readStates = null;
        }
    }

    /**
     * RawNode returns a new RawNode given configuration and a list of raft peers.
     */
    public RawNode(Config config, List<Peer> peers) {
        if (config.id == 0) {
            throw new Errors.RaftException("config.ID must not be zero");
        }
        raft = new Raft(config);

        long lastIndex = config.storage.lastIndex();
        /*
         * If the log is empty, this is a new RawNode (like StartNode); otherwise it's
         * restoring an existing RawNode (like RestartNode).
         * TODO(bdarnell): rethink RawNode initialization and whether the application needs
         * to be able to tell us when it expects the RawNode to exist.
         */
        if (lastIndex == 0) {
            raft.becomeFollower(1, None);
            List<Entry> ents = new ArrayList<>();
            for (int i = 0; i < peers.size(); i++) {
                Peer peer = peers.get(i);
                ConfChange cc = new ConfChange(0, ConfChangeAddNode, peer.id, peer.context);
                byte[] data = ConfChange.build(cc).toByteArray();
                ents.add(new Entry(1, i + 1, EntryConfChange, data));
            }

            raft.raftLog.append(ents);
            raft.raftLog.committed = ents.size();
            for (Peer peer : peers) {
                raft.addNode(peer.id);
            }
        }

        // Set the initial hard and soft states after performing all initialization.
        prevSoftSt = raft.softState();
        if (lastIndex == 0) {
            prevHardSt = new HardState();
        } else {
            prevHardSt = raft.hardState();
        }
    }

    /**
     * Tick advances the internal logical clock by a single tick.
     */
    public void tick() {
        raft.tick.get();
    }

    /**
     * TickQuiesced advances the internal logical clock by a single tick without
     * performing any other state machine processing. It allows the caller to avoid
     * periodic heartbeats and elections when all of the peers in a Raft group are
     * known to be at the same state. Expected usage is to periodically invoke Tick
     * or TickQuiesced depending on whether the group is "active" or "quiesced".
     * <p>
     * WARNING: Be very careful about using this method as it subverts the Raft
     * state machine. You should probably be using Tick instead.
     */
    public void tickQuiesced() {
        raft.electionElapsed++;
    }

    /**
     * Campaign causes this RawNode to transition to candidate state.
     */
    public void campaign() {
        Message msg = new Message();
        msg.setType(MsgHup);
        raft.step(msg);
    }

    /**
     * Propose proposes data be appended to the raft log.
     */
    public void propose(byte[] data) {
        Message msg = new Message();
        msg.setType(MsgProp);
        msg.setFrom(raft.id);
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(0, 0, null, data));
        msg.setEntries(entries);
        raft.step(msg);
    }

    /**
     * ProposeConfChange proposes a config change.
     */
    public void proposeConfChange(ConfChange cc) {
        byte[] data = ConfChange.build(cc).toByteArray();
        Message msg = new Message();
        msg.setType(MsgProp);
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(0, 0, EntryConfChange, data));
        msg.setEntries(entries);
        raft.step(msg);
    }

    /**
     * ApplyConfChange applies a config change to the local node.
     */
    public ConfState applyConfChange(ConfChange cc) {
        if (cc.getNodeID() == None) {
            return new ConfState(raft.nodes(), raft.learnerNodes());
        }
        switch (cc.getType()) {
            case ConfChangeAddNode:
                raft.addNode(cc.getNodeID());
                break;
            case ConfChangeAddLearnerNode:
                raft.addLearner(cc.getNodeID());
                break;
            case ConfChangeRemoveNode:
                raft.removeNode(cc.getNodeID());
                break;
            case ConfChangeUpdateNode:
            default:
                throw new Errors.RaftException("unexpected conf type");
        }
        return new ConfState(raft.nodes(), raft.learnerNodes());
    }

    /**
     * Step advances the state machine using the given message.
     */
    public void step(Message m) {
        // ignore unexpected local messages receiving over network
        if (isLocalMsg(m.getType())) {
            throw ERR_STEP_LOCAL_MSG;
        }
        Progress pr = raft.getProgress(m.getFrom());
        if (pr != null || !isResponseMsg(m.getType())) {
            raft.step(m);
        }
        throw ERR_STEP_PEER_NOT_FOUND;
    }

    /**
     * Ready returns the current point-in-time state of this RawNode.
     */
    public Ready Ready() {
        Ready rd = new Ready(raft, prevSoftSt, prevHardSt);
        raft.msgs = null;
        return rd;
    }

    /**
     * HasReady called when RawNode user need to check if any Ready pending.
     * Checking logic in this method should be consistent with Ready.containsUpdates().
     */
    public boolean hasReady() {
        Raft r = raft;
        if (!r.softState().equal(prevSoftSt)) {
            return true;
        }
        HardState hardSt = r.hardState();

        if (!isEmptyHardState(hardSt) && !isHardStateEqual(hardSt, prevHardSt)) {
            return true;
        }
        if (r.raftLog.unstable.snapshot != null && !isEmptySnap(r.raftLog.unstable.snapshot)) {
            return true;
        }
        if (r.msgs.size() > 0 || r.raftLog.unstableEntries().size() > 0 || r.raftLog.hasNextEnts()) {
            return true;
        }
        if (r.readStates.size() != 0) {
            return true;
        }
        return false;
    }

    /**
     * Advance notifies the RawNode that the application has applied and saved progress in the
     * last Ready results.
     */
    public void advance(Ready rd) {
        commitReady(rd);
    }

    /**
     * Status returns the current status of the given group.
     */
    public Status status() {
        return getStatus(raft);
    }

    /**
     * ReportUnreachable reports the given node is not reachable for the last send.
     */
    public void reportUnreachable(long id) {
        Message msg = new Message();
        msg.setType(MsgUnreachable);
        msg.setFrom(id);
        raft.step(msg);
    }

    /**
     * ReportSnapshot reports the status of the sent snapshot.
     */
    public void reportSnapshot(long id, SnapshotStatus status) {
        boolean rej = status == Failure;
        Message msg = new Message();
        msg.setType(MsgSnapStatus);
        msg.setFrom(id);
        msg.setReject(rej);
        raft.step(msg);
    }

    /**
     * TransferLeader tries to transfer leadership to the given transferee.
     */
    public void transferLeader(long transferee) {
        Message msg = new Message();
        msg.setType(MsgTransferLeader);
        msg.setFrom(transferee);
        raft.step(msg);
    }

    /**
     * ReadIndex requests a read state. The read state will be set in ready.
     * Read State has a read index. Once the application advances further than the read
     * index, any linearizable read requests issued before the read request can be
     * processed safely. The read state will have the same rctx attached.
     */
    public void readIndex(byte[] rctx) {
        Message msg = new Message();
        msg.setType(MsgReadIndex);
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(0, 0, null, rctx));
        msg.setEntries(entries);
        raft.step(msg);
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    /**
     * Ready encapsulates the entries and messages that are ready to read,
     * be saved to stable storage, committed or sent to other peers.
     * All fields in Ready are read-only.
     */
    public static class Ready {
        /**
         * The current volatile state of a Node.
         * SoftState will be nil if there is no update.
         * It is not required to consume or store SoftState.
         */
        public SoftState softState;

        /**
         * The current state of a Node to be saved to stable storage BEFORE
         * Messages are sent.
         * HardState will be equal to empty state if there is no update.
         */
        public HardState hardState;

        /**
         * ReadStates can be used for node to serve linearizable read requests locally
         * when its applied index is greater than the index in ReadState.
         * Note that the readState will be returned when raft receives msgReadIndex.
         * The returned is only valid for the request that requested to read.
         */
        public List<ReadState> readStates;

        /**
         * Entries specifies entries to be saved to stable storage BEFORE
         * Messages are sent.
         */
        public List<Entry> entries;

        /**
         * Snapshot specifies the snapshot to be saved to stable storage.
         */
        public Snapshot snapshot;

        /**
         * CommittedEntries specifies entries to be committed to a
         * store/state-machine. These have previously been committed to stable
         * store.
         */
        public List<Entry> committedEntries;

        /**
         * Messages specifies outbound messages to be sent AFTER Entries are
         * committed to stable storage.
         * If it contains a MsgSnap message, the application MUST report back to raft
         * when the snapshot has been received or has failed by calling ReportSnapshot.
         */
        public List<Message> messages;

        /**
         * MustSync indicates whether the HardState and Entries must be synchronously
         * written to disk or if an asynchronous write is permissible.
         */
        public boolean mustSync;

        public Ready(Raft r, SoftState prevSoftSt, HardState prevHardSt) {
            this.entries = r.raftLog.unstableEntries();
            this.committedEntries = r.raftLog.nextEnts();
            this.messages = r.msgs;
            SoftState softSt = r.softState();
            if (!softSt.equal(prevSoftSt)) {
                softState = softSt;
            }
            HardState hardSt = r.hardState();

            if (!isHardStateEqual(hardSt, prevHardSt)) {
                hardState = hardSt;
            }
            if (r.raftLog.unstable.snapshot != null) {
                snapshot = r.raftLog.unstable.snapshot;
            }
            if (r.readStates.size() != 0) {
                readStates = r.readStates;
            }
            mustSync = mustSync(hardState, prevHardSt, entries.size());
        }

        /**
         * MustSync returns true if the hard state and count of Raft entries indicate
         * that a synchronous write to persistent storage is required.
         */
        public static boolean mustSync(HardState st, HardState prevst, int entsnum) {
            /*
             * Persistent state on all servers:
             * (Updated on stable storage before responding to RPCs)
             * currentTerm
             * votedFor
             * log entries[]
             */
            return entsnum != 0 || st.getVote() != prevst.getVote() || st.getTerm() != prevst.getTerm();
        }

        public boolean containsUpdates() {
            return softState != null || !isEmptyHardState(hardState) ||
                    !isEmptySnap(snapshot) || entries.size() > 0 ||
                    committedEntries.size() > 0 || messages.size() > 0 || readStates.size() != 0;
        }

        public static boolean isHardStateEqual(HardState a, HardState b) {
            return a.getTerm() == b.getTerm() && a.getVote() == b.getVote() && a.getCommit() == b.getCommit();
        }

        /**
         * IsEmptyHardState returns true if the given HardState is empty.
         */
        public static boolean isEmptyHardState(HardState st) {
            return isHardStateEqual(st, new HardState());
        }

        /**
         * IsEmptySnap returns true if the given Snapshot is empty.
         */
        public static boolean isEmptySnap(Snapshot sp) {
            return sp.getMetadata().getIndex() == 0;
        }

        @Override
        public String toString() {
            return Strings.buildEx(this);
        }
    }

    public static class Peer {
        public long id;
        public byte[] context;

        @Override
        public String toString() {
            return Strings.buildEx(this);
        }
    }
}
