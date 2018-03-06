package com.moilioncircle.raft;

import com.moilioncircle.raft.ReadOnly.ReadState;
import com.moilioncircle.raft.entity.ConfState;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.entity.Message;
import com.moilioncircle.raft.entity.MessageType;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.util.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.moilioncircle.raft.Errors.ERR_PROPOSAL_DROPPED;
import static com.moilioncircle.raft.Errors.ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
import static com.moilioncircle.raft.Progress.ProgressState.Probe;
import static com.moilioncircle.raft.Progress.ProgressState.Replicate;
import static com.moilioncircle.raft.Raft.StateRole.Candidate;
import static com.moilioncircle.raft.Raft.StateRole.Follower;
import static com.moilioncircle.raft.Raft.StateRole.Leader;
import static com.moilioncircle.raft.Raft.StateRole.PreCandidate;
import static com.moilioncircle.raft.RawNode.Ready.isEmptySnap;
import static com.moilioncircle.raft.RawNode.Ready.isHardStateEqual;
import static com.moilioncircle.raft.ReadOnly.ReadOnlyOption.LeaseBased;
import static com.moilioncircle.raft.ReadOnly.ReadOnlyOption.Safe;
import static com.moilioncircle.raft.entity.EntryType.EntryConfChange;
import static com.moilioncircle.raft.entity.EntryType.EntryNormal;
import static com.moilioncircle.raft.entity.MessageType.MsgApp;
import static com.moilioncircle.raft.entity.MessageType.MsgAppResp;
import static com.moilioncircle.raft.entity.MessageType.MsgBeat;
import static com.moilioncircle.raft.entity.MessageType.MsgCheckQuorum;
import static com.moilioncircle.raft.entity.MessageType.MsgHeartbeat;
import static com.moilioncircle.raft.entity.MessageType.MsgHeartbeatResp;
import static com.moilioncircle.raft.entity.MessageType.MsgHup;
import static com.moilioncircle.raft.entity.MessageType.MsgPreVote;
import static com.moilioncircle.raft.entity.MessageType.MsgPreVoteResp;
import static com.moilioncircle.raft.entity.MessageType.MsgProp;
import static com.moilioncircle.raft.entity.MessageType.MsgReadIndex;
import static com.moilioncircle.raft.entity.MessageType.MsgReadIndexResp;
import static com.moilioncircle.raft.entity.MessageType.MsgSnap;
import static com.moilioncircle.raft.entity.MessageType.MsgTimeoutNow;
import static com.moilioncircle.raft.entity.MessageType.MsgVote;
import static com.moilioncircle.raft.entity.MessageType.MsgVoteResp;
import static com.moilioncircle.raft.entity.MessageType.voteRespMsgType;
import static java.lang.Math.min;

public class Raft {

    private static final Logger logger = LoggerFactory.getLogger(Raft.class);

    public enum StateRole {
        Follower,
        Candidate,
        Leader,
        PreCandidate
    }

    /**
     * campaignPreElection represents the first phase of a normal election when
     * Config.PreVote is true.
     */
    public static final String campaignPreElection = "CampaignPreElection";

    /**
     * campaignElection represents a normal (time-based) election (the second phase
     * of the election when Config.PreVote is true).
     */
    public static final String campaignElection = "CampaignElection";

    /**
     * campaignTransfer represents the type of leader transfer
     */
    public static final String campaignTransfer = "CampaignTransfer";

    public static final long None = 0;
    public static final long noLimit = Long.MAX_VALUE;

    public long id;

    public long term;

    public long vote;

    public List<ReadState> readStates;

    /**
     * the log
     */
    public RaftLog raftLog;

    public int maxInflight;
    public long maxMsgSize;
    public Map<Long, Progress> prs;
    public Map<Long, Progress> learnerPrs;

    public StateRole state;

    /**
     * isLearner is true if the local raft node is a learner.
     */
    public boolean isLearner;

    public Map<Long, Boolean> votes;

    public List<Message> msgs;

    /**
     * the leader id
     */
    public long lead;

    /**
     * leadTransferee is id of the leader transfer target when its value is not zero.
     * Follow the procedure defined in raft thesis 3.10.
     */
    public long leadTransferee;

    /**
     * Only one conf change may be pending (in the log, but not yet
     * applied) at a time. This is enforced via pendingConfIndex, which
     * is set to a value >= the log index of the latest pending
     * configuration change (if any). Config changes are only allowed to
     * be proposed if the leader's applied index is greater than this
     * value.
     */
    public long pendingConfIndex;

    public ReadOnly readOnly;

    /**
     * number of ticks since it reached last electionTimeout when it is leader
     * or candidate.
     * number of ticks since it reached last electionTimeout or received a
     * valid message from current leader when it is a follower.
     */
    public int electionElapsed;

    /**
     * number of ticks since it reached last heartbeatTimeout.
     * only leader keeps heartbeatElapsed.
     */
    public int heartbeatElapsed;

    public boolean checkQuorum;
    public boolean preVote;

    public int heartbeatTimeout;
    public int electionTimeout;

    /**
     * randomizedElectionTimeout is a random number between
     * [electiontimeout, 2 * electiontimeout - 1]. It gets reset
     * when raft changes its state to follower or candidate.
     */
    public int randomizedElectionTimeout;

    public boolean disableProposalForwarding;

    public Supplier<Void> tick;
    public BiFunction<Raft, Message, Void> step;

    public Raft(Config c) {
        c.validate();
        RaftLog raftlog = new RaftLog(c.storage);
        Tuple2<HardState, ConfState> tuple = c.storage.initialState();
        HardState hs = tuple.getV1();
        ConfState cs = tuple.getV2();
        List<Long> peers = c.peers;
        List<Long> learners = c.learners;
        if (cs.getNodes().size() > 0 || cs.getLearners().size() > 0) {
            if (peers.size() > 0 || learners.size() > 0) {
                /*
                 * TODO(bdarnell): the peers argument is always nil except in
                 * tests; the argument should be removed and these tests should be
                 * updated to specify their nodes through a snapshot.
                 */
                throw new Errors.RaftException("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
            }
            peers = cs.getNodes();
            learners = cs.getLearners();
        }
        this.id = c.id;
        this.lead = None;
        this.isLearner = false;
        this.raftLog = raftlog;
        this.maxMsgSize = c.maxSizePerMsg;
        this.maxInflight = c.maxInflightMsgs;
        this.prs = new HashMap<>();
        this.learnerPrs = new HashMap<>();
        this.electionTimeout = c.electionTick;
        this.heartbeatTimeout = c.heartbeatTick;
        this.checkQuorum = c.checkQuorum;
        this.preVote = c.preVote;
        this.readOnly = new ReadOnly(c.readOnlyOption);
        this.disableProposalForwarding = c.disableProposalForwarding;

        for (Long p : peers) {
            Progress progress = new Progress();
            progress.next = 1;
            progress.ins = new Progress.Inflights(maxInflight);
            this.prs.put(p, progress);
        }

        for (Long p : learners) {
            if (prs.containsKey(p)) {
                throw new Errors.RaftException("node " + p + " is in both learner and peer list");
            }
            Progress progress = new Progress();
            progress.next = 1;
            progress.ins = new Progress.Inflights(maxInflight);
            progress.isLearner = true;
            this.learnerPrs.put(p, progress);
            if (this.id == p) {
                isLearner = true;
            }
        }

        if (!isHardStateEqual(hs, new HardState())) {
            loadState(hs);
        }
        if (c.applied > 0) {
            raftlog.appliedTo(c.applied);
        }
        becomeFollower(term, None);

        List<String> nodesStrs = new ArrayList<>();
        for (Long n : nodes()) {
            nodesStrs.add(String.valueOf(n));
        }

        logger.info("newRaft {} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: {}, lastterm: {}]",
                id, nodesStrs, term, raftLog.committed, raftLog.applied, raftLog.lastIndex(), raftLog.lastTerm());
    }

    public boolean hasLeader() { return lead != None; }

    public HardState hardState() {
        return new HardState(term, vote, raftLog.committed);
    }

    public SoftState softState() {
        SoftState soft = new SoftState();
        soft.lead = lead;
        soft.raftState = state;
        return soft;
    }

    public int quorum() { return prs.size() / 2 + 1; }

    public List<Long> nodes() {
        List<Long> nodes = new ArrayList<>(prs.size());
        for (Map.Entry<Long, Progress> entry : prs.entrySet()) {
            nodes.add(entry.getKey());
        }
        Collections.sort(nodes);
        return nodes;
    }

    public List<Long> learnerNodes() {
        List<Long> nodes = new ArrayList<>(learnerPrs.size());
        for (Map.Entry<Long, Progress> entry : learnerPrs.entrySet()) {
            nodes.add(entry.getKey());
        }
        Collections.sort(nodes);
        return nodes;
    }

    /**
     * send persists state to stable storage and then sends to its mailbox.
     */
    public void send(Message m) {
        m.setFrom(id);
        if (m.getType() == MsgVote || m.getType() == MsgVoteResp || m.getType() == MsgPreVote || m.getType() == MsgPreVoteResp) {
            if (m.getTerm() == 0) {
                /*
                 * All {pre-,}campaign messages need to have the term set when
                 * sending.
                 * - MsgVote: m.Term is the term the node is campaigning for,
                 *   non-zero as we increment the term when campaigning.
                 * - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                 *   granted, non-zero for the same reason MsgVote is
                 * - MsgPreVote: m.Term is the term the node will campaign,
                 *   non-zero as we use m.Term to indicate the next term we'll be
                 *   campaigning for
                 * - MsgPreVoteResp: m.Term is the term received in the original
                 *   MsgPreVote if the pre-vote was granted, non-zero for the
                 *   same reasons MsgPreVote is
                 */
                throw new Errors.RaftException("term should be set when sending " + m.getType());
            }
        } else {
            if (m.getTerm() != 0) {
                throw new Errors.RaftException("term should not be set when sending " + m.getType() + " (was " + m.getTerm() + ")");
            }
            /*
             * do not attach term to MsgProp, MsgReadIndex
             * proposals are a way to forward to the leader and
             * should be treated as local message.
             * MsgReadIndex is also forwarded to leader.
             */
            if (m.getType() != MsgProp && m.getType() != MsgReadIndex) {
                m.setTerm(term);
            }
        }
        msgs.add(m);
    }

    public Progress getProgress(long id) {
        Progress pr = prs.get(id);
        if (pr != null) return pr;
        return learnerPrs.get(id);
    }

    /**
     * sendAppend sends RPC, with entries to the given peer.
     */
    public void sendAppend(long to) {
        Progress pr = getProgress(to);
        if (pr.isPaused()) {
            return;
        }
        Message m = new Message();
        m.setTo(to);

        try {
            long term = raftLog.term(pr.next - 1);
            List<Entry> ents = raftLog.entries(pr.next, maxMsgSize);
            m.setType(MsgApp);
            m.setIndex(pr.next - 1);
            m.setLogTerm(term);
            m.setEntries(ents);
            m.setCommit(raftLog.committed);
            int n = m.getEntries().size();
            if (n != 0) {
                if (pr.state == Replicate) {
                    long last = m.getEntries().get(n - 1).getIndex();
                    pr.optimisticUpdate(last);
                    pr.ins.add(last);
                } else if (pr.state == Probe) {
                    pr.pause();
                } else {
                    logger.warn("{} is sending append in unhandled state {}", id, pr.state);
                }
            }
            send(m);
        } catch (Errors.RaftException e) {
            if (!pr.recentActive) {
                logger.debug("ignore sending snapshot to {} since it is not recently active", to);
                return;
            }

            m.setType(MsgSnap);
            try {
                Snapshot snapshot = raftLog.snapshot();
                if (isEmptySnap(snapshot)) {
                    throw new Errors.RaftException("need non-empty snapshot");
                }
                m.setSnapshot(snapshot);
                long sindex = snapshot.getMetadata().getIndex();
                long sterm = snapshot.getMetadata().getTerm();
                logger.debug("{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} [{}]",
                        id, raftLog.firstIndex(), raftLog.committed, sindex, sterm, to, pr);
                pr.becomeSnapshot(sindex);
                logger.debug("{} paused sending replication messages to {} [{}]", id, to, pr);
            } catch (Errors.RaftException e1) {
                if (e1 == ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE) {
                    logger.debug("{} failed to send snapshot to {} because snapshot is temporarily unavailable", id, to);
                    return;
                }
                throw e1;
            }
        }
    }

    /**
     * sendHeartbeat sends an empty MsgApp
     */
    public void sendHeartbeat(long to, byte[] ctx) {
        /*
         * Attach the commit as min(to.matched, r.committed).
         * When the leader sends out heartbeat message,
         * the receiver(follower) might not be matched with the leader
         * or it might not have all the committed entries.
         * The leader MUST NOT forward the follower's commit to
         * an unmatched index.
         */
        long commit = min(getProgress(to).match, raftLog.committed);
        Message m = new Message();
        m.setTo(to);
        m.setType(MsgHeartbeat);
        m.setCommit(commit);
        m.setContext(ctx);
        send(m);
    }

    public void forEachProgress(BiFunction<Long, Progress, Void> f) {
        for (Map.Entry<Long, Progress> entry : prs.entrySet()) {
            f.apply(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Progress> entry : learnerPrs.entrySet()) {
            f.apply(entry.getKey(), entry.getValue());
        }
    }

    /**
     * bcastAppend sends RPC, with entries to all peers that are not up-to-date
     * according to the progress recorded in r.prs.
     */
    public void bcastAppend() {
        forEachProgress((id, pr) -> {
            if (this.id == id) {
                return null;
            }
            sendAppend(id);
            return null;
        });
    }

    /**
     * bcastHeartbeat sends RPC, without entries to all the peers.
     */
    public void bcastHeartbeat() {
        String lastCtx = readOnly.lastPendingRequestCtx();
        if (lastCtx.length() == 0) {
            bcastHeartbeatWithCtx(null);
        } else {
            bcastHeartbeatWithCtx(lastCtx.getBytes());
        }
    }

    public void bcastHeartbeatWithCtx(byte[] ctx) {
        forEachProgress((id, pr) -> {
            if (this.id == id) {
                return null;
            }
            sendHeartbeat(id, ctx);
            return null;
        });
    }

    /**
     * maybeCommit attempts to advance the commit index. Returns true if
     * the commit index changed (in which case the caller should call
     * r.bcastAppend).
     */
    public boolean maybeCommit() {
        // TODO(bmizerany): optimize.. Currently naive
        List<Long> mis = new ArrayList<>(prs.size());
        for (Map.Entry<Long, Progress> entry : prs.entrySet()) {
            mis.add(entry.getValue().match);
        }

        Collections.sort(mis, Collections.reverseOrder());
        Long mci = mis.get(quorum() - 1);
        return raftLog.maybeCommit(mci, term);
    }

    public void reset(long term) {
        if (this.term != term) {
            this.term = term;
            this.vote = None;
        }
        this.lead = None;

        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;
        resetRandomizedElectionTimeout();

        abortLeaderTransfer();

        this.votes = new HashMap<>();
        forEachProgress((id, pr) -> {
            pr.next = raftLog.lastIndex() + 1;
            pr.ins = new Progress.Inflights(maxInflight);
            if (Raft.this.id == id) {
                pr.match = raftLog.lastIndex();
            }
            return null;
        });

        pendingConfIndex = 0;
        readOnly = new ReadOnly(readOnly.option);
    }

    public void appendEntry(List<Entry> es) {
        long li = raftLog.lastIndex();
        for (int i = 0; i < es.size(); i++) {
            es.get(i).setTerm(term);
            es.get(i).setIndex(li + 1 + i);
        }
        // use latest "last" index after truncate/append
        li = raftLog.append(es);
        getProgress(id).maybeUpdate(li);
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        maybeCommit();
    }

    public Supplier<Void> tickElection = () -> {
        electionElapsed++;

        if (promotable() && pastElectionTimeout()) {
            electionElapsed = 0;
            Message msg = new Message();
            msg.setFrom(id);
            msg.setType(MsgHup);
            step(msg);
        }
        return null;
    };

    /**
     * tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
     */
    public Supplier<Void> tickHeartbeat = () -> {
        heartbeatElapsed++;
        electionElapsed++;

        if (electionElapsed >= electionTimeout) {
            electionElapsed = 0;
            if (checkQuorum) {
                Message msg = new Message();
                msg.setFrom(id);
                msg.setType(MsgCheckQuorum);
                step(msg);
            }
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (state == Leader && leadTransferee != None) {
                abortLeaderTransfer();
            }
        }

        if (state != Leader) {
            return null;
        }

        if (heartbeatElapsed >= heartbeatTimeout) {
            heartbeatElapsed = 0;
            Message msg = new Message();
            msg.setFrom(id);
            msg.setType(MsgBeat);
            step(msg);
        }
        return null;
    };

    public void becomeFollower(long term, long lead) {
        this.step = stepFollower;
        reset(term);
        this.tick = tickElection;
        this.lead = lead;
        this.state = Follower;
        logger.info("{} became follower at term {}", id, term);
    }

    public void becomeCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == Leader) {
            throw new Errors.RaftException("invalid transition [leader -> candidate]");
        }
        this.step = stepCandidate;
        reset(term + 1);
        this.tick = tickElection;
        this.vote = id;
        this.state = Candidate;
        logger.info("{} became candidate at term {}", id, term);
    }

    public void becomePreCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == Leader) {
            throw new Errors.RaftException("invalid transition [leader -> pre-candidate]");
        }
        /*
         * Becoming a pre-candidate changes our step functions and state,
         * but doesn't change anything else. In particular it does not increase
         * r.Term or change r.Vote.
         */
        this.step = stepCandidate;
        this.votes = new HashMap<>();
        this.tick = tickElection;
        this.state = PreCandidate;
        logger.info("{} became pre-candidate at term {}", id, term);
    }

    public void becomeLeader() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == Follower) {
            throw new Errors.RaftException("invalid transition [follower -> leader]");
        }
        this.step = stepLeader;
        reset(term);
        this.tick = tickHeartbeat;
        this.lead = id;
        this.state = Leader;
        List<Entry> ents = raftLog.entries(raftLog.committed + 1, noLimit);

        /*
         * Conservatively set the pendingConfIndex to the last index in the
         * log. There may or may not be a pending config change, but it's
         * safe to delay any future proposals until we commit all our
         * pending log entries, and scanning the entire tail of the log
         * could be expensive.
         */
        if (ents.size() > 0) {
            pendingConfIndex = ents.get(ents.size() - 1).getIndex();
        }

        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry());
        appendEntry(entries);
        logger.info("{} became leader at term {}", id, term);
    }

    public void campaign(String t) {
        long term = 0L;
        MessageType voteMsg = null;
        if (t == campaignPreElection) {
            becomePreCandidate();
            voteMsg = MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented r.Term.
            term = this.term + 1;
        } else {
            becomeCandidate();
            voteMsg = MsgVote;
            term = this.term;
        }
        if (quorum() == poll(id, voteRespMsgType(voteMsg), true)) {
            /*
             * We won the election after voting for ourselves (which must mean that
             * this is a single-node cluster). Advance to the next state.
             */
            if (t == campaignPreElection) {
                campaign(campaignElection);
            } else {
                becomeLeader();
            }
            return;
        }

        for (Map.Entry<Long, Progress> entry : prs.entrySet()) {
            if (this.id == entry.getKey()) {
                continue;
            }
            logger.info("{} [logterm: {}, index: {}] sent {} request to {} at term {}",
                    id, raftLog.lastTerm(), raftLog.lastIndex(), voteMsg, id, this.term);

            byte[] ctx = null;
            if (t == campaignTransfer) {
                ctx = t.getBytes();
            }
            Message msg = new Message();
            msg.setTerm(term);
            msg.setTo(id);
            msg.setType(voteMsg);
            msg.setIndex(raftLog.lastIndex());
            msg.setLogTerm(raftLog.lastTerm());
            msg.setContext(ctx);
            send(msg);
        }
    }

    public int poll(long id, MessageType t, boolean v) {
        if (v) {
            logger.info("{} received {} from {} at term {}", id, t, id, term);
        } else {
            logger.info("{} received {} rejection from {} at term {}", id, t, id, term);
        }
        if (!votes.containsKey(id)) {
            votes.put(id, v);
        }

        int r = 0;
        for (Map.Entry<Long, Boolean> entry : votes.entrySet()) {
            if (entry.getValue()) {
                r++;
            }
        }

        return r;
    }

    public void step(Message m) {
        // Handle the message term, which may result in our stepping down to a follower.
        if (m.getTerm() == 0) {
            // local message
        } else if (m.getTerm() > this.term) {
            if (m.getType() == MsgVote || m.getType() == MsgPreVote) {
                boolean force = Arrays.equals(m.getContext(), campaignTransfer.getBytes());
                boolean inLease = checkQuorum && lead != None && electionElapsed < electionTimeout;
                if (!force && inLease) {
                    // If a server receives a RequestVote request within the minimum election timeout
                    // of hearing from a current leader, it does not update its term or grant its vote
                    logger.info("{} [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: lease is not expired (remaining ticks: {})",
                            id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), term, electionTimeout - electionElapsed);
                    return;
                }
            }

            if (m.getType() == MsgPreVote) {
                // Never change our term in response to a PreVote
            } else if (m.getType() == MsgPreVoteResp && !m.isReject()) {
                /*
                 * We send pre-vote requests with a term in our future. If the
                 * pre-vote is granted, we will increment our term when we get a
                 * quorum. If it is not, the term comes from the node that
                 * rejected our vote so we should become a follower at the new
                 * term.
                 */
            } else {
                logger.info("{} [term: {}] received a {} message with higher term from {} [term: {}]",
                        id, term, m.getType(), m.getFrom(), m.getTerm());
                if (m.getType() == MsgApp || m.getType() == MsgHeartbeat || m.getType() == MsgSnap) {
                    becomeFollower(m.getTerm(), m.getFrom());
                } else {
                    becomeFollower(m.getTerm(), None);
                }
            }
        } else if (m.getTerm() < term) {
            if ((checkQuorum || preVote) && (m.getType() == MsgHeartbeat || m.getType() == MsgApp)) {
                /*
                 * We have received messages from a leader at a lower term. It is possible
                 * that these messages were simply delayed in the network, but this could
                 * also mean that this node has advanced its term number during a network
                 * partition, and it is now unable to either win an election or to rejoin
                 * the majority on the old term. If checkQuorum is false, this will be
                 * handled by incrementing term numbers in response to MsgVote with a
                 * higher term, but if checkQuorum is true we may not advance the term on
                 * MsgVote and must generate other messages to advance the term. The net
                 * result of these two features is to minimize the disruption caused by
                 * nodes that have been removed from the cluster's configuration: a
                 * removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                 * but it will not receive MsgApp or MsgHeartbeat, so it will not create
                 * disruptive term increases
                 * The above comments also true for Pre-Vote
                 */
                Message msg = new Message();
                msg.setTo(m.getFrom());
                msg.setType(MsgAppResp);
                send(msg);
            } else if (m.getType() == MsgPreVote) {
                /*
                 * Before Pre-Vote enable, there may have candidate with higher term,
                 * but less log. After update to Pre-Vote, the cluster may deadlock if
                 * we drop messages with a lower term.
                 */
                logger.info("{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), term);
                Message msg = new Message();
                msg.setTo(m.getFrom());
                msg.setTerm(term);
                msg.setType(MsgPreVoteResp);
                msg.setReject(true);
                send(msg);
            } else {
                // ignore other cases
                logger.info("{} [term: {}] ignored a {} message with lower term from {} [term: {}]",
                        id, term, m.getType(), m.getFrom(), m.getTerm());
            }
            return;
        }

        switch (m.getType()) {
            case MsgHup:
                if (state != Leader) {
                    List<Entry> ents = raftLog.slice(raftLog.applied + 1, raftLog.committed + 1, noLimit);
                    int n = numOfPendingConf(ents);
                    if (n != 0 && raftLog.committed > raftLog.applied) {
                        logger.warn("{} cannot campaign at term {} since there are still {} pending configuration changes to apply", id, term, n);
                        return;
                    }

                    logger.info("{} is starting a new election at term {}", id, term);
                    if (preVote) {
                        campaign(campaignPreElection);
                    } else {
                        campaign(campaignElection);
                    }
                } else {
                    logger.debug("{} ignoring MsgHup because already leader", id);
                }
                break;

            case MsgVote:
            case MsgPreVote:
                if (isLearner) {
                    // TODO: learner may need to vote, in case of node down when confchange.
                    logger.info("{} [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: learner can not vote",
                            id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), term);
                    return;
                }
                // We can vote if this is a repeat of a vote we've already cast...
                boolean canVote = vote == m.getFrom() ||
                        // ...we haven't voted and we don't think there's a leader yet in this term...
                        (vote == None && lead == None) ||
                        // ...or this is a PreVote for a future term...
                        (m.getType() == MsgPreVote && m.getTerm() > term);
                // ...and we believe the candidate is up to date.
                if (canVote && raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {
                    logger.info("{} [logterm: {}, index: {}, vote: {}] cast {} for {} [logterm: {}, index: {}] at term {}",
                            id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), term);
                    /*
                     * When responding to Msg{Pre,}Vote messages we include the term
                     * from the message, not the local term. To see why consider the
                     * case where a single node was previously partitioned away and
                     * it's local term is now of date. If we include the local term
                     * (recall that for pre-votes we don't update the local term), the
                     * (pre-)campaigning node on the other end will proceed to ignore
                     * the message (it ignores all out of date messages).
                     * The term in the original message and current local term are the
                     * same in the case of regular votes, but different for pre-votes.
                     */
                    Message msg = new Message();
                    msg.setTo(m.getFrom());
                    msg.setTerm(m.getTerm());
                    msg.setType(voteRespMsgType(m.getType()));
                    send(msg);
                    if (m.getType() == MsgVote) {
                        // Only record real votes.
                        electionElapsed = 0;
                        vote = m.getFrom();
                    }
                } else {
                    logger.info("{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                            id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), term);
                    Message msg = new Message();
                    msg.setTo(m.getFrom());
                    msg.setTerm(term);
                    msg.setType(voteRespMsgType(m.getType()));
                    msg.setReject(true);
                    send(msg);
                }
                break;
            default:
                this.step.apply(this, m);
        }
    }

    public BiFunction<Raft, Message, Void> stepLeader = (r, m) -> {
        // These message types do not require any progress for m.From.
        switch (m.getType()) {
            case MsgBeat:
                r.bcastHeartbeat();
                return null;
            case MsgCheckQuorum:
                if (!r.checkQuorumActive()) {
                    logger.warn("{} stepped down to follower since quorum is not active", r.id);
                    r.becomeFollower(r.term, None);
                }
                return null;
            case MsgProp:
                if (m.getEntries().size() == 0) {
                    logger.warn("{} stepped empty MsgProp", r.id);
                }
                if (r.prs.containsKey(r.id)) {
                    /*
                     * If we are not currently a member of the range (i.e. this node
                     * was removed from the configuration while serving as leader),
                     * drop any new proposals.
                     */
                    throw ERR_PROPOSAL_DROPPED;
                }

                if (r.leadTransferee != None) {
                    logger.debug("{} [term {}] transfer leadership to {} is in progress; dropping proposal", r.id, r.term, r.leadTransferee);
                    throw ERR_PROPOSAL_DROPPED;
                }

                for (int i = 0; i < m.getEntries().size(); i++) {
                    Entry e = m.getEntries().get(i);
                    if (e.getType() == EntryConfChange) {
                        if (r.pendingConfIndex > r.raftLog.applied) {
                            logger.info("propose conf {} ignored since pending unapplied configuration [index {}, applied {}]",
                                    e, r.pendingConfIndex, r.raftLog.applied);
                            Entry ent = new Entry();
                            ent.setType(EntryNormal);
                            m.getEntries().set(i, ent);
                        } else {
                            r.pendingConfIndex = r.raftLog.lastIndex() + i + 1;
                        }
                    }
                }

                r.appendEntry(m.getEntries());
                r.bcastAppend();
                return null;
            case MsgReadIndex:
                if (r.quorum() > 1) {
                    if (r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.term) {
                        // Reject read only request when this leader has not committed any log entry at its term.
                        return null;
                    }

                    /*
                     * thinking: use an interally defined context instead of the user given context.
                     * We can express this in terms of the term and index instead of a user-supplied value.
                     * This would allow multiple reads to piggyback on the same message.
                     */
                    switch (r.readOnly.option) {
                        case Safe:
                            r.readOnly.addRequest(r.raftLog.committed, m);
                            r.bcastHeartbeatWithCtx(m.getEntries().get(0).getData());
                            break;
                        case LeaseBased:
                            long ri = r.raftLog.committed;
                            if (m.getFrom() == None || m.getFrom() == r.id) { // from local member
                                ReadState rs = new ReadState();
                                rs.index = r.raftLog.committed;
                                rs.requestCtx = m.getEntries().get(0).getData();
                                r.readStates.add(rs);
                            } else {
                                Message msg = new Message();
                                msg.setTo(m.getFrom());
                                msg.setType(MsgReadIndexResp);
                                msg.setIndex(ri);
                                msg.setEntries(m.getEntries());
                                r.send(msg);
                            }
                    }
                } else {
                    ReadState rs = new ReadState();
                    rs.index = r.raftLog.committed;
                    rs.requestCtx = m.getEntries().get(0).getData();
                    r.readStates.add(rs);
                }

                return null;
        }

        // All other message types require a progress for m.From (pr).
        Progress pr = r.getProgress(m.getFrom());
        if (pr == null) {
            logger.debug("{} no progress available for {}", r.id, m.getFrom());
            return null;
        }
        switch (m.getType()) {
            case MsgAppResp:
                pr.recentActive = true;

                if (m.isReject()) {
                    logger.debug("{} received msgApp rejection(lastindex: {}) from {} for index {}",
                            r.id, m.getRejectHint(), m.getFrom(), m.getIndex());
                    if (pr.maybeDecrTo(m.getIndex(), m.getRejectHint())) {
                        logger.debug("{} decreased progress of {} to [{}]", r.id, m.getFrom(), pr);
                        if (pr.state == Replicate) {
                            pr.becomeProbe();
                        }
                        r.sendAppend(m.getFrom());
                    }
                } else {
                    boolean oldPaused = pr.isPaused();
                    if (pr.maybeUpdate(m.getIndex())) {
                        if (pr.state == Probe) {
                            pr.becomeReplicate();
                        } else if (pr.state == Progress.ProgressState.Snapshot && pr.needSnapshotAbort()) {
                            logger.debug("{} snapshot aborted, resumed sending replication messages to {} [{}]", r.id, m.getFrom(), pr);
                            pr.becomeProbe();
                        } else if (pr.state == Replicate) {
                            pr.ins.freeTo(m.getIndex());
                        }

                        if (r.maybeCommit()) {
                            r.bcastAppend();
                        } else if (oldPaused) {
                            // update() reset the wait state on this node. If we had delayed sending an update before, send it now.
                            r.sendAppend(m.getFrom());
                        }
                        // Transfer leadership is in progress.
                        if (m.getFrom() == r.leadTransferee && pr.match == r.raftLog.lastIndex()) {
                            logger.info("{} sent MsgTimeoutNow to {} after received MsgAppResp", r.id, m.getFrom());
                            r.sendTimeoutNow(m.getFrom());
                        }
                    }
                }
                return null;
            case MsgHeartbeatResp:
                pr.recentActive = true;
                pr.resume();

                // free one slot for the full inflights window to allow progress.
                if (pr.state == Replicate && pr.ins.full()) {
                    pr.ins.freeFirstOne();
                }
                if (pr.match < r.raftLog.lastIndex()) {
                    r.sendAppend(m.getFrom());
                }

                if (r.readOnly.option != Safe || m.getContext().length == 0) {
                    return null;
                }

                int ackCount = r.readOnly.recvAck(m);
                if (ackCount < r.quorum()) {
                    return null;
                }

                List<ReadOnly.ReadIndexStatus> rss = r.readOnly.advance(m);
                for (ReadOnly.ReadIndexStatus rs : rss) {
                    Message req = rs.req;
                    if (req.getFrom() == None || req.getFrom() == r.id) { // from local member
                        ReadState readState = new ReadState();
                        readState.index = rs.index;
                        readState.requestCtx = req.getEntries().get(0).getData();
                        r.readStates.add(readState);
                    } else {
                        Message msg = new Message();
                        msg.setTo(req.getFrom());
                        msg.setType(MsgReadIndexResp);
                        msg.setIndex(rs.index);
                        msg.setEntries(req.getEntries());
                        r.send(msg);
                    }
                }
                return null;
            case MsgSnapStatus:
                if (pr.state != Progress.ProgressState.Snapshot) {
                    return null;
                }
                if (!m.isReject()) {
                    pr.becomeProbe();
                    logger.debug("{} snapshot succeeded, resumed sending replication messages to {} [{}]", r.id, m.getFrom(), pr);
                } else {
                    pr.snapshotFailure();
                    pr.becomeProbe();
                    logger.debug("{} snapshot failed, resumed sending replication messages to {} [{}]", r.id, m.getFrom(), pr);
                }
                /*
                 * If snapshot finish, wait for the msgAppResp from the remote node before sending
                 * out the next msgApp.
                 * If snapshot failure, wait for a heartbeat interval before next try
                 */
                pr.pause();
                return null;
            case MsgUnreachable:
                // During optimistic replication, if the remote becomes unreachable, there is huge probability that a MsgApp is lost.
                if (pr.state == Replicate) {
                    pr.becomeProbe();
                }
                logger.debug("{} failed to send message to {} because it is unreachable [{}]", r.id, m.getFrom(), pr);
                return null;
            case MsgTransferLeader:
                if (pr.isLearner) {
                    logger.debug("{} is learner. Ignored transferring leadership", r.id);
                    return null;
                }
                long leadTransferee = m.getFrom();
                long lastLeadTransferee = r.leadTransferee;
                if (lastLeadTransferee != None) {
                    if (lastLeadTransferee == leadTransferee) {
                        logger.info("{} [term {}] transfer leadership to {} is in progress, ignores request to same node {}",
                                r.id, r.term, leadTransferee, leadTransferee);
                        return null;
                    }
                    r.abortLeaderTransfer();
                    logger.info("{} [term {}] abort previous transferring leadership to {}", r.id, r.term, lastLeadTransferee);
                }
                if (leadTransferee == r.id) {
                    logger.debug("{} is already leader. Ignored transferring leadership to self", r.id);
                    return null;
                }
                // Transfer leadership to third party.
                logger.info("{} [term {}] starts to transfer leadership to {}", r.id, r.term, leadTransferee);
                // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
                r.electionElapsed = 0;
                r.leadTransferee = leadTransferee;
                if (pr.match == r.raftLog.lastIndex()) {
                    r.sendTimeoutNow(leadTransferee);
                    logger.info("{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log", r.id, leadTransferee, leadTransferee);
                } else {
                    r.sendAppend(leadTransferee);
                }
        }
        return null;
    };

    /**
     * stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
     * whether they respond to MsgVoteResp or MsgPreVoteResp.
     */
    public BiFunction<Raft, Message, Void> stepCandidate = (r, m) -> {
        /*
         * Only handle vote responses corresponding to our candidacy (while in
         * StateCandidate, we may get stale MsgPreVoteResp messages in this term from
         * our pre-candidate state).
         */
        MessageType myVoteRespType = null;
        if (r.state == PreCandidate) {
            myVoteRespType = MsgPreVoteResp;
        } else {
            myVoteRespType = MsgVoteResp;
        }
        switch (m.getType()) {
            case MsgProp:
                logger.info("{} no leader at term {}; dropping proposal", r.id, r.term);
                throw ERR_PROPOSAL_DROPPED;
            case MsgApp:
                r.becomeFollower(m.getTerm(), m.getFrom()); // always m.Term == r.Term
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.becomeFollower(m.getTerm(), m.getFrom()); // always m.Term == r.Term
                r.handleHeartbeat(m);
                break;
            case MsgSnap:
                r.becomeFollower(m.getTerm(), m.getFrom()); // always m.Term == r.Term
                r.handleSnapshot(m);
                break;
            case MsgTimeoutNow:
                logger.debug("{} [term {} state %v] ignored MsgTimeoutNow from {}", r.id, r.term, r.state, m.getFrom());
                break;
            default:
                if (m.getType() == myVoteRespType) {
                    int gr = r.poll(m.getFrom(), m.getType(), !m.isReject());
                    logger.info("{} [quorum:{}] has received {} {} votes and {} vote rejections", r.id, r.quorum(), gr, m.getType(), r.votes.size() - gr);
                    int quorum = r.quorum();
                    if (quorum == gr) {
                        if (r.state == PreCandidate) {
                            r.campaign(campaignElection);
                        } else {
                            r.becomeLeader();
                            r.bcastAppend();
                        }
                    } else if (quorum == r.votes.size() - gr) {
                        // pb.MsgPreVoteResp contains future term of pre-candidate
                        // m.Term > r.Term; reuse r.Term
                        r.becomeFollower(r.term, None);
                    }
                }

        }
        return null;
    };

    public BiFunction<Raft, Message, Void> stepFollower = (r, m) -> {
        switch (m.getType()) {
            case MsgProp:
                if (r.lead == None) {
                    logger.info("{} no leader at term {}; dropping proposal", r.id, r.term);
                    throw ERR_PROPOSAL_DROPPED;
                } else if (r.disableProposalForwarding) {
                    logger.info("{} not forwarding to leader {} at term {}; dropping proposal", r.id, r.lead, r.term);
                    throw ERR_PROPOSAL_DROPPED;
                }
                m.setTo(r.lead);
                r.send(m);
                break;
            case MsgApp:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleHeartbeat(m);
                break;
            case MsgSnap:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleSnapshot(m);
                break;
            case MsgTransferLeader:
                if (r.lead == None) {
                    logger.info("{} no leader at term {}; dropping leader transfer msg", r.id, r.term);
                    return null;
                }
                m.setTo(r.lead);
                r.send(m);
                break;
            case MsgTimeoutNow:
                if (r.promotable()) {
                    logger.info("{} [term {}] received MsgTimeoutNow from {} and starts an election to get leadership.", r.id, r.term, m.getFrom());
                    /*
                     * Leadership transfers never use pre-vote even if r.preVote is true; we
                     * know we are not recovering from a partition so there is no need for the
                     * extra round trip.
                     */
                    r.campaign(campaignTransfer);
                } else {
                    logger.info("{} received MsgTimeoutNow from {} but is not promotable", r.id, m.getFrom());
                }
                break;
            case MsgReadIndex:
                if (r.lead == None) {
                    logger.info("{} no leader at term {}; dropping index reading msg", r.id, r.term);
                    return null;
                }
                m.setTo(r.lead);
                r.send(m);
                break;
            case MsgReadIndexResp:
                if (m.getEntries().size() != 1) {
                    logger.error("{} invalid format of MsgReadIndexResp from {}, entries count: {}", r.id, m.getFrom(), m.getEntries().size());
                    return null;
                }
                ReadState readState = new ReadState();
                readState.index = m.getIndex();
                readState.requestCtx = m.getEntries().get(0).getData();
                r.readStates.add(readState);
        }
        return null;
    };

    public void handleAppendEntries(Message m) {
        if (m.getIndex() < raftLog.committed) {
            Message msg = new Message();
            msg.setTo(m.getFrom());
            msg.setType(MsgAppResp);
            msg.setIndex(raftLog.committed);
            send(msg);
            return;
        }

        Tuple2<Long, Boolean> tuple = raftLog.maybeAppend(m.getIndex(), m.getLogTerm(), m.getCommit(), m.getEntries());
        Long mlastIndex = tuple.getV1();
        Boolean ok = tuple.getV2();
        if (ok) {
            Message msg = new Message();
            msg.setTo(m.getFrom());
            msg.setType(MsgAppResp);
            msg.setIndex(mlastIndex);
            send(msg);
        } else {
            logger.debug("{} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] from {}",
                    id, raftLog.zeroTermOnErrCompacted(raftLog.term(m.getIndex())), m.getIndex(), m.getLogTerm(), m.getIndex(), m.getFrom());
            Message msg = new Message();
            msg.setTo(m.getFrom());
            msg.setType(MsgAppResp);
            msg.setIndex(m.getIndex());
            msg.setReject(true);
            msg.setRejectHint(raftLog.lastIndex());
            send(msg);
        }
    }

    public void handleHeartbeat(Message m) {
        raftLog.commitTo(m.getCommit());
        Message msg = new Message();
        msg.setTo(m.getFrom());
        msg.setType(MsgHeartbeatResp);
        msg.setContext(m.getContext());
        send(msg);
    }

    public void handleSnapshot(Message m) {
        long sindex = m.getSnapshot().getMetadata().getIndex();
        long sterm = m.getSnapshot().getMetadata().getTerm();
        if (restore(m.getSnapshot())) {
            logger.info("{} [commit: {}] restored snapshot [index: {}, term: {}]",
                    this.id, raftLog.committed, sindex, sterm);
            Message msg = new Message();
            msg.setTo(m.getFrom());
            msg.setType(MsgAppResp);
            msg.setIndex(raftLog.lastIndex());
            send(msg);
        } else {
            logger.info("{} [commit: {}] ignored snapshot [index: {}, term: {}]",
                    this.id, raftLog.committed, sindex, sterm);
            Message msg = new Message();
            msg.setTo(m.getFrom());
            msg.setType(MsgAppResp);
            msg.setIndex(raftLog.committed);
            send(msg);
        }
    }

    /**
     * restore recovers the state machine from a snapshot. It restores the log and the
     * configuration of state machine.
     */
    public boolean restore(Snapshot s) {
        if (s.getMetadata().getIndex() <= raftLog.committed) {
            return false;
        }
        if (raftLog.matchTerm(s.getMetadata().getIndex(), s.getMetadata().getTerm())) {
            logger.info("{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
                    this.id, raftLog.committed, raftLog.lastIndex(), raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm());
            raftLog.commitTo(s.getMetadata().getIndex());
            return false;
        }

        // The normal peer can't become learner.
        if (!this.isLearner) {
            for (Long id : s.getMetadata().getConfState().getLearners()) {
                if (this.id == id) {
                    logger.error("{} can't become learner when restores snapshot [index: {}, term: {}]", this.id, s.getMetadata().getIndex(), s.getMetadata().getTerm());
                    return false;
                }
            }
        }

        logger.info("{} [commit: {}, lastindex: {}, lastterm: {}] starts to restore snapshot [index: {}, term: {}]",
                this.id, raftLog.committed, raftLog.lastIndex(), raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm());

        raftLog.restore(s);
        this.prs = new HashMap<>();
        this.learnerPrs = new HashMap<>();
        restoreNode(s.getMetadata().getConfState().getNodes(), false);
        restoreNode(s.getMetadata().getConfState().getLearners(), true);
        return true;
    }

    public void restoreNode(List<Long> nodes, boolean isLearner) {
        for (Long n : nodes) {
            long match = 0;
            long next = raftLog.lastIndex() + 1;
            if (n == this.id) {
                match = next - 1;
                this.isLearner = isLearner;
            }
            setProgress(n, match, next, isLearner);
            logger.info("{} restored progress of {} [{}]", this.id, n, getProgress(n));
        }
    }

    /**
     * promotable indicates whether state machine can be promoted to leader,
     * which is true when its own id is in progress list.
     */
    public boolean promotable() {
        return prs.containsKey(id);
    }

    public void addNode(long id) {
        addNodeOrLearnerNode(id, false);
    }

    public void addLearner(long id) {
        addNodeOrLearnerNode(id, true);
    }

    public void addNodeOrLearnerNode(long id, boolean isLearner) {
        Progress pr = getProgress(id);
        if (pr == null) {
            setProgress(id, 0, raftLog.lastIndex() + 1, isLearner);
        } else {
            if (isLearner && !pr.isLearner) {
                // can only change Learner to Voter
                logger.info("{} ignored addLearner: do not support changing {} from raft peer to learner.", this.id, id);
                return;
            }

            if (isLearner == pr.isLearner) {
                /*
                 * Ignore any redundant addNode calls (which can happen because the
                 * initial bootstrapping entries are applied twice).
                 */
                return;
            }

            // change Learner to Voter, use origin Learner progress
            learnerPrs.remove(id);
            pr.isLearner = false;
            prs.put(id, pr);
        }

        if (this.id == id) {
            this.isLearner = isLearner;
        }

        /*
         * When a node is first added, we should mark it as recently active.
         * Otherwise, CheckQuorum may cause us to step down if it is invoked
         * before the added node has a chance to communicate with us.
         */
        pr = getProgress(id);
        pr.recentActive = true;
    }

    public void removeNode(long id) {
        delProgress(id);

        // do not try to commit or abort transferring if there is no nodes in the cluster.
        if (prs.size() == 0 && learnerPrs.size() == 0) {
            return;
        }

        // The quorum size is now smaller, so see if any pending entries can be committed.
        if (maybeCommit()) {
            bcastAppend();
        }
        // If the removed node is the leadTransferee, then abort the leadership transferring.
        if (state == Leader && leadTransferee == id) {
            abortLeaderTransfer();
        }
    }

    public void setProgress(long id, long match, long next, boolean isLearner) {
        if (!isLearner) {
            learnerPrs.remove(id);
            Progress progress = new Progress();
            progress.next = next;
            progress.match = match;
            progress.ins = new Progress.Inflights(maxInflight);
            prs.put(id, progress);
            return;
        }

        if (prs.containsKey(id)) {
            throw new Errors.RaftException(this.id + " unexpected changing from voter to learner for " + id);
        }
        Progress progress = new Progress();
        progress.next = next;
        progress.match = match;
        progress.ins = new Progress.Inflights(maxInflight);
        progress.isLearner = true;
        learnerPrs.put(id, progress);
    }

    public void delProgress(long id) {
        prs.remove(id);
        learnerPrs.remove(id);
    }

    public void loadState(HardState state) {
        if (state.getCommit() < raftLog.committed || state.getCommit() > raftLog.lastIndex()) {
            logger.warn("{} state.commit {} is out of range [{}, {}]", id, state.getCommit(), raftLog.committed, raftLog.lastIndex());
        }
        raftLog.committed = state.getCommit();
        this.term = state.getTerm();
        this.vote = state.getVote();
    }

    /**
     * pastElectionTimeout returns true iff r.electionElapsed is greater
     * than or equal to the randomized election timeout in
     * [electiontimeout, 2 * electiontimeout - 1].
     */
    public boolean pastElectionTimeout() {
        return electionElapsed >= randomizedElectionTimeout;
    }

    public void resetRandomizedElectionTimeout() {
        randomizedElectionTimeout = electionTimeout + ThreadLocalRandom.current().nextInt(electionTimeout);
    }

    /**
     * checkQuorumActive returns true if the quorum is active from
     * the view of the local raft state machine. Otherwise, it returns
     * false.
     * checkQuorumActive also resets all RecentActive to false.
     */
    public boolean checkQuorumActive() {
        final AtomicInteger act = new AtomicInteger(0);

        forEachProgress((id, pr) -> {
            if (this.id == id) { // self is always active
                act.incrementAndGet();
                return null;
            }

            if (pr.recentActive && !pr.isLearner) {
                act.incrementAndGet();
            }

            pr.recentActive = false;
            return null;
        });

        return act.get() >= quorum();
    }

    public void sendTimeoutNow(long to) {
        Message msg = new Message();
        msg.setTo(to);
        msg.setType(MsgTimeoutNow);
        send(msg);
    }

    public void abortLeaderTransfer() {
        leadTransferee = None;
    }

    public static int numOfPendingConf(List<Entry> ents) {
        int n = 0;
        for (Entry e : ents) {
            if (e.getType() == EntryConfChange) {
                n++;
            }
        }
        return n;
    }

    public static class SoftState {

        /**
         * must use atomic operations to access; keep 64-bit aligned.
         */
        public long lead;

        public StateRole raftState;

        @Override
        public String toString() {
            return "SoftState{" +
                    "lead=" + lead +
                    ", raftState=" + raftState +
                    '}';
        }

        public boolean equal(SoftState b) {
            return lead == b.lead && raftState == b.raftState;
        }
    }

    /**
     * Config contains the parameters to start a raft.
     */
    public static class Config {

        /**
         * ID is the identity of the local raft. ID cannot be 0.
         */
        public long id;

        /**
         * peers contains the IDs of all nodes (including self) in the raft cluster. It
         * should only be set when starting a new raft cluster. Restarting raft from
         * previous configuration will panic if peers is set. peer is private and only
         * used for testing right now.
         */
        public List<Long> peers;

        /**
         * learners contains the IDs of all learner nodes (including self if the
         * local node is a learner) in the raft cluster. learners only receives
         * entries from the leader node. It does not vote or promote itself.
         */
        public List<Long> learners;

        /**
         * ElectionTick is the number of Node.Tick invocations that must pass between
         * elections. That is, if a follower does not receive any message from the
         * leader of current term before ElectionTick has elapsed, it will become
         * candidate and start an election. ElectionTick must be greater than
         * HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
         * unnecessary leader switching.
         */
        public int electionTick;

        /**
         * HeartbeatTick is the number of Node.Tick invocations that must pass between
         * heartbeats. That is, a leader sends heartbeat messages to maintain its
         * leadership every HeartbeatTick ticks.
         */
        public int heartbeatTick;

        /**
         * Storage is the storage for raft. raft generates entries and states to be
         * stored in storage. raft reads the persisted entries and states out of
         * Storage when it needs. raft reads out the previous state and configuration
         * out of storage when restarting.
         */
        public Storage storage;

        /**
         * Applied is the last applied index. It should only be set when restarting
         * raft. raft will not return entries to the application smaller or equal to
         * Applied. If Applied is unset when restarting, raft might return previous
         * applied entries. This is a very application dependent configuration.
         */
        public long applied;

        /**
         * MaxSizePerMsg limits the max size of each append message. Smaller value
         * lowers the raft recovery cost(initial probing and message lost during normal
         * operation). On the other side, it might affect the throughput during normal
         * replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
         * message.
         */
        public long maxSizePerMsg;

        /**
         * MaxInflightMsgs limits the max number of in-flight append messages during
         * optimistic replication phase. The application transportation layer usually
         * has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
         * overflowing that sending buffer. TODO (xiangli): feedback to application to
         * limit the proposal rate?
         */
        public int maxInflightMsgs;

        /**
         * CheckQuorum specifies if the leader should check quorum activity. Leader
         * steps down when quorum is not active for an electionTimeout.
         */
        public boolean checkQuorum;

        /**
         * PreVote enables the Pre-Vote algorithm described in raft thesis section
         * 9.6. This prevents disruption when a node that has been partitioned away
         * rejoins the cluster.
         */
        public boolean preVote;

        /**
         * ReadOnlyOption specifies how the read only request is processed.
         * <p>
         * Safe guarantees the linearizability of the read only request by
         * communicating with the quorum. It is the default and suggested option.
         * <p>
         * LeaseBased ensures linearizability of the read only request by
         * relying on the leader lease. It can be affected by clock drift.
         * If the clock drift is unbounded, leader might keep the lease longer than it
         * should (clock can move backward/pause without any bound). ReadIndex is not safe
         * in that case.
         * CheckQuorum MUST be enabled if ReadOnlyOption is LeaseBased.
         */
        public ReadOnly.ReadOnlyOption readOnlyOption;

        /**
         * DisableProposalForwarding set to true means that followers will drop
         * proposals, rather than forwarding them to the leader. One use case for
         * this feature would be in a situation where the Raft leader is used to
         * compute the data of a proposal, for example, adding a timestamp from a
         * hybrid logical clock to data in a monotonically increasing way. Forwarding
         * should be disabled to prevent a follower with an innaccurate hybrid
         * logical clock from assigning the timestamp and then forwarding the data
         * to the leader.
         */
        public boolean disableProposalForwarding;

        public void validate() {
            if (id == None) {
                throw new Errors.RaftConfigException("cannot use none as id");
            }

            if (heartbeatTick <= 0) {
                throw new Errors.RaftConfigException("heartbeat tick must be greater than 0");
            }

            if (electionTick <= heartbeatTick) {
                throw new Errors.RaftConfigException("election tick must be greater than heartbeat tick");
            }

            if (storage == null) {
                throw new Errors.RaftConfigException("storage cannot be nil");
            }

            if (maxInflightMsgs <= 0) {
                throw new Errors.RaftConfigException("max inflight messages must be greater than 0");
            }

            if (readOnlyOption == LeaseBased && !checkQuorum) {
                throw new Errors.RaftConfigException("CheckQuorum must be enabled when ReadOnlyOption is LeaseBased");
            }
        }
    }
}
