package com.moilioncircle.raft;

import com.moilioncircle.raft.Log.RaftLog;
import com.moilioncircle.raft.Node.SoftState;
import com.moilioncircle.raft.ReadOnly.ReadState;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.entity.Message;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class Raft {
    public static final long StateFollower = 0;
    public static final long StateCandidate = 1;
    public static final long StateLeader = 2;
    public static final long StatePreCandidate = 3;
    public static final long NumStates = 4;

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

    public long state;

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

    public HardState hardState() {
        return new HardState(term, vote, raftLog.getCommitted());
    }

    public SoftState softState() {
        SoftState soft = new SoftState();
        soft.setLead(lead);
        soft.setRaftState(state);
        return soft;
    }
}
