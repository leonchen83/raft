package com.moilioncircle.raft;

public class Progress {
    private long match;
    private long next;

    /**
     * State defines how the leader should interact with the follower.
     *
     * When in ProgressStateProbe, leader sends at most one replication message
     * per heartbeat interval. It also probes actual progress of the follower.
     *
     * When in ProgressStateReplicate, leader optimistically increases next
     * to the latest entry sent after sending replication message. This is
     * an optimized state for fast replicating log entries to the follower.
     *
     * When in ProgressStateSnapshot, leader should have sent out snapshot
     * before and stops sending any replication message.
     */
    private long state;

    /**
     * Paused is used in ProgressStateProbe.
     * When Paused is true, raft should pause sending replication message to this peer.
     */
    private boolean paused;

    /**
     * PendingSnapshot is used in ProgressStateSnapshot.
     * If there is a pending snapshot, the pendingSnapshot will be set to the
     * index of the snapshot. If pendingSnapshot is set, the replication process of
     * this Progress will be paused. raft will not resend snapshot until the pending one
     * is reported to be failed.
     */
    private long pendingSnapshot;

    /**
     * RecentActive is true if the progress is recently active. Receiving any messages
     * from the corresponding follower indicates the progress is active.
     * RecentActive can be reset to false after an election timeout.
     */
    private boolean recentActive;

    /**
     * inflights is a sliding window for the inflight messages.
     * Each inflight message contains one or more log entries.
     * The max number of entries per message is defined in raft config as MaxSizePerMsg.
     * Thus inflight effectively limits both the number of inflight messages
     * and the bandwidth each Progress can use.
     * When inflights is full, no more message should be sent.
     * When a leader sends out a message, the index of the last
     * entry should be added to inflights. The index MUST be added
     * into inflights in order.
     * When a leader receives a reply, the previous inflights should
     * be freed by calling inflights.freeTo with the index of the last
     * received entry.
     */
    private Inflights ins;

    /**
     * IsLearner is true if this progress is tracked for a learner.
     */
    private boolean isLearner;

    public long getMatch() {
        return match;
    }

    public long getNext() {
        return next;
    }

    public long getState() {
        return state;
    }

    public boolean isPaused() {
        return paused;
    }

    public long getPendingSnapshot() {
        return pendingSnapshot;
    }

    public boolean isRecentActive() {
        return recentActive;
    }

    public Inflights getIns() {
        return ins;
    }

    public boolean isLearner() {
        return isLearner;
    }

    public static class Inflights {
        /**
         * the starting index in the buffer
         */
        private int start;

        /**
         * number of inflights in the buffer
         */
        private int count;

        /**
         * the size of the buffer
         */
        private int size;

        /**
         * buffer contains the index of the last entry
         * inside one message.
         */
        private long[] buffer;

        public int getStart() {
            return start;
        }

        public int getCount() {
            return count;
        }

        public int getSize() {
            return size;
        }

        public long[] getBuffer() {
            return buffer;
        }
    }
}
