package com.moilioncircle.raft;

import com.moilioncircle.raft.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.moilioncircle.raft.Progress.ProgressState.Probe;
import static com.moilioncircle.raft.Progress.ProgressState.Replicate;
import static com.moilioncircle.raft.Progress.ProgressState.Snapshot;
import static java.lang.Math.min;

/**
 * Progress represents a follower’s progress in the view of the leader. Leader maintains
 * progresses of all followers, and sends entries to the follower based on its progress.
 */
public class Progress {

    private static final Logger logger = LoggerFactory.getLogger(Progress.class);

    public enum ProgressState {
        Probe,
        Replicate,
        Snapshot
    }

    public long match;

    public long next;

    /**
     * State defines how the leader should interact with the follower.
     * <p>
     * When in ProgressStateProbe, leader sends at most one replication message
     * per heartbeat interval. It also probes actual progress of the follower.
     * <p>
     * When in ProgressStateReplicate, leader optimistically increases next
     * to the latest entry sent after sending replication message. This is
     * an optimized state for fast replicating log entries to the follower.
     * <p>
     * When in ProgressStateSnapshot, leader should have sent out snapshot
     * before and stops sending any replication message.
     */
    public ProgressState state = Probe;

    /**
     * Paused is used in ProgressStateProbe.
     * When Paused is true, raft should pause sending replication message to this peer.
     */
    public boolean paused;

    /**
     * PendingSnapshot is used in ProgressStateSnapshot.
     * If there is a pending snapshot, the pendingSnapshot will be set to the
     * index of the snapshot. If pendingSnapshot is set, the replication process of
     * this Progress will be paused. raft will not resend snapshot until the pending one
     * is reported to be failed.
     */
    public long pendingSnapshot;

    /**
     * RecentActive is true if the progress is recently active. Receiving any messages
     * from the corresponding follower indicates the progress is active.
     * RecentActive can be reset to false after an election timeout.
     */
    public boolean recentActive;

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
    public Inflights ins;

    /**
     * IsLearner is true if this progress is tracked for a learner.
     */
    public boolean isLearner;

    public void resetState(ProgressState state) {
        this.paused = false;
        this.pendingSnapshot = 0;
        this.state = state;
        this.ins.reset();
    }

    public void becomeProbe() {
        /*
         * If the original state is ProgressStateSnapshot, progress knows that
         * the pending snapshot has been sent to this peer successfully, then
         * probes from pendingSnapshot + 1.
         */
        if (state == Snapshot) {
            long pendingSnapshot = this.pendingSnapshot;
            resetState(Probe);
            next = Math.max(match + 1, pendingSnapshot + 1);
        } else {
            resetState(Probe);
            next = match + 1;
        }
    }

    public void becomeReplicate() {
        resetState(Replicate);
        next = match + 1;
    }

    public void becomeSnapshot(long snapshoti) {
        resetState(Snapshot);
        pendingSnapshot = snapshoti;
    }

    /**
     * maybeUpdate returns false if the given n index comes from an outdated message.
     * Otherwise it updates the progress and returns true.
     */
    public boolean maybeUpdate(long n) {
        boolean updated = false;
        if (match < n) {
            match = n;
            updated = true;
            resume();
        }
        if (next < n + 1) {
            next = n + 1;
        }
        return updated;
    }

    public void optimisticUpdate(long n) {
        next = n + 1;
    }

    /**
     * maybeDecrTo returns false if the given to index comes from an out of order message.
     * Otherwise it decreases the progress next index to min(rejected, last) and returns true.
     */
    public boolean maybeDecrTo(long rejected, long last) {
        if (state == Replicate) {
            // the rejection must be stale if the progress has matched and "rejected" is smaller than "match".
            if (rejected <= match) {
                return false;
            }
            // directly decrease next to match + 1
            next = match + 1;
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if (next - 1 != rejected) {
            return false;
        }

        if ((next = min(rejected, last + 1)) < 1) {
            next = 1;
        }
        resume();
        return true;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    /**
     * IsPaused returns whether sending log entries to this node has been
     * paused. A node may be paused because it has rejected recent
     * MsgApps, is currently waiting for a snapshot, or has reached the
     * MaxInflightMsgs limit.
     */
    public boolean isPaused() {
        if (state == Probe) {
            return paused;
        } else if (state == Replicate) {
            return ins.full();
        } else if (state == Snapshot) {
            return true;
        } else {
            throw new Errors.RaftException("unexpected state");
        }
    }

    public void snapshotFailure() {
        pendingSnapshot = 0;
    }

    /**
     * needSnapshotAbort returns true if snapshot progress's Match
     * is equal or higher than the pendingSnapshot.
     */
    public boolean needSnapshotAbort() {
        return state == Snapshot && match >= pendingSnapshot;
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    public static class Inflights {
        /**
         * the starting index in the buffer
         */
        public int start;

        /**
         * number of inflights in the buffer
         */
        public int count;

        /**
         * the size of the buffer
         */
        public int size;

        /**
         * buffer contains the index of the last entry
         * inside one message.
         */
        public long[] buffer = new long[0];

        public Inflights(int size) {
            this.size = size;
        }

        /**
         * add adds an inflight into inflights
         */
        public void add(long inflight) {
            if (full()) {
                throw new Errors.RaftException("cannot add into a full inflights");
            }
            int next = start + count;
            int size = this.size;
            if (next >= size) {
                next -= size;
            }
            if (next >= buffer.length) {
                growBuf();
            }
            buffer[next] = inflight;
            count++;
        }

        /**
         * grow the inflight buffer by doubling up to inflights.size. We grow on demand
         * instead of preallocating to inflights.size to handle systems which have
         * thousands of Raft groups per process.
         */
        public void growBuf() {
            int newSize = buffer.length * 2;
            if (newSize == 0) {
                newSize = 1;
            } else if (newSize > size) {
                newSize = size;
            }
            long[] newBuffer = new long[newSize];
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            this.buffer = newBuffer;
        }

        /**
         * freeTo frees the inflights smaller or equal to the given `to` flight.
         */
        public void freeTo(long to) {
            if (count == 0 || to < buffer[start]) {
                // out of the left side of the window
                return;
            }

            int idx = start;
            int i = 0;
            for (i = 0; i < count; i++) {
                if (to < buffer[idx]) {
                    // found the first large inflight
                    break;
                }

                // increase index and maybe rotate
                int size = this.size;
                idx++;
                if (idx >= size) {
                    idx -= size;
                }
            }
            // free i inflights and set new start index
            count -= i;
            start = idx;
            if (count == 0) {
                // inflights is empty, reset the start index so that we don't grow the buffer unnecessarily.
                start = 0;
            }
        }

        public void freeFirstOne() {
            freeTo(buffer[start]);
        }

        /**
         * full returns true if the inflights is full.
         */
        public boolean full() {
            return count == size;
        }

        /**
         * resets frees all inflights.
         */
        public void reset() {
            count = 0;
            start = 0;
        }

        @Override
        public String toString() {
            return Strings.buildEx(this);
        }
    }
}
