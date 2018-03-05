package com.moilioncircle.raft;

import com.moilioncircle.raft.LogUnstable.*;

public class Log {

    public static class RaftLog {
        /**
         * storage contains all stable entries since the last snapshot.
         */
        private Storage storage;

        /**
         * unstable contains all unstable entries and snapshot.
         * they will be saved into storage.
         */
        private Unstable unstable;

        /**
         * committed is the highest log position that is known to be in
         * stable storage on a quorum of nodes.
         */
        private long committed;

        /**
         * applied is the highest log position that the application has
         * been instructed to apply to its state machine.
         * Invariant: applied <= committed
         */
        private long applied;

        public Storage getStorage() {
            return storage;
        }

        public Unstable getUnstable() {
            return unstable;
        }

        public long getCommitted() {
            return committed;
        }

        public long getApplied() {
            return applied;
        }
    }
}
