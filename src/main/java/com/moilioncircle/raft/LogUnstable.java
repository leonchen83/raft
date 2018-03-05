package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.Snapshot;
import java.util.List;

public class LogUnstable {
    public static class Unstable {
        /**
         * the incoming unstable snapshot, if any.
         */
        private Snapshot snapshot;

        /**
         * all entries that have not yet been written to storage.
         */
        private List<Entry> entries;

        private long offset;

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public List<Entry> getEntries() {
            return entries;
        }

        public long getOffset() {
            return offset;
        }
    }
}
