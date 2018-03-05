package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.ConfState;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.entity.SnapshotMetadata;
import com.moilioncircle.raft.util.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.raft.util.Arrays.slice;

/**
 * Storage is an interface that may be implemented by the application
 * to retrieve log entries from storage.
 * <p>
 * If any Storage method returns an error, the raft instance will
 * become inoperable and refuse to participate in elections; the
 * application is responsible for cleanup and recovery in this case.
 */
public interface Storage {

    /**
     * ErrCompacted is returned by Storage.entries/compact when a requested
     * index is unavailable because it predates the last snapshot.
     */
    String ErrCompacted = "requested index is unavailable due to compaction";

    /**
     * ErrSnapOutOfDate is returned by Storage.createSnapshot when a requested
     * index is older than the existing snapshot.
     */
    String ErrSnapOutOfDate = "requested index is older than the existing snapshot";

    /**
     * ErrUnavailable is returned by Storage interface when the requested log entries
     * are unavailable.
     */
    String ErrUnavailable = "requested entry at index is unavailable";

    /**
     * ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
     * snapshot is temporarily unavailable.
     */
    String ErrSnapshotTemporarilyUnavailable = "snapshot is temporarily unavailable";

    /**
     * InitialState returns the saved HardState and ConfState information.
     */
    Tuple2<HardState, ConfState> initialState();

    /**
     * Entries returns a slice of log entries in the range [lo,hi).
     * MaxSize limits the total size of the log entries returned, but
     * Entries returns at least one entry if any.
     */
    List<Entry> entries(long lo, long hi, long maxSize);

    /**
     * Term returns the term of entry i, which must be in the range
     * [firstIndex()-1, lastIndex()]. The term of the entry before
     * FirstIndex is retained for matching purposes even though the
     * rest of that entry may not be available.
     */
    long term(long i);

    /**
     * lastIndex returns the index of the last entry in the log.
     */
    long lastIndex();

    /**
     * firstIndex returns the index of the first log entry that is
     * possibly available via Entries (older entries have been incorporated
     * into the latest Snapshot; if storage only contains the dummy entry the
     * first log entry is not available).
     */
    long firstIndex();

    /**
     * Snapshot returns the most recent snapshot.
     * If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
     * so raft state machine could know that Storage needs some time to prepare
     * snapshot and call Snapshot later.
     */
    Snapshot snapshot();

    static List<Entry> limitSize(List<Entry> ents, long maxSize) {
        if (ents.size() == 0) {
            return ents;
        }
        int c = 1;
        for (; c < ents.size(); c++) {
            if (c >= maxSize) {
                break;
            }
        }
        return slice(ents, 0, c);
    }

    /**
     * MemoryStorage implements the Storage interface backed by an
     * in-memory array.
     */
    class MemoryStorage implements Storage {
        private static final Logger logger = LoggerFactory.getLogger(MemoryStorage.class);

        protected HardState hardState;
        protected Snapshot snapshot;
        // ents[i] has raft log position i+snapshot.Metadata.Index
        protected List<Entry> ents;

        public MemoryStorage() {
            List<Entry> ents = new ArrayList<>();
            ents.add(new Entry());
            this.ents = ents;
            this.hardState = new HardState();
            this.snapshot = new Snapshot();
        }

        public MemoryStorage(List<Entry> ents) {
            this.ents = ents;
            this.hardState = new HardState();
            this.snapshot = new Snapshot();
        }

        public synchronized void setHardState(HardState hardState) {
            this.hardState = hardState;
        }

        @Override
        public Tuple2<HardState, ConfState> initialState() {
            return new Tuple2<>(hardState, snapshot.getMetadata().getConfState());
        }

        @Override
        public synchronized List<Entry> entries(long lo, long hi, long maxSize) {
            long offset = ents.get(0).getIndex();
            if (lo <= offset) {
                throw new RuntimeException(ErrCompacted);
            }
            if (hi > lastIndex() + 1) {
                logger.warn("entries' hi{} is out of bound lastindex{}", hi, lastIndex());
            }
            // only contains dummy entries.
            if (ents.size() == 1) {
                throw new RuntimeException(ErrUnavailable);
            }

            ents = slice(ents, (int) (lo - offset), (int) (hi - offset));
            return limitSize(ents, maxSize);
        }

        @Override
        public synchronized long term(long i) {
            long offset = ents.get(0).getIndex();
            if (i < offset) {
                throw new RuntimeException(ErrCompacted);
            }
            if ((int) (i - offset) >= ents.size()) {
                throw new RuntimeException(ErrUnavailable);
            }
            return ents.get((int) (i - offset)).getTerm();
        }

        @Override
        public synchronized long lastIndex() {
            return ents.get(0).getIndex() + ents.size() - 1;
        }

        @Override
        public synchronized long firstIndex() {
            return ents.get(0).getIndex() + 1;
        }

        @Override
        public synchronized Snapshot snapshot() {
            return snapshot;
        }

        /**
         * applySnapshot overwrites the contents of this Storage object with
         * those of the given snapshot.
         */
        public synchronized void applySnapshot(Snapshot snap) {
            //handle check for old snapshot being applied
            long msIndex = snapshot.getMetadata().getIndex();
            long snapIndex = snap.getMetadata().getIndex();
            if (msIndex >= snapIndex) {
                throw new RuntimeException(ErrSnapOutOfDate);
            }

            this.snapshot = snap;
            this.ents = new ArrayList<>();
            this.ents.add(new Entry(snap.getMetadata().getTerm(), snap.getMetadata().getIndex(), null, null));
        }

        /**
         * createSnapshot makes a snapshot which can be retrieved with Snapshot() and
         * can be used to reconstruct the state at that point.
         * If any configuration changes have been made since the last compaction,
         * the result of the last ApplyConfChange must be passed in.
         */
        public synchronized Snapshot createSnapshot(long i, ConfState cs, byte[] data) {
            if (i <= snapshot.getMetadata().getIndex()) {
                throw new RuntimeException(ErrSnapOutOfDate);
            }

            long offset = ents.get(0).getIndex();
            if (i > lastIndex()) {
                logger.warn("snapshot {} is out of bound lastindex({})", i, lastIndex());
            }

            SnapshotMetadata meta = new SnapshotMetadata();
            meta.setIndex(i);
            meta.setTerm(ents.get((int) (i - offset)).getTerm());
            if (cs != null) {
                meta.setConfState(cs);
            }
            snapshot = new Snapshot(data, meta);
            return snapshot;
        }

        /**
         * compact discards all log entries prior to compactIndex.
         * It is the application's responsibility to not attempt to compact an index
         * greater than raftLog.applied.
         */
        public synchronized void compact(long compactIndex) {
            long offset = ents.get(0).getIndex();
            if (compactIndex <= offset) {
                throw new RuntimeException(ErrCompacted);
            }
            if (compactIndex > lastIndex()) {
                logger.warn("compact {} is out of bound lastindex({})", compactIndex, lastIndex());
            }

            int i = (int) (compactIndex - offset);
            List<Entry> ents = new ArrayList<>();
            ents.add(new Entry(this.ents.get(i).getIndex(), this.ents.get(i).getTerm(), null, null));
            ents.addAll(slice(this.ents, i + 1, this.ents.size()));
            this.ents = ents;
        }

        /**
         * append the new entries to storage.
         * TODO (xiangli): ensure the entries are continuous and
         * entries[0].Index > ms.entries[0].Index
         */
        public synchronized void append(List<Entry> entries) {
            if (entries.size() == 0) {
                return;
            }
            long first = firstIndex();
            long last = entries.get(0).getIndex() + entries.size() - 1;

            // shortcut if there is no new entry.
            if (last < first) {
                return;
            }
            // truncate compacted entries
            if (first > entries.get(0).getIndex()) {
                entries = slice(entries, (int) (first - entries.get(0).getIndex()), entries.size());
            }

            long offset = entries.get(0).getIndex() - ents.get(0).getIndex();
            if (ents.size() > offset) {
                ents = slice(ents, 0, (int) offset);
                ents.addAll(entries);
            } else if (ents.size() == offset) {
                ents.addAll(entries);
            } else {
                logger.warn("missing log entry [last: {}, append at: {}]", lastIndex(), entries.get(0).getIndex());
            }
        }
    }
}
