package com.moilioncircle.raft;

import com.google.protobuf.ByteString;
import com.moilioncircle.raft.pb.RaftPb;
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
     * ErrCompacted is returned by Storage.Entries/Compact when a requested
     * index is unavailable because it predates the last snapshot.
     */
    String ErrCompacted = "requested index is unavailable due to compaction";

    /**
     * ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
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
    Tuple2<RaftPb.HardState, RaftPb.ConfState> initialState();

    /**
     * Entries returns a slice of log entries in the range [lo,hi).
     * MaxSize limits the total size of the log entries returned, but
     * Entries returns at least one entry if any.
     */
    List<RaftPb.Entry> entries(long lo, long hi, long maxSize);

    /**
     * Term returns the term of entry i, which must be in the range
     * [FirstIndex()-1, LastIndex()]. The term of the entry before
     * FirstIndex is retained for matching purposes even though the
     * rest of that entry may not be available.
     */
    long term(long i);

    /**
     * LastIndex returns the index of the last entry in the log.
     */
    long lastIndex();

    /**
     * FirstIndex returns the index of the first log entry that is
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
    RaftPb.Snapshot snapshot();

    static List<RaftPb.Entry> limitSize(List<RaftPb.Entry> ents, long maxSize) {
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

        private RaftPb.HardState hardState;
        private RaftPb.Snapshot snapshot;
        // ents[i] has raft log position i+snapshot.Metadata.Index
        private List<RaftPb.Entry> ents;

        public MemoryStorage() {
            ents = new ArrayList<>();
        }

        public MemoryStorage(List<RaftPb.Entry> ents) {
            this.ents = ents;
        }

        public synchronized void setHardState(RaftPb.HardState hardState) {
            this.hardState = hardState;
        }

        @Override
        public Tuple2<RaftPb.HardState, RaftPb.ConfState> initialState() {
            return new Tuple2<>(hardState, snapshot.getMetadata().getConfState());
        }

        @Override
        public synchronized List<RaftPb.Entry> entries(long lo, long hi, long maxSize) {
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
        public synchronized RaftPb.Snapshot snapshot() {
            return snapshot;
        }

        /**
         * ApplySnapshot overwrites the contents of this Storage object with
         * those of the given snapshot.
         */
        public synchronized void applySnapshot(RaftPb.Snapshot snap) {
            //handle check for old snapshot being applied
            long msIndex = snapshot.getMetadata().getIndex();
            long snapIndex = snap.getMetadata().getIndex();
            if (msIndex >= snapIndex) {
                throw new RuntimeException(ErrSnapOutOfDate);
            }

            this.snapshot = snap;
            this.ents = new ArrayList<>();
            this.ents.add(RaftPb.Entry.newBuilder().setTerm(snap.getMetadata().getTerm()).setIndex(snap.getMetadata().getIndex()).build());
        }

        /**
         * CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
         * can be used to reconstruct the state at that point.
         * If any configuration changes have been made since the last compaction,
         * the result of the last ApplyConfChange must be passed in.
         */
        public synchronized RaftPb.Snapshot createSnapshot(long i, RaftPb.ConfState cs, byte[] data) {
            if (i <= snapshot.getMetadata().getIndex()) {
                throw new RuntimeException(ErrSnapOutOfDate);
            }

            long offset = ents.get(0).getIndex();
            if (i > lastIndex()) {
                logger.warn("snapshot {} is out of bound lastindex({})", i, lastIndex());
            }

            RaftPb.SnapshotMetadata.Builder builder = snapshot.getMetadata().toBuilder();
            builder.setIndex(i).setTerm(ents.get((int) (i - offset)).getTerm());
            if (cs != null) {
                builder.setConfState(cs);
            }
            snapshot.toBuilder().setData(ByteString.copyFrom(data));
            return snapshot;
        }

        /**
         * Compact discards all log entries prior to compactIndex.
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
            List<RaftPb.Entry> ents = new ArrayList<>();
            ents.add(RaftPb.Entry.newBuilder().setIndex(this.ents.get(i).getIndex()).setTerm(this.ents.get(i).getTerm()).build());
            ents.addAll(slice(this.ents, i + 1, this.ents.size()));
            this.ents = ents;
        }

        /**
         * Append the new entries to storage.
         * TODO (xiangli): ensure the entries are continuous and
         * entries[0].Index > ms.entries[0].Index
         */
        public synchronized void append(List<RaftPb.Entry> entries) {
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
