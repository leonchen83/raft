package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.util.Arrays;
import com.moilioncircle.raft.util.Strings;
import com.moilioncircle.raft.util.Tuples;
import com.moilioncircle.raft.util.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogUnstable {

    private static final Logger logger = LoggerFactory.getLogger(LogUnstable.class);

    /**
     * the incoming unstable snapshot, if any.
     */
    public Snapshot snapshot;

    /**
     * all entries that have not yet been written to storage.
     */
    public List<Entry> entries;

    public long offset;

    /**
     * maybeFirstIndex returns the index of the first possible entry in entries
     * if it has a snapshot.
     */
    public Tuple2<Long, Boolean> maybeFirstIndex() {
        if (snapshot != null) {
            return Tuples.of(snapshot.getMetadata().getIndex() + 1, true);
        }
        return Tuples.of(0L, false);
    }

    /**
     * maybeLastIndex returns the last index if it has at least one
     * unstable entry or snapshot.
     */
    public Tuple2<Long, Boolean> maybeLastIndex() {
        int l = entries.size();
        if (l != 0) {
            return Tuples.of(offset + l - 1, true);
        }
        if (snapshot != null) {
            return Tuples.of(snapshot.getMetadata().getIndex(), true);
        }
        return Tuples.of(0L, false);
    }

    /**
     * maybeTerm returns the term of the entry at index i, if there
     * is any.
     */
    public Tuple2<Long, Boolean> maybeTerm(long i) {
        if (i < offset) {
            if (snapshot == null) {
                return Tuples.of(0L, false);
            }
            if (snapshot.getMetadata().getIndex() == i) {
                return Tuples.of(snapshot.getMetadata().getTerm(), true);
            }
            return Tuples.of(0L, false);
        }

        Tuple2<Long, Boolean> tuple = maybeLastIndex();
        if (!tuple.getV2()) {
            return Tuples.of(0L, false);
        }
        if (i > tuple.getV1()) {
            return Tuples.of(0L, false);
        }
        return Tuples.of(entries.get((int) (i - offset)).getTerm(), true);
    }

    public void stableTo(long i, long t) {
        Tuple2<Long, Boolean> tuple = maybeTerm(i);
        if (!tuple.getV2()) {
            return;
        }
        /*
         * if i < offset, term is matched with the snapshot
         * only update the unstable entries if term is matched with
         * an unstable entry.
         */
        if (tuple.getV1().longValue() == t && i >= offset) {
            entries = Arrays.slice(entries, (int) (i + 1 - offset), entries.size());
            offset = i + 1;
        }
    }

    public void stableSnapTo(long i) {
        if (snapshot != null && snapshot.getMetadata().getIndex() == i) {
            snapshot = null;
        }
    }

    public void restore(Snapshot s) {
        offset = s.getMetadata().getIndex() + 1;
        entries = null;
        snapshot = s;
    }

    public void truncateAndAppend(List<Entry> ents) {
        long after = ents.get(0).getIndex();
        if (after == offset + entries.size()) {
            // after is the next index in the u.entries directly append
            entries.addAll(ents);
        } else if (after <= offset) {
            logger.info("replace the unstable entries from index {}", after);
            /*
             * The log is being truncated to before our current offset
             * portion, so set the offset and replace the entries
             */
            offset = after;
            entries = ents;
        } else {
            // truncate to after and copy to u.entries then append
            logger.info("truncate the unstable entries before index {}", after);
            entries = slice(offset, after);
            entries.addAll(ents);
        }
    }

    public List<Entry> slice(long lo, long hi) {
        mustCheckOutOfBounds(lo, hi);
        return Arrays.slice(entries, (int) (lo - offset), (int) (hi - offset));
    }

    /**
     * u.offset <= lo <= hi <= u.offset+len(u.entries)
     */
    public void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            throw new Errors.RaftException("invalid unstable.slice " + lo + " > " + hi);
        }
        long upper = offset + entries.size();
        if (lo < offset || hi > upper) {
            throw new Errors.RaftException("unstable.slice[" + lo + "," + hi + ") out of bound [" + offset + "," + upper + "]");
        }
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }
}
