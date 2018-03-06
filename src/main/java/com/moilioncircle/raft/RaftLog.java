package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.util.Arrays;
import com.moilioncircle.raft.util.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.moilioncircle.raft.Raft.noLimit;
import static com.moilioncircle.raft.Storage.ErrCompacted;
import static com.moilioncircle.raft.Storage.limitSize;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class RaftLog {

    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);

    /**
     * storage contains all stable entries since the last snapshot.
     */
    public Storage storage;

    /**
     * unstable contains all unstable entries and snapshot.
     * they will be saved into storage.
     */
    public LogUnstable unstable;

    /**
     * committed is the highest log position that is known to be in
     * stable storage on a quorum of nodes.
     */
    public long committed;

    /**
     * applied is the highest log position that the application has
     * been instructed to apply to its state machine.
     * Invariant: applied <= committed
     */
    public long applied;

    @Override
    public String toString() {
        return "RaftLog{" +
                "storage=" + storage +
                ", unstable=" + unstable +
                ", committed=" + committed +
                ", applied=" + applied +
                '}';
    }

    // newLog returns log using the given storage. It recovers the log to the state
    // that it just commits and applies the latest snapshot.
    public RaftLog(Storage storage) {
        Objects.requireNonNull(storage);
        this.storage = storage;
        long firstIndex = storage.firstIndex();
        long lastIndex = storage.lastIndex();
        unstable = new LogUnstable();
        unstable.offset = lastIndex + 1;
        // Initialize our committed and applied pointers to the time of the last compaction.
        committed = firstIndex - 1;
        applied = firstIndex - 1;
    }

    // maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries, true).
    public Tuple2<Long, Boolean> maybeAppend(long index, long logTerm, long committed, List<Entry> ents) {
        if (matchTerm(index, logTerm)) {
            long lastnewi = index + ents.size();
            long ci = findConflict(ents);
            if (ci == 0 || ci <= committed) {
                logger.warn("entry {} conflict with committed entry [committed({})]", ci, committed);
            } else {
                long offset = index + 1;
                append(Arrays.slice(ents, (int) (ci - offset), ents.size()));
            }
            commitTo(min(committed, lastnewi));
            return new Tuple2<>(lastnewi, true);
        }
        return new Tuple2<>(0L, false);
    }

    public long append(List<Entry> ents) {
        if (ents.size() == 0) {
            return lastIndex();
        }
        long after = ents.get(0).getIndex() - 1;
        if (after < committed) {
            logger.warn("after({}) is out of range [committed({})]", after, committed);
        }
        unstable.truncateAndAppend(ents);
        return lastIndex();
    }

    // findConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    public long findConflict(List<Entry> ents) {
        for (Entry ne : ents) {
            if (!matchTerm(ne.getIndex(), ne.getTerm())) {
                if (ne.getIndex() <= lastIndex()) {
                    logger.info("found conflict at index {} [existing term: {}, conflicting term: {}]",
                            ne.getIndex(), zeroTermOnErrCompacted(term(ne.getIndex())), ne.getTerm());
                }
                return ne.getIndex();
            }
        }
        return 0L;
    }

    public List<Entry> unstableEntries() {
        if (unstable.entries.size() == 0) {
            return null;
        }
        return unstable.entries;
    }

    // nextEnts returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    public List<Entry> nextEnts() {
        long off = max(applied + 1, firstIndex());
        if (committed + 1 > off) {
            List<Entry> ents = slice(off, committed + 1, noLimit);
//                if err != nil {
//                    l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
//                }
            return ents;
        }
        return null;
    }

    // hasNextEnts returns if there is any available entries for execution. This
    // is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
    public boolean hasNextEnts() {
        long off = max(applied + 1, firstIndex());
        return committed + 1 > off;
    }

    public Snapshot snapshot() {
        if (unstable.snapshot != null) {
            return unstable.snapshot;
        }
        return storage.snapshot();
    }

    public long firstIndex() {
        Tuple2<Long, Boolean> tuple = unstable.maybeFirstIndex();
        if (tuple.getV2()) {
            return tuple.getV1();
        }
        long index = storage.firstIndex();
//            if err != nil {
//                panic(err) // TODO(bdarnell)
//            }
        return index;
    }

    public long lastIndex() {
        Tuple2<Long, Boolean> tuple = unstable.maybeLastIndex();
        if (tuple.getV2()) {
            return tuple.getV1();
        }
        long index = storage.lastIndex();
//            if err != nil {
//                panic(err) // TODO(bdarnell)
//            }
        return index;
    }

    public void commitTo(long tocommit) {
        // never decrease commit
        if (committed < tocommit) {
            if (lastIndex() < tocommit) {
                logger.warn("tocommit({}) is out of range [lastIndex({})]. Was the raft log corrupted, truncated, or lost?", tocommit, lastIndex());
            }
            committed = tocommit;
        }
    }

    public void appliedTo(long i) {
        if (i == 0) {
            return;
        }
        if (committed < i || i < applied) {
            logger.warn("applied({}) is out of range [prevApplied({}), committed({})]", i, applied, committed);
        }
        applied = i;
    }

    public void stableTo(long i, long t) { unstable.stableTo(i, t); }

    public void stableSnapTo(long i) { unstable.stableSnapTo(i); }

    public long lastTerm() {
        long t = term(lastIndex());
//            if err != nil {
//                l.logger.Panicf("unexpected error when getting the last term (%v)", err)
//            }
        return t;
    }

    public long term(long i) {
        // the valid term range is [index of dummy entry, last index]
        long dummyIndex = firstIndex() - 1;
        if (i < dummyIndex || i > lastIndex()) {
            // TODO: return an error instead?
            return 0L;
        }

        Tuple2<Long, Boolean> tuple = unstable.maybeTerm(i);
        if (tuple.getV2()) {
            return tuple.getV1();
        }

        return storage.term(i);
//            if err == ErrCompacted || err == ErrUnavailable {
//                return 0, err
//            }
    }

    public List<Entry> entries(long i, long maxsize) {
        if (i > lastIndex()) {
            return null;
        }
        return slice(i, lastIndex() + 1, maxsize);
    }

    // allEntries returns all entries in the log.
    public List<Entry> allEntries() {
        List<Entry> ents = entries(firstIndex(), noLimit);
        return ents;
//            if err == ErrCompacted { // try again if there was a racing compaction
//                return l.allEntries()
//            }
//            // TODO (xiangli): handle error?
//            panic(err)
    }

    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    public boolean isUpToDate(long lasti, long term) {
        return term > lastTerm() || (term == lastTerm() && lasti >= lastIndex());
    }

    public boolean matchTerm(long i, long term) {
        long t = term(i);
//            if err != nil {
//                return false
//            }
        return t == term;
    }

    public boolean maybeCommit(long maxIndex, long term) {
        if (maxIndex > committed && zeroTermOnErrCompacted(term(maxIndex)) == term) {
            commitTo(maxIndex);
            return true;
        }
        return false;
    }

    public void restore(Snapshot s) {
        logger.info("log [{}] starts to restore snapshot [index: {}, term: {}]", this, s.getMetadata().getIndex(), s.getMetadata().getTerm());
        committed = s.getMetadata().getIndex();
        unstable.restore(s);
    }

    // slice returns a slice of log entries from lo through hi-1, inclusive.
    public List<Entry> slice(long lo, long hi, long maxSize) {
        mustCheckOutOfBounds(lo, hi);
//            if err != nil {
//                return nil, err
//            }
        if (lo == hi) {
            return null;
        }
        List<Entry> ents = new ArrayList<>();
        if (lo < unstable.offset) {
            List<Entry> storedEnts = storage.entries(lo, min(hi, unstable.offset), maxSize);
//                if err == ErrCompacted {
//                    return nil, err
//                } else if err == ErrUnavailable {
//                    l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
//                } else if err != nil {
//                    panic(err) // TODO(bdarnell)
//                }

            // check if ents has reached the size limitation
            if (storedEnts.size() < min(hi, unstable.offset) - lo) {
                return storedEnts;
            }
            ents = storedEnts;
        }
        if (hi > unstable.offset) {
            List<Entry> unstable = this.unstable.slice(max(lo, this.unstable.offset), hi);
            if (ents.size() > 0) {
                ents.addAll(unstable);
            } else {
                ents = unstable;
            }
        }
        return limitSize(ents, maxSize);
    }

    // l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
    public void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            logger.warn("invalid slice {} > {}", lo, hi);
        }
        long fi = firstIndex();
        if (lo < fi) {
            throw new RuntimeException(ErrCompacted);
        }

        long length = lastIndex() + 1 - fi;
        if (lo < fi || hi > fi + length) {
            logger.warn("slice[{},{}) out of bound [{},{}]", lo, hi, fi, lastIndex());
        }
    }

    public long zeroTermOnErrCompacted(long t) {
        return t;
//            if err == nil {
//                return t
//            }
//            if err == ErrCompacted {
//                return 0
//            }
//            logger.warn("unexpected error");
//            return 0L;
    }
}
