package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.ConfState;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.entity.SnapshotMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.moilioncircle.raft.Errors.ERR_COMPACTED;
import static com.moilioncircle.raft.Errors.ERR_SNAP_OUT_OF_DATE;
import static com.moilioncircle.raft.Errors.ERR_UNAVAILABLE;
import static com.moilioncircle.raft.TestUtil.newEntry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Baoyi Chen
 * @since 1.0.0
 */
public class StorageTest {

    @Test
    public void testStorageTerm() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));

        class Test {
            public Test(long i, Exception werr, long wterm, boolean wpanic) {
                this.i = i;
                this.werr = werr;
                this.wterm = wterm;
                this.wpanic = wpanic;
            }

            public long i;
            public Exception werr;
            public long wterm;
            public boolean wpanic;
        }
        Test[] tests = new Test[] {
                new Test(2, ERR_COMPACTED, 0, false),
            new Test(3, null, 3, false),
            new Test(4, null, 4, false),
            new Test(5, null, 5, false),
                new Test(6, ERR_UNAVAILABLE, 0, false)
        };
        for (Test tt : tests) {
            try {
                Storage s = new Storage.MemoryStorage(ents);
                long term = s.term(tt.i);
                if (term != tt.wterm) {
                    fail();
                }
            } catch (Errors.RaftException e) {
                if (e != tt.werr) {
                    fail();
                }
            }
        }

    }

    @Test
    public void testStorageEntries() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        ents.add(newEntry(6L, 6L));
        class Test {
            public Test(long lo, long hi, long maxsize, Exception werr, List<Entry> wentries) {
                this.lo = lo;
                this.hi = hi;
                this.maxsize = maxsize;
                this.werr = werr;
                this.wentries = wentries;
            }

            public long lo, hi, maxsize;
            public Exception werr;
            public List<Entry> wentries;
        }
        Test[] tests = new Test[] {
                new Test(2, 6, Long.MAX_VALUE, ERR_COMPACTED, null),
                new Test(3, 4, Long.MAX_VALUE, ERR_COMPACTED, null),
                new Test(4, 5, Long.MAX_VALUE, ERR_COMPACTED, Arrays.asList(newEntry(4L, 4L))),
                new Test(4, 6, Long.MAX_VALUE, ERR_COMPACTED, Arrays.asList(
                        newEntry(4L, 4L),
                        newEntry(5L, 5L))),
                new Test(4, 7, Long.MAX_VALUE, ERR_COMPACTED, Arrays.asList(
                        newEntry(4L, 4L),
                        newEntry(5L, 5L),
                        newEntry(6L, 6L))),
                new Test(4, 7, 0, ERR_COMPACTED, Arrays.asList(
                        newEntry(4L, 4L))),
                new Test(4, 7, 2, ERR_COMPACTED, Arrays.asList(
                        newEntry(4L, 4L),
                        newEntry(5L, 5L))),
                new Test(4, 7, 3, ERR_COMPACTED, Arrays.asList(
                        newEntry(4L, 4L),
                        newEntry(5L, 5L),
                        newEntry(6L, 6L)))
        };

        for (Test tt : tests) {
            Storage s = new Storage.MemoryStorage(ents);
            try {
                List<Entry> entries = s.entries(tt.lo, tt.hi, tt.maxsize);
                assertEquals(entries.size(), tt.wentries.size());
                for (int i = 0; i < entries.size(); i++) {
                    assertEquals(entries.get(i).getTerm(), tt.wentries.get(i).getTerm());
                    assertEquals(entries.get(i).getIndex(), tt.wentries.get(i).getIndex());
                }
            } catch (Errors.RaftException e) {
                if (e != tt.werr) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageLastIndex() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        Storage.MemoryStorage s = new Storage.MemoryStorage(ents);

        long last = s.lastIndex();
        assertEquals(5L, last);
        s.append(Arrays.asList(newEntry(5L, 6L)));
        last = s.lastIndex();
        assertEquals(6L, last);
    }

    @Test
    public void testStorageFirstIndex() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
        long first = s.firstIndex();
        assertEquals(4L, first);
        s.compact(4);
        first = s.firstIndex();
        assertEquals(5L, first);
    }

    @Test
    public void testStorageCompact() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        class Test {
            public Test(long i, Exception werr, long windex, long wterm, int wlen) {
                this.i = i;
                this.werr = werr;
                this.wterm = wterm;
                this.windex = windex;
                this.wlen = wlen;
            }

            public long i;
            public Exception werr;
            public long windex;
            public long wterm;
            public int wlen;
        }
        Test[] tests = new Test[] {
                new Test(2, ERR_COMPACTED, 3, 3, 3),
                new Test(3, ERR_COMPACTED, 3, 3, 3),
            new Test(4, null, 4, 4, 2),
            new Test(5, null, 5, 5, 1)
        };
        for (Test tt : tests) {
            Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
            try {
                s.compact(tt.i);
                assertEquals(tt.windex, s.ents.get(0).getIndex());
                assertEquals(tt.wterm, s.ents.get(0).getTerm());
                assertEquals(tt.wlen, s.ents.size());
            } catch (RuntimeException e) {
                if (e != tt.werr) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageCreateSnapshot() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        ConfState cs = new ConfState();
        cs.setNodes(Arrays.asList(1L, 2L, 3L));
        byte[] data = "data".getBytes();
        class Test {
            public Test(long i, Exception werr, Snapshot wsnap) {
                this.i = i;
                this.werr = werr;
                this.wsnap = wsnap;
            }

            public long i;
            public Exception werr;
            public Snapshot wsnap;
        }
        Snapshot s1 = new Snapshot();
        SnapshotMetadata m1 = new SnapshotMetadata();
        m1.setConfState(cs);
        m1.setIndex(4L);
        m1.setTerm(4L);
        s1.setData(data);
        s1.setMetadata(m1);

        Snapshot s2 = new Snapshot();
        SnapshotMetadata m2 = new SnapshotMetadata();
        m2.setConfState(cs);
        m2.setIndex(5L);
        m2.setTerm(5L);
        s2.setData(data);
        s2.setMetadata(m2);
        Test[] tests = new Test[] {
                new Test(4, null, s1),
                new Test(5, null, s2)
        };
        for (Test tt : tests) {
            Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
            try {
                Snapshot snap = s.createSnapshot(tt.i, cs, data);
                assertArrayEquals(snap.getData(), tt.wsnap.getData());
                assertEquals(snap.getMetadata().getIndex(), tt.wsnap.getMetadata().getIndex());
                assertEquals(snap.getMetadata().getTerm(), tt.wsnap.getMetadata().getTerm());
                assertEquals(snap.getMetadata().getConfState().getNodes().size(), tt.wsnap.getMetadata().getConfState().getNodes().size());
            } catch (Errors.RaftException e) {
                if (e != tt.werr) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageAppend() {
        List<Entry> ents = new ArrayList<>();
        ents.add(newEntry(3L, 3L));
        ents.add(newEntry(4L, 4L));
        ents.add(newEntry(5L, 5L));
        class Test {
            public Test(List<Entry> entries, Exception werr, List<Entry> wentries) {
                this.entries = entries;
                this.werr = werr;
                this.wentries = wentries;
            }

            public List<Entry> entries;
            public Exception werr;
            public List<Entry> wentries;
        }
        Test[] tests = new Test[] {
            new Test(Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(4L, 4L),
                    newEntry(5L, 5L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(4L, 4L),
                    newEntry(5L, 5L)
            )),
            new Test(Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(6L, 4L),
                    newEntry(6L, 5L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(6L, 4L),
                    newEntry(6L, 5L)
            )),
            new Test(Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(4L, 4L),
                    newEntry(5L, 5L),
                    newEntry(6L, 6L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(4L, 4L),
                    newEntry(5L, 5L),
                    newEntry(6L, 6L)
            )),
            new Test(Arrays.asList(
                    newEntry(3L, 2L),
                    newEntry(3L, 3L),
                    newEntry(5L, 4L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(5L, 4L)
            )),
            new Test(Arrays.asList(
                    newEntry(5L, 4L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(5L, 4L)
            )),
            new Test(Arrays.asList(
                    newEntry(5L, 6L)
            ), null, Arrays.asList(
                    newEntry(3L, 3L),
                    newEntry(4L, 4L),
                    newEntry(5L, 5L),
                    newEntry(5L, 6L)
            ))
        };

        for (Test tt : tests) {
            Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
            try {
                s.append(tt.entries);
                assertEquals(s.ents.size(), tt.wentries.size());
                for (int i = 0; i < s.ents.size(); i++) {
                    assertEquals(s.ents.get(i).getTerm(), tt.wentries.get(i).getTerm());
                    assertEquals(s.ents.get(i).getIndex(), tt.wentries.get(i).getIndex());
                }
            } catch (Errors.RaftException e) {
                if (e != tt.werr) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageApplySnapshot() {
        ConfState cs = new ConfState();
        cs.setNodes(Arrays.asList(1L, 2L, 3L));
        byte[] data = "data".getBytes();
        Snapshot s1 = new Snapshot();
        SnapshotMetadata m1 = new SnapshotMetadata();
        m1.setConfState(cs);
        m1.setIndex(4L);
        m1.setTerm(4L);
        s1.setData(data);
        s1.setMetadata(m1);

        Snapshot s2 = new Snapshot();
        SnapshotMetadata m2 = new SnapshotMetadata();
        m2.setConfState(cs);
        m2.setIndex(3L);
        m2.setTerm(3L);
        s2.setData(data);
        s2.setMetadata(m2);

        Snapshot[] tests = new Snapshot[] {
                s1, s2
        };
        Storage.MemoryStorage s = new Storage.MemoryStorage();
        int i = 0;
        Snapshot tt = tests[i];
        try {
            s.applySnapshot(tt);
        } catch (Errors.RaftException e) {
            fail();
        }

        //Apply Snapshot fails due to ErrSnapOutOfDate
        i = 1;
        tt = tests[i];
        try {
            s.applySnapshot(tt);
            fail();
        } catch (Errors.RaftException e) {
            if (e != ERR_SNAP_OUT_OF_DATE) {
                fail();
            }
        }
    }

}