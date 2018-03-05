package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.ConfState;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.Snapshot;
import com.moilioncircle.raft.entity.SnapshotMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static com.moilioncircle.raft.Storage.ErrCompacted;
import static com.moilioncircle.raft.Storage.ErrSnapOutOfDate;
import static com.moilioncircle.raft.Storage.ErrUnavailable;
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
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));

        class Test {
            public Test(long i, String werr, long wterm, boolean wpanic) {
                this.i = i;
                this.werr = werr;
                this.wterm = wterm;
                this.wpanic = wpanic;
            }

            public long i;
            public String werr;
            public long wterm;
            public boolean wpanic;
        }
        Test[] tests = new Test[] {
            new Test(2, ErrCompacted, 0, false),
            new Test(3, null, 3, false),
            new Test(4, null, 4, false),
            new Test(5, null, 5, false),
            new Test(6, ErrUnavailable, 0, false)
        };
        for (Test tt : tests) {
            try {
                Storage s = new Storage.MemoryStorage(ents);
                long term = s.term(tt.i);
                if (term != tt.wterm) {
                    fail();
                }
            } catch (RuntimeException e) {
                if (!e.getMessage().equals(tt.werr)) {
                    fail();
                }
            }
        }

    }

    @Test
    public void testStorageEntries() {
        List<Entry> ents = new ArrayList<>();
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
        ents.add(new Entry(6L, 6L, null, null));
        class Test {
            public Test(long lo, long hi, long maxsize, String werr, List<Entry> wentries) {
                this.lo = lo;
                this.hi = hi;
                this.maxsize = maxsize;
                this.werr = werr;
                this.wentries = wentries;
            }

            public long lo, hi, maxsize;
            public String werr;
            public List<Entry> wentries;
        }
        Test[] tests = new Test[] {
            new Test(2, 6, Long.MAX_VALUE, ErrCompacted, null),
            new Test(3, 4, Long.MAX_VALUE, ErrCompacted, null),
            new Test(4, 5, Long.MAX_VALUE, ErrCompacted, Arrays.asList(new Entry(4L, 4L, null, null))),
            new Test(4, 6, Long.MAX_VALUE, ErrCompacted, Arrays.asList(
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null))),
            new Test(4, 7, Long.MAX_VALUE, ErrCompacted, Arrays.asList(
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null),
                new Entry(6L, 6L, null, null))),
            new Test(4, 7, 0, ErrCompacted, Arrays.asList(
                new Entry(4L, 4L, null, null))),
            new Test(4, 7, 2, ErrCompacted, Arrays.asList(
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null))),
            new Test(4, 7, 3, ErrCompacted, Arrays.asList(
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null),
                new Entry(6L, 6L, null, null)))
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
            } catch (RuntimeException e) {
                if (!e.getMessage().equals(tt.werr)) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageLastIndex() {
        List<Entry> ents = new ArrayList<>();
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
        Storage.MemoryStorage s = new Storage.MemoryStorage(ents);

        long last = s.lastIndex();
        assertEquals(5L, last);
        s.append(Arrays.asList(new Entry(5L, 6L, null, null)));
        last = s.lastIndex();
        assertEquals(6L, last);
    }

    @Test
    public void testStorageFirstIndex() {
        List<Entry> ents = new ArrayList<>();
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
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
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
        class Test {
            public Test(long i, String werr, long windex, long wterm, int wlen) {
                this.i = i;
                this.werr = werr;
                this.wterm = wterm;
                this.windex = windex;
                this.wlen = wlen;
            }

            public long i;
            public String werr;
            public long windex;
            public long wterm;
            public int wlen;
        }
        Test[] tests = new Test[] {
            new Test(2, ErrCompacted, 3, 3, 3),
            new Test(3, ErrCompacted, 3, 3, 3),
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
                if (!e.getMessage().equals(tt.werr)) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageCreateSnapshot() {
        List<Entry> ents = new ArrayList<>();
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
        ConfState cs = new ConfState(Arrays.asList(1L, 2L, 3L), null);
        byte[] data = "data".getBytes();
        class Test {
            public Test(long i, String werr, Snapshot wsnap) {
                this.i = i;
                this.werr = werr;
                this.wsnap = wsnap;
            }

            public long i;
            public String werr;
            public Snapshot wsnap;
        }
        Test[] tests = new Test[] {
            new Test(4, null, new Snapshot(data, new SnapshotMetadata(cs, 4L, 4L))),
            new Test(5, null, new Snapshot(data, new SnapshotMetadata(cs, 5L, 5L)))
        };
        for (Test tt : tests) {
            Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
            try {
                Snapshot snap = s.createSnapshot(tt.i, cs, data);
                assertArrayEquals(snap.getData(), tt.wsnap.getData());
                assertEquals(snap.getMetadata().getIndex(), tt.wsnap.getMetadata().getIndex());
                assertEquals(snap.getMetadata().getTerm(), tt.wsnap.getMetadata().getTerm());
                assertEquals(snap.getMetadata().getConfState().getNodes().size(), tt.wsnap.getMetadata().getConfState().getNodes().size());
            } catch (RuntimeException e) {
                e.printStackTrace();
                if (!e.getMessage().equals(tt.werr)) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageAppend() {
        List<Entry> ents = new ArrayList<>();
        ents.add(new Entry(3L, 3L, null, null));
        ents.add(new Entry(4L, 4L, null, null));
        ents.add(new Entry(5L, 5L, null, null));
        class Test {
            public Test(List<Entry> entries, String werr, List<Entry> wentries) {
                this.entries = entries;
                this.werr = werr;
                this.wentries = wentries;
            }

            public List<Entry> entries;
            public String werr;
            public List<Entry> wentries;
        }
        Test[] tests = new Test[] {
            new Test(Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null)
            )),
            new Test(Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(6L, 4L, null, null),
                new Entry(6L, 5L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(6L, 4L, null, null),
                new Entry(6L, 5L, null, null)
            )),
            new Test(Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null),
                new Entry(6L, 6L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null),
                new Entry(6L, 6L, null, null)
            )),
            new Test(Arrays.asList(
                new Entry(3L, 2L, null, null),
                new Entry(3L, 3L, null, null),
                new Entry(5L, 4L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(5L, 4L, null, null)
            )),
            new Test(Arrays.asList(
                new Entry(5L, 4L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(5L, 4L, null, null)
            )),
            new Test(Arrays.asList(
                new Entry(5L, 6L, null, null)
            ), null, Arrays.asList(
                new Entry(3L, 3L, null, null),
                new Entry(4L, 4L, null, null),
                new Entry(5L, 5L, null, null),
                new Entry(5L, 6L, null, null)
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
            } catch (RuntimeException e) {
                e.printStackTrace();
                if (!e.getMessage().equals(tt.werr)) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testStorageApplySnapshot() {
        ConfState cs = new ConfState(Arrays.asList(1L, 2L, 3L), null);
        byte[] data = "data".getBytes();
        Snapshot[] tests = new Snapshot[] {
            new Snapshot(data, new SnapshotMetadata(cs, 4L, 4L)),
            new Snapshot(data, new SnapshotMetadata(cs, 3L, 3L)),
        };
        Storage.MemoryStorage s = new Storage.MemoryStorage();
        int i = 0;
        Snapshot tt = tests[i];
        try {
            s.applySnapshot(tt);
        } catch (RuntimeException e) {
            fail();
        }

        //Apply Snapshot fails due to ErrSnapOutOfDate
        i = 1;
        tt = tests[i];
        try {
            s.applySnapshot(tt);
            fail();
        } catch (RuntimeException e) {
            if (!e.getMessage().equals(ErrSnapOutOfDate)) {
                fail();
            }
        }
    }

}