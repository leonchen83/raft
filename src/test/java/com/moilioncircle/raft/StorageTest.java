package com.moilioncircle.raft;

import com.google.protobuf.ByteString;
import com.moilioncircle.raft.entity.Entry;
import com.moilioncircle.raft.entity.proto.RaftProto;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        class Test {
            public Test(long i, String werr, long wterm, boolean wpanic) {
                this.i = i; this.werr = werr; this.wterm = wterm; this.wpanic = wpanic;
            }

            public long i;
            public String werr;
            public long wterm;
            public boolean wpanic;
        }
        Test[] tests = new Test[]{
                new Test(2, ErrCompacted, 0, false),
                new Test(3, null, 3, false),
                new Test(4, null, 4, false),
                new Test(5, null, 5, false),
                new Test(6, ErrUnavailable, 0, false)
        };
        for (Test tt : tests) {
            try {
                Storage s = new Storage.MemoryStorage(Entry.valueOf(ents));
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
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(6L).setTerm(6L).build());
        class Test {
            public Test(long lo, long hi, long maxsize, String werr, List<RaftProto.Entry> wentries) {
                this.lo = lo; this.hi = hi; this.maxsize = maxsize; this.werr = werr; this.wentries = wentries;
            }

            public long lo, hi, maxsize;
            public String werr;
            public List<RaftProto.Entry> wentries;
        }
        Test[] tests = new Test[]{
                new Test(2, 6, Long.MAX_VALUE, ErrCompacted, null),
                new Test(3, 4, Long.MAX_VALUE, ErrCompacted, null),
                new Test(4, 5, Long.MAX_VALUE, ErrCompacted, Arrays.asList(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build())),
                new Test(4, 6, Long.MAX_VALUE, ErrCompacted, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build())),
                new Test(4, 7, Long.MAX_VALUE, ErrCompacted, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build(),
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(6L).build())),
                new Test(4, 7, 0, ErrCompacted, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build())),
                new Test(4, 7, 2, ErrCompacted, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build())),
                new Test(4, 7, 3, ErrCompacted, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build(),
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(6L).build())),
        };

        for (Test tt : tests) {
            Storage s = new Storage.MemoryStorage(ents);
            try {
                List<RaftProto.Entry> entries = s.entries(tt.lo, tt.hi, tt.maxsize);
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
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        Storage.MemoryStorage s = new Storage.MemoryStorage(ents);

        long last = s.lastIndex();
        assertEquals(5L, last);
        s.append(Arrays.asList(RaftProto.Entry.newBuilder().setIndex(6L).setTerm(5L).build()));
        last = s.lastIndex();
        assertEquals(6L, last);
    }

    @Test
    public void testStorageFirstIndex() {
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
        long first = s.firstIndex();
        assertEquals(4L, first);
        s.compact(4);
        first = s.firstIndex();
        assertEquals(5L, first);
    }

    @Test
    public void testStorageCompact() {
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        class Test {
            public Test(long i, String werr, long windex, long wterm, int wlen) {
                this.i = i; this.werr = werr; this.wterm = wterm; this.windex = windex; this.wlen = wlen;
            }

            public long i;
            public String werr;
            public long windex;
            public long wterm;
            public int wlen;
        }
        Test[] tests = new Test[]{
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
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        RaftProto.ConfState cs = RaftProto.ConfState.newBuilder().addNodes(1).addNodes(2).addNodes(3).build();
        byte[] data = "data".getBytes();
        class Test {
            public Test(long i, String werr, RaftProto.Snapshot wsnap) {
                this.i = i; this.werr = werr; this.wsnap = wsnap;
            }

            public long i;
            public String werr;
            public RaftProto.Snapshot wsnap;
        }
        Test[] tests = new Test[]{
                new Test(4, null, RaftProto.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProto.SnapshotMetadata.newBuilder().setIndex(4L).setTerm(4L).setConfState(cs).build()).build()),
                new Test(5, null, RaftProto.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProto.SnapshotMetadata.newBuilder().setIndex(5L).setTerm(5L).setConfState(cs).build()).build()),

        };
        for (Test tt : tests) {
            Storage.MemoryStorage s = new Storage.MemoryStorage(ents);
            try {
                RaftProto.Snapshot snap = s.createSnapshot(tt.i, cs, data);
                assertArrayEquals(snap.getData().toByteArray(), tt.wsnap.getData().toByteArray());
                assertEquals(snap.getMetadata().getIndex(), tt.wsnap.getMetadata().getIndex());
                assertEquals(snap.getMetadata().getTerm(), tt.wsnap.getMetadata().getTerm());
                assertEquals(snap.getMetadata().getConfState().getNodesCount(), tt.wsnap.getMetadata().getConfState().getNodesCount());
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
        List<RaftProto.Entry> ents = new ArrayList<>();
        ents.add(RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build());
        ents.add(RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build());
        class Test {
            public Test(List<RaftProto.Entry> entries, String werr, List<RaftProto.Entry> wentries) {
                this.entries = entries; this.werr = werr; this.wentries = wentries;
            }

            public List<RaftProto.Entry> entries;
            public String werr;
            public List<RaftProto.Entry> wentries;
        }
        Test[] tests = new Test[]{
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build()
                )),
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(6L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(6L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(6L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(6L).build()
                )),
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build(),
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(5L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build(),
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(5L).build()
                )),
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(2L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(5L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(5L).build()
                )),
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(5L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(5L).build()
                )),
                new Test(Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(5L).build()
                ), null, Arrays.asList(
                        RaftProto.Entry.newBuilder().setIndex(3L).setTerm(3L).build(),
                        RaftProto.Entry.newBuilder().setIndex(4L).setTerm(4L).build(),
                        RaftProto.Entry.newBuilder().setIndex(5L).setTerm(5L).build(),
                        RaftProto.Entry.newBuilder().setIndex(6L).setTerm(5L).build()
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
        RaftProto.ConfState cs = RaftProto.ConfState.newBuilder().addNodes(1).addNodes(2).addNodes(3).build();
        byte[] data = "data".getBytes();
        RaftProto.Snapshot[] tests = new RaftProto.Snapshot[]{
                RaftProto.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProto.SnapshotMetadata.newBuilder().setIndex(4L).setTerm(4L).setConfState(cs).build()).build(),
                RaftProto.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProto.SnapshotMetadata.newBuilder().setIndex(3L).setTerm(3L).setConfState(cs).build()).build()
        };
        Storage.MemoryStorage s = new Storage.MemoryStorage();
        int i = 0;
        RaftProto.Snapshot tt = tests[i];
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