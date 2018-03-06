package com.moilioncircle.raft;

import com.moilioncircle.raft.Node.SoftState;
import com.moilioncircle.raft.entity.HardState;

import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.raft.Raft.StateLeader;

public class Status {

    private long id;
    private long applied;
    private HardState hardState;
    private SoftState softState;
    private long leadTransferee;
    private Map<Long, Progress> progress;

    public long getId() {
        return id;
    }

    public HardState getHardState() {
        return hardState;
    }

    public SoftState getSoftState() {
        return softState;
    }

    public long getApplied() {
        return applied;
    }

    public Map<Long, Progress> getProgress() {
        return progress;
    }

    public long getLeadTransferee() {
        return leadTransferee;
    }

    // getStatus gets a copy of the current raft status.
    public Status getStatus(Raft r) {
        Status s = new Status();
        s.id = r.id;
        s.leadTransferee = r.leadTransferee;
        s.hardState = r.hardState();
        s.softState = r.softState();
        s.applied = r.raftLog.getApplied();

        if (s.softState.getRaftState() == StateLeader) {
            s.progress = new HashMap<>();
            for (Map.Entry<Long, Progress> entry : r.prs.entrySet()) {
                s.progress.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Long, Progress> entry : r.learnerPrs.entrySet()) {
                s.progress.put(entry.getKey(), entry.getValue());
            }
        }
        return s;
    }

    @Override
    public String toString() {
        return "Status{" +
                "id=" + id +
                ", applied=" + applied +
                ", hardState=" + hardState +
                ", softState=" + softState +
                ", leadTransferee=" + leadTransferee +
                ", progress=" + progress +
                '}';
    }
}
