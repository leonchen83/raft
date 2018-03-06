package com.moilioncircle.raft;

import com.moilioncircle.raft.Raft.SoftState;
import com.moilioncircle.raft.entity.HardState;

import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.raft.Raft.StateRole.Leader;

public class Status {

    public long id;

    public long applied;

    public HardState hardState;

    public SoftState softState;

    public long leadTransferee;

    public Map<Long, Progress> progress;

    /**
     * getStatus gets a copy of the current raft status.
     */
    public static Status getStatus(Raft r) {
        Status s = new Status();
        s.id = r.id;
        s.leadTransferee = r.leadTransferee;
        s.hardState = r.hardState();
        s.softState = r.softState();
        s.applied = r.raftLog.applied;

        if (s.softState.raftState == Leader) {
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
