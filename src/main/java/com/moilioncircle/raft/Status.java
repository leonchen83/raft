package com.moilioncircle.raft;

import com.moilioncircle.raft.Raft.SoftState;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.util.Maps;
import com.moilioncircle.raft.util.Strings;
import java.util.Map;

import static com.moilioncircle.raft.Raft.StateRole.Leader;

public class Status {

    public long id;

    public long applied;

    public HardState hardState = new HardState();

    public SoftState softState = new SoftState();

    public long leadTransferee;

    public Map<Long, Progress> progress = Maps.of();

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
            s.progress = Maps.of();
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
        return Strings.buildEx(this);
    }
}
