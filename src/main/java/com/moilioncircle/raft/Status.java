package com.moilioncircle.raft;

import com.moilioncircle.json.parser.JSON;
import com.moilioncircle.json.parser.JSONObject;
import com.moilioncircle.raft.entity.HardState;
import com.moilioncircle.raft.Node.SoftState;
import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.raft.Raft.StateLeader;

public class Status {

    private long id;
    private HardState hardState;
    private SoftState softState;
    private long applied;
    private Map<Long, Progress> progress;
    private long leadTransferee;

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

    /**
     * marshalJSON translates the raft status into JSON.
     * TODO: try to simplify this by introducing ID type into raft
     */
    public String marshalJSON() {
        JSONObject object = new JSONObject();
        object.put("id", id);
        object.put("term", hardState.getTerm());
        object.put("vote", hardState.getVote());
        object.put("commit", hardState.getCommit());
        object.put("lead", softState.getLead());
        object.put("raftState", softState.getRaftState());
        object.put("applied", applied);
        if (progress.size() > 0) {
            for (Map.Entry<Long, Progress> entry : progress.entrySet()) {
                JSONObject progress = new JSONObject();
                progress.put("match", entry.getValue().getMatch());
                progress.put("next", entry.getValue().getNext());
                progress.put("state", entry.getValue().getState());
                object.put(String.valueOf(entry.getKey()), progress);
            }
        }
        object.put("leadtransferee", leadTransferee);
        return JSON.writeAsString(object);
    }

    @Override
    public String toString() {
        return marshalJSON();
    }
}
