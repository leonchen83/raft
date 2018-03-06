package com.moilioncircle.raft;

public class Node {

    public static class SoftState {

        /**
         * must use atomic operations to access; keep 64-bit aligned.
         */
        private long lead;

        private long raftState;

        public long getLead() {
            return lead;
        }

        public long getRaftState() {
            return raftState;
        }

        public void setLead(long lead) {
            this.lead = lead;
        }

        public void setRaftState(long raftState) {
            this.raftState = raftState;
        }

        @Override
        public String toString() {
            return "SoftState{" +
                    "lead=" + lead +
                    ", raftState=" + raftState +
                    '}';
        }
    }
}
