package com.moilioncircle.raft;

public class Node {

    public static class SoftState {

        /**
         * must use atomic operations to access; keep 64-bit aligned.
         */
        public long lead;

        public long raftState;

        @Override
        public String toString() {
            return "SoftState{" +
                    "lead=" + lead +
                    ", raftState=" + raftState +
                    '}';
        }
    }
}
