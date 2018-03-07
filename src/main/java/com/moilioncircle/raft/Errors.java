/*
 * Copyright 2016-2017 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.raft;

public class Errors {

    /**
     * ErrProposalDropped is returned when the proposal is ignored by some cases,
     * so that the proposer can be notified and fail fast.
     */
    private static final String ErrProposalDropped = "raft proposal dropped";

    /**
     * ErrCompacted is returned by Storage.entries/compact when a requested
     * index is unavailable because it predates the last snapshot.
     */
    private static final String ErrCompacted = "requested index is unavailable due to compaction";

    /**
     * ErrSnapOutOfDate is returned by Storage.createSnapshot when a requested
     * index is older than the existing snapshot.
     */
    private static final String ErrSnapOutOfDate = "requested index is older than the existing snapshot";

    /**
     * ErrUnavailable is returned by Storage interface when the requested log entries
     * are unavailable.
     */
    private static final String ErrUnavailable = "requested entry at index is unavailable";

    /**
     * ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
     * snapshot is temporarily unavailable.
     */
    private static final String ErrSnapshotTemporarilyUnavailable = "snapshot is temporarily unavailable";

    /**
     * ErrStepLocalMsg is returned when try to step a local raft message
     */
    private static final String ErrStepLocalMsg = "raft: cannot step raft local message";

    /**
     * ErrStepPeerNotFound is returned when try to step a response message
     * but there is no peer found in raft.prs for that node.
     */
    private static final String ErrStepPeerNotFound = "raft: cannot step as peer not found";

    public static final RaftException ERR_PROPOSAL_DROPPED = new RaftException(ErrProposalDropped);

    public static final RaftException ERR_COMPACTED = new RaftException(ErrCompacted);

    public static final RaftException ERR_SNAP_OUT_OF_DATE = new RaftException(ErrSnapOutOfDate);

    public static final RaftException ERR_UNAVAILABLE = new RaftException(ErrUnavailable);

    public static final RaftException ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE = new RaftException(ErrSnapshotTemporarilyUnavailable);

    public static final RaftException ERR_STEP_LOCAL_MSG = new RaftException(ErrStepLocalMsg);

    public static final RaftException ERR_STEP_PEER_NOT_FOUND = new RaftException(ErrStepPeerNotFound);

    public static class RaftException extends RuntimeException {

        public RaftException() {
            super();
        }

        public RaftException(String message) {
            super(message);
        }

        public RaftException(String message, Throwable cause) {
            super(message, cause);
        }

        public RaftException(Throwable cause) {
            super(cause);
        }

        protected RaftException(String message, Throwable cause, boolean s, boolean w) {
            super(message, cause, s, w);
        }
    }

    public static class RaftConfigException extends RaftException {

        public RaftConfigException() {
            super();
        }

        public RaftConfigException(String message) {
            super(message);
        }

        public RaftConfigException(String message, Throwable cause) {
            super(message, cause);
        }

        public RaftConfigException(Throwable cause) {
            super(cause);
        }

        protected RaftConfigException(String message, Throwable cause, boolean s, boolean w) {
            super(message, cause, s, w);
        }
    }
}
