package com.moilioncircle.raft;

import com.moilioncircle.raft.entity.Message;
import com.moilioncircle.raft.util.Lists;
import com.moilioncircle.raft.util.Maps;
import com.moilioncircle.raft.util.Strings;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.moilioncircle.raft.util.Lists.slice;

public class ReadOnly {

    private static final Logger logger = LoggerFactory.getLogger(ReadOnly.class);

    public ReadOnlyOption option = ReadOnlyOption.Safe;
    public Map<String, ReadIndexStatus> pendingReadIndex = Maps.of();
    public List<String> readIndexQueue = Lists.of();

    public ReadOnly(ReadOnlyOption option) {
        this.option = option;
    }

    /**
     * addRequest adds a read only reuqest into readonly struct.
     * `index` is the commit index of the raft state machine when it received
     * the read only request.
     * `m` is the original read only request message from the local or remote node.
     */
    public void addRequest(long index, Message m) {
        String ctx = new String(m.getEntries().get(0).getData());
        if (!pendingReadIndex.containsKey(ctx)) return;
        ReadIndexStatus status = new ReadIndexStatus();
        status.req = m;
        status.index = index;
        pendingReadIndex.put(ctx, status);
        readIndexQueue.add(ctx);
    }

    /**
     * recvAck notifies the readonly struct that the raft state machine received
     * an acknowledgment of the heartbeat that attached with the read only request
     * context.
     */
    public int recvAck(Message m) {
        ReadIndexStatus rs = pendingReadIndex.get(new String(m.getContext()));
        if (rs == null) {
            return 0;
        }
        rs.acks.put(m.getFrom(), new Object());
        // add one to include an ack from local node
        return Maps.size(rs.acks) + 1;
    }

    /**
     * advance advances the read only request queue kept by the readonly struct.
     * It dequeues the requests until it finds the read only request that has
     * the same context as the given `m`.
     */
    public List<ReadIndexStatus> advance(Message m) {
        int i = 0;
        boolean found = false;
        String ctx = new String(m.getContext());
        List<ReadIndexStatus> rss = Lists.of();
        for (String okctx : readIndexQueue) {
            i++;
            ReadIndexStatus ok = pendingReadIndex.get(okctx);
            if (ok == null) {
                throw new Errors.RaftException("cannot find corresponding read state from pending map");
            }
            rss.add(ok);
            if (okctx.equals(ctx)) {
                found = true;
                break;
            }
        }

        if (found) {
            readIndexQueue = slice(readIndexQueue, i, Lists.size(readIndexQueue));
            for (ReadIndexStatus rs : rss) {
                pendingReadIndex.remove(new String(rs.req.getEntries().get(0).getData()));
            }
            return rss;
        }

        return null;
    }

    /**
     * lastPendingRequestCtx returns the context of the last pending read only
     * request in readonly struct.
     */
    public String lastPendingRequestCtx() {
        if (Lists.size(readIndexQueue) == 0)
            return "";
        return readIndexQueue.get(Lists.size(readIndexQueue) - 1);
    }

    @Override
    public String toString() {
        return Strings.buildEx(this);
    }

    /**
     * ReadState provides state for read only query.
     * It's caller's responsibility to call ReadIndex first before getting
     * this state from ready, it's also caller's duty to differentiate if this
     * state is what it requests through RequestCtx, eg. given a unique id as
     * RequestCtx
     */
    public static class ReadState {
        public long index;
        public byte[] requestCtx = new byte[0];

        @Override
        public String toString() {
            return Strings.buildEx(this);
        }
    }

    public static class ReadIndexStatus {
        public Message req = new Message();
        public long index;
        public Map<Long, Object> acks = Maps.of();

        @Override
        public String toString() {
            return Strings.buildEx(this);
        }
    }

    public enum ReadOnlyOption {
        Safe, LeaseBased
    }
}
