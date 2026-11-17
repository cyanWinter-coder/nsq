## nsqd

`nsqd` is the daemon that receives, queues, and delivers messages to clients.

Read the [docs](https://nsq.io/components/nsqd.html)

## Channel Backlog and Consumer Reconnect

### Requeue on disconnect

When a consumer disconnects, any of its in-flight messages (delivered but not
yet `FIN`ished) are immediately requeued to the channel so that other — or
reconnecting — consumers receive them without waiting for the message timeout
(`--msg-timeout`, default `60s`) to expire. This bounds message redelivery
latency after a disconnect while preserving at-least-once semantics: a message
whose `FIN` was lost in the disconnect may be delivered twice, but it is never
dropped.

This behavior is controlled by:

 * `--requeue-on-disconnect` (default `true`, config file key
   `requeue_on_disconnect`) — set to `false` to restore the legacy behavior
   where in-flight messages of a disconnected consumer are only requeued when
   their message timeout expires.

Each immediately requeued message increments the channel's `requeue_count`
stat (the same stat used by explicit `REQ`), and message `Attempts` are
incremented on redelivery as usual. Channel backlog (memory + disk queue
depth) is unaffected by consumer churn: messages remain queued until a
consumer with sufficient `RDY` count drains them.

### Node discovery changes

`nsqlookupd` is only used for discovery and registration. Adding, removing, or
replacing `nsqlookupd` nodes (eg. via `SIGHUP` config reload of
`nsqlookupd_tcp_addresses`) does not affect already established producer or
consumer TCP connections; `nsqd` re-registers all of its topics and channels
with any newly added `nsqlookupd` node automatically.

### Tests

```
# unit tests: timeout, deferred requeue, disconnect requeue
go test ./nsqd/ -run 'TestInFlightWorker|TestDeferredRequeueWorker|TestRequeueOnDisconnect|TestDisconnectRequeue|TestClientMsgTimeout' -v

# multi-node integration tests (discovery changes, reconnect, backlog drain)
go test ./nsqd/ -run 'TestCluster' -v

# full suite
./test.sh
```

### Known limitations

 * Redelivery is at-least-once: duplicates are bounded to the messages that
   were in-flight when a consumer disconnected (or that timed out), they
   cannot be eliminated entirely.
 * Immediately requeued messages are appended to the channel backlog, so
   strict per-channel message ordering is not preserved across a disconnect.
 * When an `nsqd` node is removed from the discovery topology, its stale
   registrations remain visible in a previously used `nsqlookupd` until the
   node actually disconnects from that `nsqlookupd`.
