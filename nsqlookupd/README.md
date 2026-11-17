## nsqlookupd

`nsqlookupd` is the daemon that manages topology metadata and serves client requests to
discover the location of topics at runtime.

Read the [docs](https://nsq.io/components/nsqlookupd.html)

## Discovery Robustness

`nsqlookupd` only maintains topology metadata; it is not on the message path.
Established producer/consumer connections to an `nsqd` node are therefore
unaffected by `nsqlookupd` restarts or topology changes — they only limit
*new* consumers' ability to discover nodes until `nsqd` re-registers (which
happens automatically on reconnect or when its configured
`nsqlookupd_tcp_addresses` change).

Connection and error handling notes:

 * A clean TCP disconnect (EOF) from an `nsqd` node or client is logged at
   `INFO` level and is not treated as an error; other read failures are
   logged with the failing client and command context.
 * When a producer connection closes, all of its registrations are removed
   from the registration DB; when it reconnects and re-`IDENTIFY`s, `nsqd`
   re-`REGISTER`s all of its topics and channels.
 * A failure to resolve the local hostname while answering `IDENTIFY` is
   logged and returned as an empty `hostname` field rather than being fatal
   to the daemon.

### Tests

```
# nsqlookupd unit tests (registration DB, lookup protocol, HTTP API)
go test ./nsqlookupd/ -v

# multi-node integration tests live in the nsqd package
go test ./nsqd/ -run 'TestCluster' -v
```
