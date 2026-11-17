package nsqd

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqlookupd"
)

type clusterLookupResponse struct {
	Producers []struct {
		BroadcastAddress string `json:"broadcast_address"`
		TCPPort          int    `json:"tcp_port"`
	} `json:"producers"`
	Channels []string `json:"channels"`
}

func waitForLookupProducers(t *testing.T, httpAddr net.Addr, topicName string, num int) {
	t.Helper()
	var lr clusterLookupResponse
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	for i := 0; i < 200; i++ {
		lr.Producers = nil
		lr.Channels = nil
		err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &lr)
		if err == nil && len(lr.Producers) == num {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d producers of topic %s on nsqlookupd %s (got %d)",
		num, topicName, httpAddr, len(lr.Producers))
}

func waitForDepth(t *testing.T, ch *Channel, depth int64) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ch.Depth() == depth {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for channel(%s) depth %d (got %d)", ch.name, depth, ch.Depth())
}

func pubMessages(t *testing.T, conn net.Conn, topicName string, bodies []string) {
	t.Helper()
	for _, body := range bodies {
		_, err := nsq.Publish(topicName, []byte(body)).WriteTo(conn)
		test.Nil(t, err)
		readValidate(t, conn, frameTypeResponse, "OK")
	}
}

func recvMessages(t *testing.T, conn net.Conn, num int) []*Message {
	t.Helper()
	var msgs []*Message
	for i := 0; i < num; i++ {
		resp, err := nsq.ReadResponse(conn)
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.Equal(t, frameTypeMessage, frameType)
		msg, err := decodeMessage(data)
		test.Nil(t, err)
		msgs = append(msgs, msg)
	}
	return msgs
}

// multi-node integration test: two nsqd nodes registered with one nsqlookupd,
// a consumer disconnects mid-stream and a reconnecting consumer (located via
// nsqlookupd) continues to drain the channel backlog without message loss
// and with bounded duplicates
func TestClusterConsumerReconnect(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	lookupdTCPAddr, lookupdHTTPAddr, lookupd := mustStartNSQLookupd(lopts)
	defer lookupd.Exit()

	opts1 := NewOptions()
	opts1.Logger = test.NewTestLogger(t)
	opts1.BroadcastAddress = "127.0.0.1"
	opts1.NSQLookupdTCPAddresses = []string{lookupdTCPAddr.String()}
	tcpAddr1, httpAddr1, nsqd1 := mustStartNSQD(opts1)
	defer func() { _ = os.RemoveAll(opts1.DataPath) }()
	defer nsqd1.Exit()

	opts2 := NewOptions()
	opts2.Logger = test.NewTestLogger(t)
	opts2.BroadcastAddress = "127.0.0.1"
	opts2.NSQLookupdTCPAddresses = []string{lookupdTCPAddr.String()}
	_, httpAddr2, nsqd2 := mustStartNSQD(opts2)
	defer func() { _ = os.RemoveAll(opts2.DataPath) }()
	defer nsqd2.Exit()

	topicName := "cluster_reconnect" + strconv.Itoa(int(time.Now().Unix()))

	// create the topic/channel on both nodes so both register with nsqlookupd
	for _, httpAddr := range []net.Addr{httpAddr1, httpAddr2} {
		url := fmt.Sprintf("http://%s/topic/create?topic=%s", httpAddr, topicName)
		err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url, nil, nil)
		test.Nil(t, err)
		url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", httpAddr, topicName)
		err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url, nil, nil)
		test.Nil(t, err)
	}

	// both nodes should be discoverable
	waitForLookupProducers(t, lookupdHTTPAddr, topicName, 2)

	topic1 := nsqd1.GetTopic(topicName)
	ch1 := topic1.GetChannel("ch")
	ch2 := nsqd2.GetTopic(topicName).GetChannel("ch")

	// publish 100 messages to node 1
	const numMessages = 100
	bodies := make([]string, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		bodies = append(bodies, fmt.Sprintf("msg-%d", i))
	}
	pconn, err := mustConnectNSQD(tcpAddr1)
	test.Nil(t, err)
	defer func() { _ = pconn.Close() }()
	pubMessages(t, pconn, topicName, bodies)
	waitForDepth(t, ch1, numMessages)

	// consumer 1 subscribes, receives 10 messages, then disconnects
	// without FINishing them
	conn1, err := mustConnectNSQD(tcpAddr1)
	test.Nil(t, err)
	identify(t, conn1, map[string]interface{}{
		"msg_timeout": 60000,
	}, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(10).WriteTo(conn1)
	test.Nil(t, err)
	firstBatch := recvMessages(t, conn1, 10)
	_ = conn1.Close()

	// the 10 in-flight messages should be requeued promptly
	waitForDepth(t, ch1, numMessages)
	test.Equal(t, 10, int(atomic.LoadUint64(&ch1.requeueCount)))

	// consumer 2 reconnects (in practice located via nsqlookupd) and
	// drains the remaining channel backlog
	conn2, err := mustConnectNSQD(tcpAddr1)
	test.Nil(t, err)
	defer func() { _ = conn2.Close() }()
	identify(t, conn2, map[string]interface{}{
		"msg_timeout": 60000,
	}, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(numMessages).WriteTo(conn2)
	test.Nil(t, err)
	secondBatch := recvMessages(t, conn2, numMessages)
	for _, msg := range secondBatch {
		_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn2)
		test.Nil(t, err)
	}

	// no message loss: exactly numMessages unique messages were delivered
	// across both consumers, and duplicates are bounded to the 10 messages
	// that were in-flight when consumer 1 disconnected
	seen := make(map[MessageID]int)
	for _, msg := range firstBatch {
		seen[msg.ID]++
	}
	for _, msg := range secondBatch {
		seen[msg.ID]++
	}
	test.Equal(t, numMessages, len(seen))
	duplicates := 0
	for _, n := range seen {
		if n > 1 {
			duplicates++
		}
	}
	test.Equal(t, 10, duplicates)

	waitForDepth(t, ch1, 0)

	// node 2 was never published to and must remain unaffected
	test.Equal(t, int64(0), ch2.Depth())
}

// multi-node integration test: changing the set of nsqlookupd nodes (node
// discovery changes) must not affect already established consumer
// connections, and the nsqd node must re-register with the new nsqlookupd
func TestClusterLookupdChangeStableConnection(t *testing.T) {
	lopts1 := nsqlookupd.NewOptions()
	lopts1.Logger = test.NewTestLogger(t)
	lopts1.BroadcastAddress = "127.0.0.1"
	lookupd1TCPAddr, _, lookupd1 := mustStartNSQLookupd(lopts1)
	defer lookupd1.Exit()

	lopts2 := nsqlookupd.NewOptions()
	lopts2.Logger = test.NewTestLogger(t)
	lopts2.BroadcastAddress = "127.0.0.1"
	lookupd2TCPAddr, lookupd2HTTPAddr, lookupd2 := mustStartNSQLookupd(lopts2)
	defer lookupd2.Exit()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.BroadcastAddress = "127.0.0.1"
	opts.NSQLookupdTCPAddresses = []string{lookupd1TCPAddr.String()}
	tcpAddr, httpAddr, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "cluster_lookupd_change" + strconv.Itoa(int(time.Now().Unix()))

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", httpAddr, topicName)
	err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url, nil, nil)
	test.Nil(t, err)
	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url, nil, nil)
	test.Nil(t, err)

	ch := nsqd.GetTopic(topicName).GetChannel("ch")

	// establish a consumer connection before the discovery change
	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = conn.Close() }()
	identify(t, conn, map[string]interface{}{
		"msg_timeout": 60000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(10).WriteTo(conn)
	test.Nil(t, err)

	pconn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = pconn.Close() }()

	pubMessages(t, pconn, topicName, []string{"before-1", "before-2"})
	for _, msg := range recvMessages(t, conn, 2) {
		_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
		test.Nil(t, err)
	}

	// swap the discovery topology: nsqd moves from lookupd1 to lookupd2
	newOpts := NewOptions()
	newOpts.Logger = opts.Logger
	newOpts.BroadcastAddress = "127.0.0.1"
	newOpts.BroadcastTCPPort = nsqd.getOpts().BroadcastTCPPort
	newOpts.BroadcastHTTPPort = nsqd.getOpts().BroadcastHTTPPort
	newOpts.NSQLookupdTCPAddresses = []string{lookupd2TCPAddr.String()}
	nsqd.swapOpts(newOpts)
	nsqd.triggerOptsNotification()

	// the node should re-register itself (and its topics/channels) with
	// the new nsqlookupd
	waitForLookupProducers(t, lookupd2HTTPAddr, topicName, 1)

	// the already established consumer connection must keep receiving
	// messages after the discovery change
	pubMessages(t, pconn, topicName, []string{"after-1", "after-2", "after-3"})
	msgs := recvMessages(t, conn, 3)
	for i, msg := range msgs {
		test.Equal(t, fmt.Sprintf("after-%d", i+1), string(msg.Body))
		_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
		test.Nil(t, err)
	}

	waitForDepth(t, ch, 0)
}
