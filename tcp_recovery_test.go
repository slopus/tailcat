// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package tailcat

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// newRecoveryTestStack is a gVisor stack set up the way tailcat's netstack
// sets up TCP, with room to queue a full congestion window.
func newRecoveryTestStack(t *testing.T, addr tcpip.Address) (*stack.Stack, *channel.Endpoint) {
	t.Helper()
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	cubic := tcpip.CongestionControlOption("cubic") // as tailscale.com/wgengine/netstack
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &cubic); err != nil {
		t.Fatalf("cubic: %v", err)
	}
	ep := channel.New(4096, 1280, "")
	if err := s.CreateNIC(testNICID, ep); err != nil {
		t.Fatalf("CreateNIC: %v", err)
	}
	if err := s.AddProtocolAddress(testNICID, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: addr.WithPrefix(),
	}, stack.AddressProperties{}); err != nil {
		t.Fatalf("AddProtocolAddress: %v", err)
	}
	s.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: testNICID}})
	t.Cleanup(func() {
		ep.Close()
		s.Close()
		s.Wait()
	})
	return s, ep
}

// delayTestPackets forwards packets from src to dst after d, dropping those
// for which drop returns true.
func delayTestPackets(ctx context.Context, src, dst *channel.Endpoint, d time.Duration, drop func() bool) {
	type delayed struct {
		at  time.Time
		pkt *stack.PacketBuffer
		pn  tcpip.NetworkProtocolNumber
	}
	q := make(chan delayed, 1<<16)
	go func() {
		for p := range q {
			time.Sleep(time.Until(p.at))
			dst.InjectInbound(p.pn, p.pkt)
			p.pkt.DecRef()
		}
	}()
	defer close(q)
	for {
		pkt := src.ReadContext(ctx)
		if pkt == nil {
			return
		}
		if !drop() {
			in := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: pkt.ToBuffer()})
			q <- delayed{time.Now().Add(d), in, pkt.NetworkProtocolNumber}
		}
		pkt.DecRef()
	}
}

// TestSACKRecoversFromBlackout checks that a download whose sender times out
// while the receiver offers SACK, the way a phone that briefly loses its path
// does, finishes in a few seconds. Upstream gVisor then repaired one or two
// segments per doubled timeout, and this download took 80 s or more.
func TestSACKRecoversFromBlackout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	clientAddr := tcpip.AddrFrom4([4]byte{10, 0, 0, 1})
	serverAddr := tcpip.AddrFrom4([4]byte{10, 0, 0, 2})
	clientStack, clientEP := newRecoveryTestStack(t, clientAddr)
	serverStack, serverEP := newRecoveryTestStack(t, serverAddr)
	var blackout atomic.Bool
	go delayTestPackets(ctx, clientEP, serverEP, 35*time.Millisecond, func() bool { return false })
	go delayTestPackets(ctx, serverEP, clientEP, 35*time.Millisecond, blackout.Load)

	ln, err := gonet.ListenTCP(serverStack, tcpip.FullAddress{NIC: testNICID, Addr: serverAddr, Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	const size = 2_000_000
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		c.Write(make([]byte, size))
		c.Close()
	}()

	c, err := gonet.DialTCP(clientStack, tcpip.FullAddress{NIC: testNICID, Addr: serverAddr, Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	start := time.Now()
	c.SetReadDeadline(start.Add(30 * time.Second))
	buf := make([]byte, 64<<10)
	got := 0
	for {
		n, err := c.Read(buf)
		if got < size/4 && got+n >= size/4 {
			// Long enough for the sender's retransmission timer to fire.
			blackout.Store(true)
			time.AfterFunc(300*time.Millisecond, func() { blackout.Store(false) })
		}
		got += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("after %v and %d of %d bytes: %v", time.Since(start).Round(time.Millisecond), got, size, err)
		}
	}
	if got != size {
		t.Fatalf("got %d bytes; want %d", got, size)
	}
	t.Logf("%d bytes in %v", got, time.Since(start).Round(time.Millisecond))
}
