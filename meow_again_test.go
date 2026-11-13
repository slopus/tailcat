// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package tailcat

import (
	"context"
	"testing"
	"time"

	"tailscale.com/derp"
	"tailscale.com/derp/derphttp"
	"tailscale.com/net/netmon"
	"tailscale.com/tailcfg"
	"tailscale.com/tstest/integration"
	"tailscale.com/types/key"
)

// derpPeer is a bare DERP client standing in for a tailcat client.
type derpPeer struct {
	c    *derphttp.Client
	pkts chan derp.ReceivedPacket
}

func newDERPPeer(t *testing.T, reg *tailcfg.DERPRegion, priv key.NodePrivate) *derpPeer {
	t.Helper()
	c := derphttp.NewRegionClient(priv, t.Logf, netmon.NewStatic(), func() *tailcfg.DERPRegion { return reg })
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("DERP connect: %v", err)
	}
	p := &derpPeer{c: c, pkts: make(chan derp.ReceivedPacket, 16)}
	go func() {
		for {
			m, err := c.Recv()
			if err != nil {
				return
			}
			if pkt, ok := m.(derp.ReceivedPacket); ok {
				pkt.Data = append([]byte(nil), pkt.Data...)
				p.pkts <- pkt
			}
		}
	}()
	return p
}

// next returns the next packet from src within d, or nil.
func (p *derpPeer) next(src key.NodePublic, d time.Duration) []byte {
	timeout := time.After(d)
	for {
		select {
		case pkt := <-p.pkts:
			if pkt.Source == src {
				return pkt.Data
			}
		case <-timeout:
			return nil
		}
	}
}

func TestServerAsksAClientItHasNoSessionForToMeowAgain(t *testing.T) {
	t.Parallel()

	dm := integration.RunDERPAndSTUN(t, mkLogger(t, "derpstun"), "127.0.0.1")
	reg := dm.Regions[1]
	serverKey := key.NewNode()
	s := &Server{Key: serverKey, Logf: mkLogger(t, "server"), Region: reg}
	t.Cleanup(func() { s.Close() })
	if err := s.Start(); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	server := serverKey.Public()
	// Packets to a server not yet on DERP are dropped there.
	for deadline := time.Now().Add(10 * time.Second); s.lb.sys.HealthTracker.Get().GetDERPRegionReceivedTime(s.lb.derpRegionID()).IsZero(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("server never connected to DERP")
		}
	}
	wireGuardData := []byte{4, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0}

	// A client still sending on a session from before the server restarted.
	stale := newDERPPeer(t, reg, key.NewNode())
	if err := stale.c.Send(server, wireGuardData); err != nil {
		t.Fatal(err)
	}
	if got := stale.next(server, 5*time.Second); !IsMeowAgainPacket(got) {
		t.Fatalf("reply = %x; want meow again", got)
	}
	// At most once a second for one client.
	stale.c.Send(server, wireGuardData)
	if got := stale.next(server, 300*time.Millisecond); got != nil {
		t.Errorf("second reply within a second: %x", got)
	}

	// A client the server has admitted is not told to meow again.
	priv := key.NewNode()
	admitted := newDERPPeer(t, reg, priv)
	admitted.c.Send(server, EncodeMeowPing(priv.Public(), key.NewDisco().Public()))
	if got := admitted.next(server, 5*time.Second); !IsMeowedPacket(got) {
		t.Fatalf("reply to meow = %x; want meowed", got)
	}
	admitted.c.Send(server, wireGuardData)
	if got := admitted.next(server, 500*time.Millisecond); IsMeowAgainPacket(got) {
		t.Errorf("an admitted client was told to meow again")
	}

	// Nor is a client the server wouldn't admit.
	s.AddAllowedClient(key.NewNode().Public())
	stranger := newDERPPeer(t, reg, key.NewNode())
	stranger.c.Send(server, wireGuardData)
	if got := stranger.next(server, 500*time.Millisecond); got != nil {
		t.Errorf("a client not allowed got %x", got)
	}
}
