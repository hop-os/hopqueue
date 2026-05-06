package eventbus

import (
	"sync"
	"testing"
	"time"
)

func TestSubscribeJob_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := New()
	ch, unsub := b.SubscribeJob("job-1")
	defer unsub()

	go b.PublishJob(Event{ID: 1, JobID: "job-1", Type: "enqueued"})

	select {
	case ev := <-ch:
		if ev.ID != 1 || ev.JobID != "job-1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("never received event")
	}
}

func TestPublishJob_NoSubscriber_NoOp(t *testing.T) {
	t.Parallel()
	b := New()
	b.PublishJob(Event{ID: 1, JobID: "nobody-listening", Type: "enqueued"})
	// Just verifying we don't deadlock or panic.
}

func TestSubscribeJob_OtherJobsNotDelivered(t *testing.T) {
	t.Parallel()
	b := New()
	ch, unsub := b.SubscribeJob("job-1")
	defer unsub()

	b.PublishJob(Event{ID: 1, JobID: "job-2", Type: "enqueued"})
	select {
	case ev := <-ch:
		t.Errorf("got unexpected event for job-2: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestSubscribeJob_DropsOnFullBuffer(t *testing.T) {
	t.Parallel()
	b := New()
	_, unsub := b.SubscribeJob("job-1")
	defer unsub()

	// Buffer is 32; flooding more than that without draining must not
	// block the publisher.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			b.PublishJob(Event{ID: int64(i + 1), JobID: "job-1", Type: "x"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PublishJob blocked on full buffer")
	}
}

func TestUnsub_StopsDelivery(t *testing.T) {
	t.Parallel()
	b := New()
	ch, unsub := b.SubscribeJob("job-1")

	b.PublishJob(Event{ID: 1, JobID: "job-1", Type: "x"})
	<-ch // consume the first

	unsub()
	b.PublishJob(Event{ID: 2, JobID: "job-1", Type: "x"})
	select {
	case ev := <-ch:
		t.Errorf("received event after unsub: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscribeLane_Coalesces(t *testing.T) {
	t.Parallel()
	b := New()
	ch, unsub := b.SubscribeLane("default")
	defer unsub()

	for i := 0; i < 10; i++ {
		b.SignalLane("default")
	}
	// At least one signal arrives.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("never received lane signal")
	}
	// Subsequent reads non-blocking should not all succeed (coalesced).
	select {
	case <-ch:
		t.Error("multiple signals not coalesced")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestConcurrentSubscribePublishUnsub(t *testing.T) {
	t.Parallel()
	b := New()
	const subs = 100

	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, unsub := b.SubscribeJob("job")
			defer unsub()
			b.PublishJob(Event{ID: 1, JobID: "job", Type: "x"})
			select {
			case <-ch:
			case <-time.After(time.Second):
			}
		}()
	}
	wg.Wait()
}
