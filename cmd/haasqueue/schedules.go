package main

import "haasqueue/internal/scheduler"

// schedules returns the cron-driven enqueues this binary fires while
// it's the active leader. Add Tasks here to wire up periodic work;
// each entry is validated at startup, so a malformed cron expression
// is a fatal error rather than a silent skip.
//
// Empty by default — production deployments override this in their
// own fork or via a build tag if they need scheduled jobs. Keeping
// the list in code (vs env-driven) means the contract is enforced by
// the type system and changes show up in code review.
func schedules() []scheduler.Task {
	return nil
}
