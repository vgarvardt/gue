package gue

import (
	"errors"
	"fmt"
	"time"
)

// ErrJobPanicked is returned when the job failed ot be handled because it is panicked.
// Error is normally returned wrapped, so use `errors.Is(err, gue.ErrJobPanicked)` to ensure this is the error you're
// looking for.
var ErrJobPanicked = errors.New("job panicked")

// ErrJobReschedule interface implementation allows errors to reschedule jobs in the individual basis.
type ErrJobReschedule interface {
	rescheduleJobAt() time.Time
}

type errJobRescheduleIn struct {
	d time.Duration
	s string
}

// ErrRescheduleJobIn spawns an error that reschedules a job to run after some predefined duration.
func ErrRescheduleJobIn(d time.Duration, reason string) error {
	return errJobRescheduleIn{d: d, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleIn) Error() string {
	return fmt.Sprintf("rescheduling job in %q because %q", e.d.String(), e.s)
}

func (e errJobRescheduleIn) rescheduleJobAt() time.Time {
	return time.Now().Add(e.d)
}

type errJobRescheduleAt struct {
	t time.Time
	s string
}

// ErrRescheduleJobAt spawns an error that reschedules a job to run at some predefined time.
func ErrRescheduleJobAt(t time.Time, reason string) error {
	return errJobRescheduleAt{t: t, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleAt) Error() string {
	return fmt.Sprintf("rescheduling job at %q because %q", e.t.String(), e.s)
}

func (e errJobRescheduleAt) rescheduleJobAt() time.Time {
	return e.t
}

type errJobDiscard struct {
	s string
}

// ErrDiscardJob spawns an error that unconditionally discards a job.
func ErrDiscardJob(reason string) error {
	return errJobDiscard{s: reason}
}

// Error implements error.Error()
func (e errJobDiscard) Error() string {
	return fmt.Sprintf("discarding job because %q", e.s)
}

func (e errJobDiscard) rescheduleJobAt() time.Time {
	return time.Time{}
}
