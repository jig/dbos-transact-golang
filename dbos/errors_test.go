package dbos

import (
	"errors"
	"testing"
)

// TestErrorFromRecorded checks that a recorded step-error string is rebuilt into
// a *DBOSError preserving its code, so errors.As keeps working across a recovery
// or durable-suspension replay (regression for recv/getEvent timeout detection).
func TestErrorFromRecorded(t *testing.T) {
	orig := newTimeoutError("wf-1", "DBOS.recv", "no message received within 5s")

	got := errorFromRecorded(orig.Error())

	var de *DBOSError
	if !errors.As(got, &de) {
		t.Fatalf("errors.As failed to recover *DBOSError: got %T (%v)", got, got)
	}
	if de.Code != TimeoutError {
		t.Fatalf("recovered code = %d, want %d (TimeoutError)", de.Code, TimeoutError)
	}
	if got.Error() != orig.Error() {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", got.Error(), orig.Error())
	}

	// Text that is not a DBOSError render falls back to a plain error.
	var none *DBOSError
	plain := errorFromRecorded("some other failure")
	if errors.As(plain, &none) {
		t.Fatalf("plain text should not parse as *DBOSError: %v", plain)
	}
	if plain.Error() != "some other failure" {
		t.Fatalf("plain text round-trip = %q", plain.Error())
	}
}
