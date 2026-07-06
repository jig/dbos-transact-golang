package dbos

// Transient system-database errors during Recv / GetEvent / Sleep must not
// consume fresh step IDs per retry attempt: the caller reserves the IDs once,
// when building the operation's input — outside the retry loop — and every
// attempt re-runs the same logical operation with them. When allocation
// instead happened inside the retried call, a transient failure (e.g.
// SQLITE_BUSY) leaked IDs and recorded a gapped history, and a replay
// re-executed inside the gap (observed as a fluxos8 golden-history
// divergence on the previous fork).
//
// The tests wrap the system database with a facade whose recv/getEvent/sleep
// fail the first attempts with a retryable (net.Error) fault while recording
// the step IDs each attempt carried; every attempt must carry the same IDs,
// and the operation must be recorded at step 0.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// simulatedNetError is matched by the retry condition chain's net.Error
// branch on any backend.
type simulatedNetError struct{}

func (simulatedNetError) Error() string   { return "simulated transient network error" }
func (simulatedNetError) Timeout() bool   { return true }
func (simulatedNetError) Temporary() bool { return true }

// flakyStepSysDB delegates to the real system database, failing the first N
// calls of each wait-style operation and recording the step IDs every attempt
// carried. It exercises the facade seam concrete() exists for.
type flakyStepSysDB struct {
	systemDatabase

	mu       sync.Mutex
	failLeft map[string]int
	seen     map[string][][]int // op -> per-attempt carried step IDs
}

func newFlakySysDB(real systemDatabase) *flakyStepSysDB {
	return &flakyStepSysDB{systemDatabase: real, failLeft: map[string]int{}, seen: map[string][][]int{}}
}

func (f *flakyStepSysDB) arm(op string, failures int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failLeft[op] = failures
	f.seen[op] = nil
}

func (f *flakyStepSysDB) observe(op string, ids ...int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen[op] = append(f.seen[op], ids)
	if f.failLeft[op] > 0 {
		f.failLeft[op]--
		return true
	}
	return false
}

func (f *flakyStepSysDB) attempts(op string) [][]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]int(nil), f.seen[op]...)
}

func (f *flakyStepSysDB) recv(ctx context.Context, input recvInput) (*recvResult, error) {
	if f.observe("recv", input.stepID, input.sleepStepID) {
		return nil, simulatedNetError{}
	}
	return f.systemDatabase.recv(ctx, input)
}

func (f *flakyStepSysDB) getEvent(ctx context.Context, input getEventInput) (*getEventResult, error) {
	if input.isInWorkflow && f.observe("getEvent", input.stepID, input.sleepStepID) {
		return nil, simulatedNetError{}
	}
	return f.systemDatabase.getEvent(ctx, input)
}

func (f *flakyStepSysDB) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	if f.observe("sleep", input.stepID) {
		return 0, simulatedNetError{}
	}
	return f.systemDatabase.sleep(ctx, input)
}

// requireStableIDs asserts every attempt carried the same pre-reserved IDs.
func requireStableIDs(t *testing.T, attempts [][]int, wantAttempts int) {
	t.Helper()
	require.Len(t, attempts, wantAttempts)
	for i, a := range attempts {
		require.Equal(t, attempts[0], a, "attempt %d used different step IDs than attempt 0 (allocation leaked into the retry loop)", i)
	}
}

// requireStepAtID asserts that the workflow recorded functionName at stepID.
func requireStepAtID(t *testing.T, ctx DBOSContext, workflowID, functionName string, stepID int) {
	t.Helper()
	steps, err := GetWorkflowSteps(ctx, workflowID)
	require.NoError(t, err)
	for _, s := range steps {
		if s.StepName == functionName {
			require.Equal(t, stepID, s.StepID,
				"%s recorded at step %d; transient retries leaked step IDs", functionName, s.StepID)
			return
		}
	}
	t.Fatalf("workflow %s did not record a %s step (got %+v)", workflowID, functionName, steps)
}

func TestTransientRetriesDoNotLeakStepIDs(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	flaky := newFlakySysDB(dbosCtx.(*dbosContext).systemDB)
	dbosCtx.(*dbosContext).systemDB = flaky

	recvWF := func(ctx DBOSContext, _ string) (string, error) {
		return Recv[string](ctx, "signal", 10*time.Second)
	}
	sleepWF := func(ctx DBOSContext, _ string) (string, error) {
		if _, err := Sleep(ctx, 10*time.Millisecond); err != nil {
			return "", err
		}
		return "slept", nil
	}
	setterWF := func(ctx DBOSContext, _ string) (string, error) {
		return "set", SetEvent(ctx, "key", "value")
	}
	getterWF := func(ctx DBOSContext, target string) (string, error) {
		return GetEvent[string](ctx, target, "key", 10*time.Second)
	}
	RegisterWorkflow(dbosCtx, recvWF)
	RegisterWorkflow(dbosCtx, sleepWF)
	RegisterWorkflow(dbosCtx, setterWF)
	RegisterWorkflow(dbosCtx, getterWF)
	require.NoError(t, Launch(dbosCtx))

	t.Run("recv", func(t *testing.T) {
		flaky.arm("recv", 2)
		h, err := RunWorkflow(dbosCtx, recvWF, "", WithWorkflowID("flaky-recv"))
		require.NoError(t, err)
		require.NoError(t, Send(dbosCtx, "flaky-recv", "hello", "signal"))
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "hello", res)
		requireStableIDs(t, flaky.attempts("recv"), 3)
		requireStepAtID(t, dbosCtx, "flaky-recv", "DBOS.recv", 0)
	})

	t.Run("sleep", func(t *testing.T) {
		flaky.arm("sleep", 2)
		h, err := RunWorkflow(dbosCtx, sleepWF, "", WithWorkflowID("flaky-sleep"))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
		requireStableIDs(t, flaky.attempts("sleep"), 3)
		requireStepAtID(t, dbosCtx, "flaky-sleep", "DBOS.sleep", 0)
	})

	t.Run("getEvent", func(t *testing.T) {
		hSet, err := RunWorkflow(dbosCtx, setterWF, "", WithWorkflowID("flaky-setter"))
		require.NoError(t, err)
		_, err = hSet.GetResult()
		require.NoError(t, err)

		flaky.arm("getEvent", 2)
		h, err := RunWorkflow(dbosCtx, getterWF, "flaky-setter", WithWorkflowID("flaky-getter"))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "value", res)
		requireStableIDs(t, flaky.attempts("getEvent"), 3)
		requireStepAtID(t, dbosCtx, "flaky-getter", "DBOS.getEvent", 0)
	})
}
