package dbos

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jig/dbos-transact-golang/dbos/internal/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForStatus polls the workflow status until it matches the expected status or times out.
func waitForStatus(t *testing.T, handle WorkflowHandle[string], expected WorkflowStatusType, timeout time.Duration) WorkflowStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var status WorkflowStatus
	for time.Now().Before(deadline) {
		var err error
		status, err = handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		if status.Status == expected {
			return status
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not reach status %s within %v (last status: %s)", handle.GetWorkflowID(), expected, timeout, status.Status)
	return status
}

func TestDurableSleepSuspension(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, durableSleepThreshold: 200 * time.Millisecond})

	userQueue := NewWorkflowQueue(dbosCtx, "durable-sleep-queue",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))

	var bodyExecutions, stepExecutions atomic.Int64
	sleepingWorkflow := func(ctx DBOSContext, input string) (string, error) {
		bodyExecutions.Add(1)
		stepResult, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
			stepExecutions.Add(1)
			return input + "-step", nil
		})
		if err != nil {
			return "", err
		}
		if _, err := Sleep(ctx, 2*time.Second); err != nil {
			return "", err
		}
		return stepResult + "-done", nil
	}

	var loopBodyExecutions, loopStepExecutions atomic.Int64
	loopingWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		loopBodyExecutions.Add(1)
		for range 2 {
			if _, err := RunAsStep(ctx, func(ctx context.Context) (int64, error) {
				return loopStepExecutions.Add(1), nil
			}); err != nil {
				return "", err
			}
			if _, err := Sleep(ctx, 1*time.Second); err != nil {
				return "", err
			}
		}
		return "looped", nil
	}

	var shortBodyExecutions atomic.Int64
	shortSleepWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		shortBodyExecutions.Add(1)
		if _, err := Sleep(ctx, 100*time.Millisecond); err != nil {
			return "", err
		}
		return "short-done", nil
	}

	RegisterWorkflow(dbosCtx, sleepingWorkflow)
	RegisterWorkflow(dbosCtx, loopingWorkflow)
	RegisterWorkflow(dbosCtx, shortSleepWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("DirectRunWorkflowSuspends", func(t *testing.T) {
		bodyExecutions.Store(0)
		stepExecutions.Store(0)
		tBefore := time.Now()

		handle, err := RunWorkflow(dbosCtx, sleepingWorkflow, "in")
		require.NoError(t, err, "failed to start workflow")

		// The workflow must suspend to DELAYED and be parked on the internal queue
		status := waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)
		assert.Equal(t, models.InternalQueueName, status.QueueName, "direct-run workflow should be parked on the internal queue")
		tolerance := 200 * time.Millisecond
		assert.True(t, status.DelayUntil.After(tBefore.Add(2*time.Second).Add(-tolerance)),
			"delay_until should be around tBefore + sleep duration (delay_until=%v)", status.DelayUntil)

		// The in-memory handle must still deliver the final result (polling fallback)
		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result after suspension")
		assert.Equal(t, "in-step-done", result)

		finalStatus, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)

		// The body re-ran on wake-up; the step was memoized and ran only once
		assert.Equal(t, int64(2), bodyExecutions.Load(), "workflow body should run once before and once after suspension")
		assert.Equal(t, int64(1), stepExecutions.Load(), "step should be memoized across suspension")
		assert.Equal(t, 1, finalStatus.Attempts, "suspension should reset the recovery attempt counter")
	})

	t.Run("EnqueuedWorkflowSuspendsOnItsQueue", func(t *testing.T) {
		bodyExecutions.Store(0)
		stepExecutions.Store(0)

		handle, err := RunWorkflow(dbosCtx, sleepingWorkflow, "queued", WithQueue(userQueue.Name))
		require.NoError(t, err, "failed to enqueue workflow")

		status := waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)
		assert.Equal(t, userQueue.Name, status.QueueName, "enqueued workflow should stay on its own queue")

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result after suspension")
		assert.Equal(t, "queued-step-done", result)

		assert.Equal(t, int64(2), bodyExecutions.Load())
		assert.Equal(t, int64(1), stepExecutions.Load())
	})

	t.Run("RepeatedSleepsSuspendRepeatedly", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, loopingWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result")
		assert.Equal(t, "looped", result)

		finalStatus, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)

		// One initial run plus one wake-up per sleep
		assert.Equal(t, int64(3), loopBodyExecutions.Load(), "workflow body should run once per suspension plus the initial run")
		assert.Equal(t, int64(2), loopStepExecutions.Load(), "each loop step should be memoized across suspensions")
		assert.Equal(t, 1, finalStatus.Attempts, "repeated suspensions should not accumulate recovery attempts")
	})

	t.Run("SleepBelowThresholdDoesNotSuspend", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, shortSleepWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result")
		assert.Equal(t, "short-done", result)
		assert.Equal(t, int64(1), shortBodyExecutions.Load(), "short sleeps must keep run-once semantics")
	})
}

func TestDurableResultSuspension(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, durableSleepThreshold: 200 * time.Millisecond})

	childQueue := NewWorkflowQueue(dbosCtx, "durable-result-queue",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))

	var sleepingChildBody atomic.Int64
	sleepingChild := func(ctx DBOSContext, input string) (string, error) {
		sleepingChildBody.Add(1)
		if _, err := Sleep(ctx, 1500*time.Millisecond); err != nil {
			return "", err
		}
		return input + "-child", nil
	}

	var sleepingParentBody atomic.Int64
	sleepingParent := func(ctx DBOSContext, input string) (string, error) {
		sleepingParentBody.Add(1)
		handle, err := RunWorkflow(ctx, sleepingChild, input)
		if err != nil {
			return "", err
		}
		res, err := handle.GetResult()
		if err != nil {
			return "", err
		}
		return res + "-parent", nil
	}

	var slowChildBody atomic.Int64
	slowChild := func(ctx DBOSContext, input string) (string, error) {
		slowChildBody.Add(1)
		return RunAsStep(ctx, func(ctx context.Context) (string, error) {
			time.Sleep(1 * time.Second)
			return input + "-slow", nil
		})
	}

	var slowParentBody atomic.Int64
	slowParent := func(ctx DBOSContext, input string) (string, error) {
		slowParentBody.Add(1)
		handle, err := RunWorkflow(ctx, slowChild, input, WithQueue(childQueue.Name))
		if err != nil {
			return "", err
		}
		res, err := handle.GetResult()
		if err != nil {
			return "", err
		}
		return res + "-parent", nil
	}

	var fastChildBody atomic.Int64
	fastChild := func(ctx DBOSContext, input string) (string, error) {
		fastChildBody.Add(1)
		return input + "-fast", nil
	}

	var fastParentBody atomic.Int64
	fastParent := func(ctx DBOSContext, input string) (string, error) {
		fastParentBody.Add(1)
		handle, err := RunWorkflow(ctx, fastChild, input)
		if err != nil {
			return "", err
		}
		res, err := handle.GetResult()
		if err != nil {
			return "", err
		}
		return res + "-parent", nil
	}

	var grandParentBody atomic.Int64
	grandParent := func(ctx DBOSContext, input string) (string, error) {
		grandParentBody.Add(1)
		handle, err := RunWorkflow(ctx, sleepingParent, input)
		if err != nil {
			return "", err
		}
		res, err := handle.GetResult()
		if err != nil {
			return "", err
		}
		return res + "-grandparent", nil
	}

	RegisterWorkflow(dbosCtx, sleepingChild)
	RegisterWorkflow(dbosCtx, sleepingParent)
	RegisterWorkflow(dbosCtx, slowChild)
	RegisterWorkflow(dbosCtx, slowParent)
	RegisterWorkflow(dbosCtx, fastChild)
	RegisterWorkflow(dbosCtx, fastParent)
	RegisterWorkflow(dbosCtx, grandParent)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ParentSuspendsWhileChildSleeps", func(t *testing.T) {
		sleepingChildBody.Store(0)
		sleepingParentBody.Store(0)

		handle, err := RunWorkflow(dbosCtx, sleepingParent, "in")
		require.NoError(t, err, "failed to start parent workflow")

		// The child suspends on its Sleep; the suspension must cascade to the parent
		status := waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)
		assert.Equal(t, models.InternalQueueName, status.QueueName)

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get parent result")
		assert.Equal(t, "in-child-parent", result)

		finalStatus, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)

		assert.Equal(t, int64(2), sleepingParentBody.Load(), "parent should run once before and once after suspension")
		assert.Equal(t, int64(2), sleepingChildBody.Load(), "child should run once before and once after its sleep suspension")
		assert.Equal(t, 1, finalStatus.Attempts, "result suspension should reset the recovery attempt counter")
	})

	t.Run("ParentSuspendsOnSlowQueuedChild", func(t *testing.T) {
		slowChildBody.Store(0)
		slowParentBody.Store(0)

		handle, err := RunWorkflow(dbosCtx, slowParent, "in")
		require.NoError(t, err, "failed to start parent workflow")

		// The child does not suspend (it works inside a step), but it outlasts the
		// threshold, so the parent must suspend while waiting for it. The DELAYED
		// window can be too short to observe reliably by polling under load; the
		// body-count assertion below is the durable evidence of the suspension
		// (a parent that never suspended would run its body exactly once).
		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get parent result")
		assert.Equal(t, "in-slow-parent", result)

		assert.Equal(t, int64(2), slowParentBody.Load(), "parent should run once before and once after suspension")
		assert.Equal(t, int64(1), slowChildBody.Load(), "child should run exactly once")
	})

	t.Run("FastChildDoesNotSuspendParent", func(t *testing.T) {
		fastChildBody.Store(0)
		fastParentBody.Store(0)

		handle, err := RunWorkflow(dbosCtx, fastParent, "in")
		require.NoError(t, err, "failed to start parent workflow")

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get parent result")
		assert.Equal(t, "in-fast-parent", result)

		assert.Equal(t, int64(1), fastParentBody.Load(), "parent waiting under the threshold must keep run-once semantics")
		assert.Equal(t, int64(1), fastChildBody.Load())
	})

	t.Run("ThreeLevelCascade", func(t *testing.T) {
		sleepingChildBody.Store(0)
		sleepingParentBody.Store(0)
		grandParentBody.Store(0)

		handle, err := RunWorkflow(dbosCtx, grandParent, "in")
		require.NoError(t, err, "failed to start grandparent workflow")

		// The child's sleep suspension must cascade up through parent and grandparent
		waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get grandparent result")
		assert.Equal(t, "in-child-parent-grandparent", result)

		assert.Equal(t, int64(2), grandParentBody.Load(), "grandparent should run once before and once after suspension")
		assert.Equal(t, int64(2), sleepingParentBody.Load(), "parent should run once before and once after suspension")
		assert.Equal(t, int64(2), sleepingChildBody.Load(), "child should run once before and once after its sleep suspension")
	})
}

func TestDurableRecvSuspension(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, durableSleepThreshold: 200 * time.Millisecond})

	var recvBody atomic.Int64
	recvWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		recvBody.Add(1)
		msg, err := Recv[string](ctx, "signal", 30*time.Second)
		if err != nil {
			return "", err
		}
		return "got-" + msg, nil
	}

	var timeoutBody atomic.Int64
	timeoutWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		timeoutBody.Add(1)
		if _, err := Recv[string](ctx, "never", 1*time.Second); err != nil {
			// The timeout surfaces on the post-suspension replay: the recorded error
			// must still carry its DBOSError code (regression for errorFromRecorded).
			var dbosErr *DBOSError
			if errors.As(err, &dbosErr) && dbosErr.Code == TimeoutError {
				return "timed-out", nil
			}
			return "", fmt.Errorf("expected a TimeoutError, got: %w", err)
		}
		return "unexpected-message", nil
	}

	senderWorkflow := func(ctx DBOSContext, destinationID string) (string, error) {
		if err := Send(ctx, destinationID, "from-workflow", "signal"); err != nil {
			return "", err
		}
		return "sent", nil
	}

	preludeEvent := NewEvent()
	var gatedBody atomic.Int64
	gatedRecvWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		gatedBody.Add(1)
		if _, err := RunAsStep(ctx, func(c context.Context) (string, error) {
			preludeEvent.Wait() // hold the workflow here until the message is in the database
			return "gated", nil
		}); err != nil {
			return "", err
		}
		msg, err := Recv[string](ctx, "sig2", 30*time.Second)
		if err != nil {
			return "", err
		}
		return "got-" + msg, nil
	}

	RegisterWorkflow(dbosCtx, recvWorkflow)
	RegisterWorkflow(dbosCtx, timeoutWorkflow)
	RegisterWorkflow(dbosCtx, senderWorkflow)
	RegisterWorkflow(dbosCtx, gatedRecvWorkflow)
	require.NoError(t, Launch(dbosCtx))

	t.Run("SendWakesSuspendedReceiver", func(t *testing.T) {
		recvBody.Store(0)
		handle, err := RunWorkflow(dbosCtx, recvWorkflow, "")
		require.NoError(t, err, "failed to start receiver workflow")

		// With a 30s timeout and a 200ms threshold, the receiver must suspend
		status := waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)
		assert.Equal(t, models.InternalQueueName, status.QueueName)

		tSend := time.Now()
		require.NoError(t, Send(dbosCtx, handle.GetWorkflowID(), "hello", "signal"))

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get receiver result")
		assert.Equal(t, "got-hello", result)
		assert.Less(t, time.Since(tSend), 15*time.Second, "the send must wake the receiver well before the 30s recv timeout")
		assert.Equal(t, int64(2), recvBody.Load(), "receiver should run once before and once after suspension")
	})

	t.Run("WorkflowSendWakesSuspendedReceiver", func(t *testing.T) {
		recvBody.Store(0)
		receiver, err := RunWorkflow(dbosCtx, recvWorkflow, "")
		require.NoError(t, err, "failed to start receiver workflow")
		waitForStatus(t, receiver, WorkflowStatusDelayed, 5*time.Second)

		// The send goes through the in-workflow (transactional step) path
		sender, err := RunWorkflow(dbosCtx, senderWorkflow, receiver.GetWorkflowID())
		require.NoError(t, err, "failed to start sender workflow")
		_, err = sender.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get sender result")

		result, err := receiver.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get receiver result")
		assert.Equal(t, "got-from-workflow", result)
		assert.Equal(t, int64(2), recvBody.Load())
	})

	t.Run("TimeoutWhileSuspended", func(t *testing.T) {
		timeoutBody.Store(0)
		tStart := time.Now()
		handle, err := RunWorkflow(dbosCtx, timeoutWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result")
		assert.Equal(t, "timed-out", result)
		assert.GreaterOrEqual(t, time.Since(tStart), 1*time.Second, "must not report a timeout before the recv deadline")
		assert.Equal(t, int64(2), timeoutBody.Load(), "workflow should run once before and once after suspension")
	})

	t.Run("PendingMessageDoesNotSuspend", func(t *testing.T) {
		gatedBody.Store(0)
		handle, err := RunWorkflow(dbosCtx, gatedRecvWorkflow, "")
		require.NoError(t, err, "failed to start gated workflow")

		// Deliver the message while the workflow is still held inside its first step
		require.NoError(t, Send(dbosCtx, handle.GetWorkflowID(), "early", "sig2"))
		preludeEvent.Set()

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get workflow result")
		assert.Equal(t, "got-early", result)
		assert.Equal(t, int64(1), gatedBody.Load(), "a recv with a pending message must keep run-once semantics")
	})
}

func TestDurableGetEventSuspension(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, durableSleepThreshold: 200 * time.Millisecond})

	// The producer sets the event between two gates, so the tests can observe the
	// consumer being woken by SetEvent while the producer is still running (i.e. the
	// wake is event-driven, not the producer's completion).
	setGate := NewEvent()
	finishGate := NewEvent()
	producerWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		if _, err := RunAsStep(ctx, func(c context.Context) (string, error) {
			setGate.Wait()
			return "", nil
		}); err != nil {
			return "", err
		}
		if err := SetEvent(ctx, "status", "ready"); err != nil {
			return "", err
		}
		if _, err := RunAsStep(ctx, func(c context.Context) (string, error) {
			finishGate.Wait()
			return "", nil
		}); err != nil {
			return "", err
		}
		return "produced", nil
	}

	var consumerBody atomic.Int64
	consumerWorkflow := func(ctx DBOSContext, targetID string) (string, error) {
		consumerBody.Add(1)
		val, err := GetEvent[string](ctx, targetID, "status", 30*time.Second)
		if err != nil {
			return "", err
		}
		return "got-" + val, nil
	}

	var timeoutBody atomic.Int64
	timeoutConsumerWorkflow := func(ctx DBOSContext, targetID string) (string, error) {
		timeoutBody.Add(1)
		if _, err := GetEvent[string](ctx, targetID, "missing", 1*time.Second); err != nil {
			// The timeout surfaces on the post-suspension replay: the recorded error
			// must still carry its DBOSError code (regression for errorFromRecorded).
			var dbosErr *DBOSError
			if errors.As(err, &dbosErr) && dbosErr.Code == TimeoutError {
				return "timed-out", nil
			}
			return "", fmt.Errorf("expected a TimeoutError, got: %w", err)
		}
		return "unexpected-event", nil
	}

	RegisterWorkflow(dbosCtx, producerWorkflow)
	RegisterWorkflow(dbosCtx, consumerWorkflow)
	RegisterWorkflow(dbosCtx, timeoutConsumerWorkflow)
	require.NoError(t, Launch(dbosCtx))

	producer, err := RunWorkflow(dbosCtx, producerWorkflow, "")
	require.NoError(t, err, "failed to start producer workflow")
	t.Cleanup(func() {
		setGate.Set()
		finishGate.Set()
	})

	t.Run("SetEventWakesSuspendedConsumer", func(t *testing.T) {
		consumerBody.Store(0)
		consumer, err := RunWorkflow(dbosCtx, consumerWorkflow, producer.GetWorkflowID())
		require.NoError(t, err, "failed to start consumer workflow")

		// With a 30s timeout and a 200ms threshold, the consumer must suspend
		waitForStatus(t, consumer, WorkflowStatusDelayed, 5*time.Second)

		setGate.Set() // the producer sets the event (and keeps running)

		result, err := consumer.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get consumer result")
		assert.Equal(t, "got-ready", result)
		assert.Equal(t, int64(2), consumerBody.Load(), "consumer should run once before and once after suspension")

		// The wake came from SetEvent: the producer is still running
		producerStatus, err := producer.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusPending, producerStatus.Status, "producer must still be running when the consumer wakes")
	})

	t.Run("TimeoutWhileSuspended", func(t *testing.T) {
		timeoutBody.Store(0)
		tStart := time.Now()
		handle, err := RunWorkflow(dbosCtx, timeoutConsumerWorkflow, producer.GetWorkflowID())
		require.NoError(t, err, "failed to start timeout consumer")

		waitForStatus(t, handle, WorkflowStatusDelayed, 5*time.Second)

		result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get timeout consumer result")
		assert.Equal(t, "timed-out", result)
		assert.GreaterOrEqual(t, time.Since(tStart), 1*time.Second, "must not report a timeout before the getEvent deadline")
		assert.Equal(t, int64(2), timeoutBody.Load())
	})

	t.Run("ExistingEventDoesNotSuspend", func(t *testing.T) {
		consumerBody.Store(0)
		consumer, err := RunWorkflow(dbosCtx, consumerWorkflow, producer.GetWorkflowID())
		require.NoError(t, err, "failed to start consumer workflow")

		result, err := consumer.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
		require.NoError(t, err, "failed to get consumer result")
		assert.Equal(t, "got-ready", result)
		assert.Equal(t, int64(1), consumerBody.Load(), "a getEvent on an already-set event must keep run-once semantics")
	})

	finishGate.Set()
	_, err = producer.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
	require.NoError(t, err, "failed to get producer result")
}

func TestDurableSleepDisabledByDefault(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	var bodyExecutions atomic.Int64
	sleepingWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		bodyExecutions.Add(1)
		if _, err := Sleep(ctx, 1*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, sleepingWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	handle, err := RunWorkflow(dbosCtx, sleepingWorkflow, "")
	require.NoError(t, err, "failed to start workflow")

	result, err := handle.GetResult(WithHandleTimeout(60*time.Second), WithHandlePollingInterval(100*time.Millisecond))
	require.NoError(t, err, "failed to get workflow result")
	assert.Equal(t, "done", result)
	assert.Equal(t, int64(1), bodyExecutions.Load(), "without a threshold the workflow must not suspend")

	finalStatus, err := handle.GetStatus()
	require.NoError(t, err)
	assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)
}
