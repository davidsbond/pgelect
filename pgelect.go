// Package pgelect provides a simple leadership election mechanism using PostgreSQL advisory locks.
package pgelect

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	// The CandidateStatus type is an integer representing the current candidate's status between candidates.
	CandidateStatus int

	// The Option type is a function that configures aspects of the leader election mechanism.
	Option func(*options)

	options struct {
		lockID           uint64
		electionInterval time.Duration
		logger           *slog.Logger
	}
)

const (
	// CandidateStatusUnknown is the initial status of a candidate prior to attempting election.
	CandidateStatusUnknown CandidateStatus = iota
	// CandidateStatusLeader indicates that the candidate has successfully obtained the advisory lock and become
	// the leader.
	CandidateStatusLeader
	// CandidateStatusFollower indicates the candidate did not obtain or lost the advisory lock and has become a follower.
	CandidateStatusFollower
)

func defaultOptions() options {
	return options{
		lockID:           1337,
		electionInterval: time.Second,
		logger:           slog.New(slog.DiscardHandler),
	}
}

// WithLockID is an Option implementation that modifies the integer value used by the leader election mechanism as
// the lock identifier passed into pg_try_advisory_lock. Defaults to 1337.
func WithLockID(id uint64) Option {
	return func(c *options) {
		c.lockID = id
	}
}

// WithElectionInterval is an Option implementation that modifies the frequency at which followers will attempt to
// obtain leadership and the frequency at which leaders will query their own leadership status. Defaults to 1 second.
// Setting a high value for election intervals will cause a delay between new leaders begin elected, which should
// be considered when modifying it.
func WithElectionInterval(interval time.Duration) Option {
	return func(c *options) {
		c.electionInterval = interval
	}
}

// WithLogger is an Option implementation that modifies the logger used by the election process. By default, it uses
// the slog.DiscardHandler which will not produce any logs. This is mainly intended for debugging purposes as logs
// produced by this package may be noisy.
func WithLogger(logger *slog.Logger) Option {
	return func(c *options) {
		c.logger = logger.WithGroup("pgelect")
	}
}

// Run the leader election process using the provided pgxpool.Pool instance. This function will attempt to acquire an
// advisory lock to indicate leadership status. If the lock is acquired, this candidate becomes the leader. The leader
// will then periodically check it still maintains ownership of the lock.
//
// Changes in candidate status are written to the provided channel and must be read to avoid blocking the election
// process. This function blocks until an error occurs or the provided context.Context is cancelled. Zero or more Option
// implementations can be provided to modify the behaviour of this candidate.
func Run(ctx context.Context, db *pgxpool.Pool, updates chan<- CandidateStatus, options ...Option) error {
	opts := defaultOptions()
	for _, opt := range options {
		opt(&opts)
	}

	conn, err := db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// We use a short timer here so that we immediately attempt an election. This timer is then reset to the configured
	// interval.
	timer := time.NewTimer(time.Nanosecond)
	defer timer.Stop()

	status := CandidateStatusUnknown
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			opts.logger.InfoContext(ctx, "attempting election")
			timer.Reset(opts.electionInterval)

			// Attempt to become the leader using the advisory lock.
			newStatus, err := attemptElection(ctx, conn, opts)
			if err != nil {
				return fmt.Errorf("error while attempting election: %w", err)
			}

			// If our status has changed from its initial or previous value, push an update to the channel.
			if status != newStatus {
				status = newStatus
				updates <- status
			}

			if status != CandidateStatusLeader {
				continue
			}

			opts.logger.InfoContext(ctx, "became leader")

			// If we've become the leader, we then monitor our leadership status periodically. This will block for
			// the duration of our leadership.
			if err = watchLeadershipStatus(ctx, conn, opts); err != nil {
				return fmt.Errorf("error while monitoring leadership status: %w", err)
			}

			// If we're here, we've become a follower.
			opts.logger.InfoContext(ctx, "became follower")
			status = CandidateStatusFollower
			updates <- CandidateStatusFollower
		}
	}
}

func attemptElection(ctx context.Context, conn *pgxpool.Conn, opts options) (CandidateStatus, error) {
	logger := opts.logger.With(slog.Uint64("lock_id", opts.lockID))

	const q = `SELECT pg_try_advisory_lock($1)`

	// Attempt to obtain an advisory lock with the configured identifier. If we do obtain it, we've assumed
	// leadership.
	var elected bool
	logger.DebugContext(ctx, "attempting to acquire lock")
	if err := conn.QueryRow(ctx, q, opts.lockID).Scan(&elected); err != nil {
		return 0, err
	}

	if elected {
		logger.DebugContext(ctx, "lock acquired")
		return CandidateStatusLeader, nil
	}

	return CandidateStatusFollower, nil
}

func watchLeadershipStatus(ctx context.Context, conn *pgxpool.Conn, opts options) error {
	logger := opts.logger.With(slog.Uint64("lock_id", opts.lockID))

	const q = `
		SELECT EXISTS(
			SELECT 1 FROM pg_locks 
			WHERE pid = PG_BACKEND_PID() AND locktype = 'advisory' AND objid = $1
		)
	`

	ticker := time.NewTicker(opts.electionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var exists bool

			// Periodically check if our connection is the one that owns the lock. If not, we've lost our leadership
			// status and should return.
			logger.DebugContext(ctx, "checking lock status")
			if err := conn.QueryRow(ctx, q, opts.lockID).Scan(&exists); err != nil {
				return err
			}

			if !exists {
				logger.DebugContext(ctx, "lock lost")
				return nil
			}
		}
	}
}
