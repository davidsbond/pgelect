package pgelect_test

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/davidsbond/pgelect"
)

func TestRun(t *testing.T) {
	db := testDB(t)

	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()

	updates := make(chan pgelect.CandidateStatus, 1)

	gotUpdate := false

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		for status := range updates {
			assert.NotEqual(t, pgelect.CandidateStatusUnknown, status)
			gotUpdate = true
		}

		return nil
	})

	group.Go(func() error {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		defer close(updates)

		return pgelect.Run(ctx, db, updates,
			pgelect.WithElectionInterval(time.Second),
			pgelect.WithLockID(69),
			pgelect.WithLogger(logger),
		)
	})

	err := group.Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	assert.True(t, gotUpdate)
}

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	u := &url.URL{
		Scheme:   "postgres",
		Host:     "localhost:5432",
		User:     url.UserPassword("postgres", "postgres"),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}

	db, err := pgxpool.New(t.Context(), u.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	require.NoError(t, db.Ping(t.Context()))

	return db
}
