package timescale

import (
	"context"
	"sync"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/extensions/hooks"
)

// this file contains an endblock hook, which allows a node to create hypertables as-needed.
// This is a bit of a hack around Kwil v0.8. In v0.9, there is a much cleaner way to have a connection
// dedicated to only creating hypertables, but it is not released yet.

// makeHyperTable is an endblock hook that creates hypertables as-needed.
func makeHypertable(ctx context.Context, app *common.App, block *common.BlockContext) error {
	// get all the hypertables that need to be created
	slices := hypertables.getAndClear()

	// create the hypertables
	for _, slice := range slices {
		for _, config := range slice.schema.hypertables {
			// create the hypertable
			err := config.ensureApplied(ctx, app.DB, slice.schema.pgSchema)
			if err != nil {
				return err
			}
		}

		// signal that the hypertable has been created
		close(slice.done)
	}

	return nil
}

var _ hooks.EndBlockHook = makeHypertable

type schemaHypertables struct {
	pgSchema    string
	hypertables []*hypertableConfig
}

// safeSlice is a thread-safe slice that stores slices of hypertable configs (a group of configs
// tied to a schema) and a notification channel to signal when the hypertable has been created.
type safeSlice struct {
	sync.Mutex
	slices []struct {
		schema *schemaHypertables
		done   chan struct{}
	}
}

// add adds a slice of hypertable configs to the safeSlice and returns a channel that will be closed
func (s *safeSlice) add(schema *schemaHypertables) chan struct{} {
	s.Lock()
	defer s.Unlock()

	done := make(chan struct{})
	s.slices = append(s.slices, struct {
		schema *schemaHypertables
		done   chan struct{}
	}{
		schema: schema,
		done:   done,
	})

	return done
}

// getAndClear returns all the slices of hypertable configs and clears the safeSlice
func (s *safeSlice) getAndClear() []struct {
	schema *schemaHypertables
	done   chan struct{}
} {
	s.Lock()
	defer s.Unlock()

	slices := s.slices
	s.slices = nil

	return slices
}

var hypertables = &safeSlice{}
