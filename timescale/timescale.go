// package timescale implements a Kuneiform Precompile for timescaledb
package timescale

import (
	"context"
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/hooks"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
)

func RegisterTimescaleDB() {
	err := precompiles.RegisterPrecompile("timescaledb", initializeTimescale)
	if err != nil {
		panic(err)
	}

	err = hooks.RegisterEndBlockHook("make_hypertables", makeHypertable)
	if err != nil {
		panic(err)
	}
}

func initializeTimescale(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
	var hypertableConfigs []*hypertableConfig
	for _, table := range ctx.Schema.Tables {
		hypertableCfg, ok := metadata[table.Name]
		if !ok {
			continue
		}

		// split the dimensions
		dimensions := strings.Split(hypertableCfg, ",")
		if len(dimensions) == 0 {
			return nil, fmt.Errorf("hypertable %s must have at least one dimension", table.Name)
		}

		hypertableConfigs = append(hypertableConfigs, &hypertableConfig{
			tableName:  table.Name,
			dimensions: dimensions,
		})
	}

	ready := hypertables.add(&schemaHypertables{
		pgSchema:    "ds_" + ctx.Schema.DBID(),
		hypertables: hypertableConfigs,
	})

	return &timescaleInstance{
		// a bit of a hack, we can make this canonicalized with better extension tooling
		pgSchema: "ds_" + ctx.Schema.DBID(),
		ready:    ready,
	}, nil
}

var _ precompiles.Initializer = initializeTimescale

type timescaleInstance struct {
	// pgSchema is the name of the postgres schema to use
	pgSchema string
	ready    chan struct{}
}

func (t *timescaleInstance) Call(scoper *precompiles.ProcedureContext, app *common.App, method string, inputs []any) ([]any, error) {
	if err := t.ensureReady(scoper.TxCtx.Ctx); err != nil {
		return nil, err
	}

	accesser, ok := app.DB.(sql.AccessModer)
	if !ok {
		// this should never happen
		return nil, fmt.Errorf("cannot guarantee that the database is in a read-only state")
	}

	// set the postgres search path
	if err := t.ensurePGSchema(scoper.TxCtx.Ctx, app.DB); err != nil {
		return nil, err
	}
	defer t.unEnsurePGSchema(scoper.TxCtx.Ctx, app.DB)

	switch strings.ToLower(method) {
	default:
		return nil, fmt.Errorf("unknown method for timescale extension: %s", method)
	case "query":
		if accesser.AccessMode() != sql.ReadOnly {
			return nil, fmt.Errorf(`timescale extension's "query" method can only be used in read-only transactions, as it is non-deterministic`)
		}

		query, ok := inputs[0].(string)
		if !ok {
			return nil, fmt.Errorf("query must be a string")
		}

		// we may want to validate inputs here, but for now we'll just pass them through
		var err error
		scoper.Result, err = app.DB.Execute(scoper.TxCtx.Ctx, query, inputs[1:]...)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

// ensureInitialized ensures that the local table is initialized
func (t *timescaleInstance) ensureReady(ctx context.Context) error {
	for {
		select {
		case <-t.ready:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("error while waiting for node to create hypertables: %w", ctx.Err())
		}
	}
}

// ensurePGSchema ensures that the postgres schema is set in the search path
func (t *timescaleInstance) ensurePGSchema(ctx context.Context, db sql.Executor) error {
	_, err := db.Execute(ctx, "set search_path to "+t.pgSchema+";")
	return err
}

// unEnsurePGSchema ensures that the postgres schema is removed from the search path
func (t *timescaleInstance) unEnsurePGSchema(ctx context.Context, db sql.Executor) error {
	_, err := db.Execute(ctx, "set search_path to public;")
	return err
}

type hypertableConfig struct {
	tableName  string
	dimensions []string
}

// ensureApplied ensures that the hypertable is applied.
func (h *hypertableConfig) ensureApplied(ctx context.Context, db sql.Executor, pgSchema string) error {
	str := strings.Builder{}
	str.WriteString("select 1 from (select create_hypertable('")
	str.WriteString(pgSchema)
	str.WriteString(".")
	str.WriteString(h.tableName)
	str.WriteString("'")
	for _, dim := range h.dimensions {
		str.WriteString(", ")
		str.WriteString("'")
		str.WriteString(dim)
		str.WriteString("'")
	}
	str.WriteString("));")

	stmt := str.String()

	_, err := db.Execute(ctx, stmt)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "already a hypertable") {
		return nil
	}

	return err
}
