package engine

import (
	"context"
	"log/slog"
	"time"
)

type Config struct {
	TasksPerWorker int64
	MaxWorkers     int32
	MinWorkers     int32
	CooldownPeriod time.Duration
}

type BacklogProvider interface {
	GetBacklog(ctx context.Context) (int64, error)
}

type WorkloadScaler interface {
	GetReplicas(ctx context.Context) (int32, error)
	SetReplicas(ctx context.Context, replicas int32) error
}

type Engine struct {
	logger          *slog.Logger
	provider        BacklogProvider
	scaler          WorkloadScaler
	config          Config
	lastScaleUpTime time.Time
}

func New(logger *slog.Logger, p BacklogProvider, s WorkloadScaler, cfg Config) *Engine {
	return &Engine{
		logger:   logger.WithGroup("streamscaler-engine"),
		provider: p,
		scaler:   s,
		config:   cfg,
	}
}

func (e *Engine) Reconcile(ctx context.Context) error {
	backlog, err := e.provider.GetBacklog(ctx)
	if err != nil {
		return err
	}

	desired := e.calculateReplicas(backlog)

	current, err := e.scaler.GetReplicas(ctx)
	if err != nil {
		return err
	}

	if desired < current {
		if time.Since(e.lastScaleUpTime) < e.config.CooldownPeriod {
			e.logger.Debug("Cooldown active, skipping scale-down")
			return nil
		}
	} else if desired > current {
		e.lastScaleUpTime = time.Now()
	}

	if current != desired {
		if err := e.scaler.SetReplicas(ctx, desired); err != nil {
			return err
		}
		e.logger.Debug("Deployment scaled successfully", "from", current, "to", desired)
	}
	return nil
}

func (e *Engine) calculateReplicas(backlog int64) int32 {
	desired := backlog / e.config.TasksPerWorker
	if backlog%e.config.TasksPerWorker != 0 {
		desired++
	}

	if desired < int64(e.config.MinWorkers) {
		desired = int64(e.config.MinWorkers)
	}

	if desired > int64(e.config.MaxWorkers) {
		desired = int64(e.config.MaxWorkers)
	}
	return int32(desired)
}
