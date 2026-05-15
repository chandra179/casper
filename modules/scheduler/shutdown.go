package scheduler

import (
	"context"
	"log"

	"github.com/google/uuid"

	"casper/modules/task"
)

type TaskResetter interface {
	ResetTask(ctx context.Context, id uuid.UUID, version int64) error
}

func (s *Scheduler) drainRemaining(ctx context.Context, tasks []*task.Task) {
	if s.resetter == nil || s.cfg.ShutdownDrainTimeout <= 0 {
		return
	}

	log.Printf("shutdown: draining %d claimed tasks (deadline: %v)", len(tasks), s.cfg.ShutdownDrainTimeout)

	drainCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownDrainTimeout)
	defer cancel()

	s.drainTasks(drainCtx, tasks)
}

func (s *Scheduler) drainTasks(ctx context.Context, tasks []*task.Task) {
	for i, t := range tasks {
		select {
		case <-ctx.Done():
			remaining := tasks[i:]
			log.Printf("shutdown: drain deadline exceeded, releasing %d undispatched tasks", len(remaining))
			for _, rt := range remaining {
				if err := s.resetter.ResetTask(context.Background(), rt.ID, rt.Version); err != nil {
					log.Printf("shutdown: reset task %s: %v", rt.ID, err)
				}
			}
			return
		default:
		}

		if err := s.publishTask(ctx, t); err != nil {
			log.Printf("shutdown: publish task %s failed, releasing: %v", t.ID, err)
			if err := s.resetter.ResetTask(context.Background(), t.ID, t.Version); err != nil {
				log.Printf("shutdown: reset task %s: %v", t.ID, err)
			}
		}
	}

	log.Println("shutdown: drain complete")
}

func (s *Scheduler) DrainForTesting(resetter TaskResetter, tasks []*task.Task) {
	orig := s.resetter
	s.resetter = resetter
	defer func() { s.resetter = orig }()
	s.drainRemaining(context.Background(), tasks)
}
