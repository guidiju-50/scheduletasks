// Exemplo com variável app partilhada: main cria o scheduler e app; outro ficheiro usa app.NotifySchedule().
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"scheduletasks/scheduler"
)

// App guarda o scheduler; outros ficheiros que recebam *App podem chamar NotifySchedule().
type App struct {
	Sched *scheduler.Scheduler
}

func (a *App) NotifySchedule() {
	a.Sched.NotifyScheduleChanged()
}

func main() {
	store := newStubStore()
	exec := newStubExecutor()
	s := scheduler.NewScheduler(store, exec)
	app := &App{Sched: s}

	ctx, cancel := context.WithCancel(context.Background())
	go app.Sched.Run(ctx)

	// Passa app para other.go (other.go guarda em appRef).
	SetApp(app)
	// Invoca a função que está em other.go: a chamada app.NotifySchedule() acontece dentro de other.go.
	NotifyScheduleNow()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	cancel()
	time.Sleep(200 * time.Millisecond)
	log.Print("exit")
}

// stub para exemplo (em produção use Db.Schedule e o seu executor).
type stubStore struct {
	tasks []scheduler.ScheduleTask
}

func newStubStore() *stubStore {
	return &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID: 1, StartAt: time.Now(), NextRunAt: time.Now().Add(1 * time.Hour),
			IntervalMinutes: 1, Enabled: true,
		}},
	}
}

func (s *stubStore) ReadEnabled(ctx context.Context) ([]scheduler.ScheduleTask, error) {
	return s.tasks, nil
}

func (s *stubStore) UpdateNextRun(ctx context.Context, id int, nextRun, lastRun time.Time) error {
	return nil
}

func (s *stubStore) Disable(ctx context.Context, id int, lastRun time.Time) error {
	return nil
}

type stubExecutor struct{}

func newStubExecutor() *stubExecutor { return &stubExecutor{} }

func (e *stubExecutor) Execute(ctx context.Context, t scheduler.ScheduleTask) error {
	log.Printf("executed task id=%d", t.ID)
	return nil
}
