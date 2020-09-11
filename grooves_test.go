package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	groove "github.com/datomar-labs-inc/groove/common"
)

var r *groove.TaskSet

func TestGrooveMaster_Enqueue(t *testing.T) {
	g := New()

	eqwg := sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		eqwg.Add(1)

		go func(idx int) {
			var tasks []groove.Task

			for j := 0; j < 20; j++ {

				for k := 0; k < 5; k++ {
					tasks = append(tasks, groove.Task{
						ID:   fmt.Sprintf("memory.%d.%d.%d", idx, j, k),
						Data: nil,
					})
				}
			}

			g.Enqueue(tasks)
			eqwg.Done()
		}(i)
	}

	eqwg.Wait()

	wg := sync.WaitGroup{}

	var proccessed uint32

	for i := 0; i < 80; i++ {
		wg.Add(1)

		go func(consumerNum int) {
			defer wg.Done()

			for {
				dq := g.Dequeue(10, "memory", 10 * time.Second)

				if dq != nil {
					atomic.AddUint32(&proccessed, uint32(len(dq.Tasks)))

					err := g.Ack(dq.ID)
					if err != nil {
						fmt.Println("Consumer", consumerNum, "ERROR:", err.Error())
					}
				} else {
					fmt.Println("Consumer", consumerNum, "done")
					break
				}
			}
		}(i)
	}

	wg.Wait()

	fmt.Println("Processed", proccessed)
}

func TestGrooveMaster_EnqueueAndWait(t *testing.T) {
	g := New()

	eqwg := sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		eqwg.Add(1)

		go func(idx int) {
			var tasks []groove.Task

			for j := 0; j < 20; j++ {

				for k := 0; k < 5; k++ {
					tasks = append(tasks, groove.Task{
						ID:   fmt.Sprintf("memory.%d.%d.%d", idx, j, k),
						Data: nil,
					})
				}
			}

			waits := g.EnqueueAndWait(tasks)

			eqwg.Done()

			start := time.Now()

			for _, w := range waits {
				<- w
			}

			duration := time.Now().Sub(start)

			fmt.Printf("%d - Wait took %dms\n", idx, duration.Milliseconds())
		}(i)
	}

	eqwg.Wait()

	wg := sync.WaitGroup{}

	var proccessed uint32

	for i := 0; i < 80; i++ {
		wg.Add(1)

		go func(consumerNum int) {
			defer wg.Done()

			for {
				dq := g.Dequeue(10, "memory", 10 * time.Second)

				if dq != nil {
					atomic.AddUint32(&proccessed, uint32(len(dq.Tasks)))

					err := g.Ack(dq.ID)
					if err != nil {
						fmt.Println("Consumer", consumerNum, "ERROR:", err.Error())
					}
				} else {
					fmt.Println("Consumer", consumerNum, "done")
					break
				}
			}
		}(i)
	}

	wg.Wait()

	fmt.Println("Processed", proccessed)
}

func TestGrooveMaster_Dequeue(t *testing.T) {
	g := New()

	var proccessed uint32

	go func() {
		for {
			dq := g.Dequeue(10, "", 2*time.Second)

			if dq != nil {
				atomic.AddUint32(&proccessed, uint32(len(dq.Tasks)))
			}
		}
	}()

	waits := g.EnqueueAndWait([]groove.Task{
		{
			ID:         "test.task",
			Data:       nil,
		},
	})

	res := <- waits[0]

	if res != false {
		t.Error("expected wait 0 to be false")
		return
	}

	if proccessed != 2 {
		t.Error("expected task to be retried (processed == 2)")
		return
	}
}

func TestGrooveMaster_PutTask(t *testing.T) {
	g := New()

	task := groove.Task{
		ID:   "super.long.task.id.5",
		Data: nil,
	}

	g.putTask(task)

	g.Print()
}

func BenchmarkEnqueue10(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 10; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
	}
}

func BenchmarkEnqueue100(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 100; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
	}
}

func BenchmarkEnqueue100Deep(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 100; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
	}
}

func BenchmarkEnqueue100Parallel(b *testing.B) {
	g := New()

	var tasks [][]groove.Task

	for i := 0; i < 10; i++ {
		var t []groove.Task

		for j := 0; j < 10; j++ {
			t = append(t, groove.Task{
				ID: fmt.Sprintf("task.%d", i),
				Data: map[string]interface{}{
					"test": "data",
				},
			})
		}

		tasks = append(tasks, t)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}

		for _, t := range tasks {
			wg.Add(1)

			go func(taskList []groove.Task) {
				g.Enqueue(taskList)
				wg.Done()
			}(t)
		}

		wg.Wait()
	}
}

func BenchmarkEnqueue100DeepParallel(b *testing.B) {
	g := New()

	var tasks [][]groove.Task

	for i := 0; i < 10; i++ {
		var t []groove.Task

		for j := 0; j < 10; j++ {
			t = append(t, groove.Task{
				ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
				Data: map[string]interface{}{
					"test": "data",
				},
			})
		}

		tasks = append(tasks, t)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}

		for _, t := range tasks {
			wg.Add(1)

			go func(taskList []groove.Task) {
				g.Enqueue(taskList)
				wg.Done()
			}(t)
		}

		wg.Wait()
	}
}

func BenchmarkEnqueue1000(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 1000; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
	}
}

func BenchmarkEnqueue10000Parallel(b *testing.B) {
	g := New()

	var tasks [][]groove.Task

	for i := 0; i < 10; i++ {
		var t []groove.Task

		for j := 0; j < 1000; j++ {
			t = append(t, groove.Task{
				ID: fmt.Sprintf("task.%d", i),
				Data: map[string]interface{}{
					"test": "data",
				},
			})
		}

		tasks = append(tasks, t)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}

		for _, t := range tasks {
			wg.Add(1)

			go func(taskList []groove.Task) {
				g.Enqueue(taskList)
				wg.Done()
			}(t)
		}

		wg.Wait()
	}
}

func BenchmarkDequeue10(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 10; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
		r = g.Dequeue(10, "", 10 * time.Second)
	}
}

func BenchmarkDequeue250(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
		r = g.Dequeue(250, "", 10 * time.Second)
	}
}

func BenchmarkDequeue250Deep(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
		r = g.Dequeue(250, "", 10 * time.Second)
	}
}

func BenchmarkDequeue250DeepAck(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
		r = g.Dequeue(250, "", 10 * time.Second)
		_ = g.Ack(r.ID)
	}
}

func BenchmarkDequeue250Parallel(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i ++ {
			wg.Add(1)

			go func() {
				r = g.Dequeue(25, "", 10 * time.Second)
				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkDequeue250ParallelAck(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i ++ {
			wg.Add(1)

			go func() {
				r = g.Dequeue(25, "", 10 * time.Second)

				if r != nil {
					_ = g.Ack(r.ID)
				}
				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkDequeue250ParallelDeep(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i ++ {
			wg.Add(1)

			go func() {
				r = g.Dequeue(25, "", 10 * time.Second)
				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkDequeue250ParallelDeepAck(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 250; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d.%d.%d.%d", i, rand.Intn(10), rand.Intn(10), rand.Intn(10)),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i ++ {
			wg.Add(1)

			go func() {
				r = g.Dequeue(25, "", 10 * time.Second)
				_ = g.Ack(r.ID)
				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkDequeue1000(b *testing.B) {
	g := New()

	var tasks []groove.Task

	for i := 0; i < 1000; i++ {
		tasks = append(tasks, groove.Task{
			ID: fmt.Sprintf("task.%d", i),
			Data: map[string]interface{}{
				"test": "data",
			},
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		g.Enqueue(tasks)
		r = g.Dequeue(1000, "", 10 * time.Second)
	}
}
