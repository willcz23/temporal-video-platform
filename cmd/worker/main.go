package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	videoactivity "video_demo/internal/activity"
	videoworkflow "video_demo/internal/workflow"

	"github.com/joho/godotenv"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("未找到 .env 文件，将使用系统环境变量")
	}

	// fmt.Println("VIDEO_PROMPT", os.Getenv("VIDEO_PROMPT"))
	// fmt.Println("AK: main", os.Getenv("VOLCENGINE_ACCESS_KEY_ID"))
	// fmt.Println("SK:", os.Getenv("VOLCENGINE_SECRET_ACCESS_KEY"))

	hostPort := os.Getenv("TEMPORAL_HOST_PORT")
	namespace := os.Getenv("TEMPORAL_NAMESPACE")
	taskQueue := os.Getenv("TEMPORAL_TASK_QUEUE")

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
	if err != nil {
		log.Fatalf("failed to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(videoworkflow.VideoGenWorkflow)
	w.RegisterActivity(videoactivity.UploadToStorage)
	w.RegisterActivity(videoactivity.SubmitVideoGenTask)
	w.RegisterActivity(videoactivity.PollTaskStatus)
	w.RegisterActivity(videoactivity.NotifyUser)

	log.Printf("Temporal worker starting (host=%s namespace=%s task_queue=%s)", hostPort, namespace, taskQueue)

	if err := w.Start(); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down worker...")
	w.Stop()
	log.Println("worker stopped")
}
