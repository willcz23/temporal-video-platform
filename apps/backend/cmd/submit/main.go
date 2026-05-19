package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"video_demo/apps/backend/internal/config/envloader"

	"go.temporal.io/sdk/client"

	videoworkflow "video_demo/apps/backend/internal/workflow"
)

func main() {
	if _, err := envloader.Load(); err != nil {
		log.Println("No .env file found or error loading, continuing with OS environment")
	}
	hostPort := getenv("TEMPORAL_HOST_PORT", client.DefaultHostPort)
	namespace := getenv("TEMPORAL_NAMESPACE", client.DefaultNamespace)
	taskQueue := getenv("TEMPORAL_TASK_QUEUE", "video-gen-task-queue")

	var (
		userID    = flag.String("user-id", getenv("VIDEO_USER_ID", "demo-user"), "business user id")
		reqKey    = flag.String("req-key", getenv("VOLC_REQ_KEY", "jimeng_i2v_first_tail_v30_1080"), "Visual API req_key")
		first     = flag.String("first-frame", getenv("VIDEO_FIRST_FRAME_URL", ""), "first frame image URL (required)")
		tail      = flag.String("tail-frame", getenv("VIDEO_TAIL_FRAME_URL", ""), "tail frame image URL (required)")
		video_url = flag.String("video_demo", getenv("VIDEO_DEMO_URL", ""), "video_demo URL (required)")
		prompt    = flag.String("prompt", getenv("VIDEO_PROMPT", ""), "prompt text (required)")
		seed      = flag.Int("seed", getIntEnv("VIDEO_SEED", -1), "random seed, -1 means random")
		frames    = flag.Int("frames", getIntEnv("VIDEO_FRAMES", 121), "frame count: 121(5s) or 241(10s)")
		reqJSON   = flag.String("req-json", getenv("VIDEO_REQ_JSON", ""), "optional req_json for aigc_meta watermark")
	)

	flag.Parse()

	if *first == "" || *tail == "" || *prompt == "" {
		log.Fatal("required flags missing: -first-frame, -tail-frame, -prompt")
	}
	if *frames != 121 && *frames != 241 {
		log.Fatal("invalid -frames, only 121 or 241 are allowed")
	}

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
	if err != nil {
		log.Fatalf("failed to create Temporal client: %v", err)
	}
	defer c.Close()

	workflowID := fmt.Sprintf("video-gen-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, videoworkflow.VideoGenWorkflow, videoworkflow.VideoGenWorkflowInput{
		UserID:             *userID,
		ReqKey:             *reqKey,
		Prompt:             *prompt,
		FirstFrameImageURL: *first,
		TailFrameImageURL:  *tail,
		VideoURL:           *video_url,
		Seed:               *seed,
		Frames:             *frames,
		ReqJSON:            *reqJSON,
	})
	if err != nil {
		log.Fatalf("failed to start workflow: %v", err)
	}

	log.Printf("workflow started: workflow_id=%s run_id=%s", we.GetID(), we.GetRunID())
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var out int
	if _, err := fmt.Sscanf(v, "%d", &out); err != nil {
		return fallback
	}
	return out
}
