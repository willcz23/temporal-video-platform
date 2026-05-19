package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"video_demo/apps/backend/internal/activity"
)

type VideoGenWorkflowInput struct {
	UserID             string
	ReqKey             string
	Prompt             string
	FirstFrameImageURL string
	TailFrameImageURL  string
	VideoURL           string
	Seed               int
	Frames             int
	ReqJSON            string
}

type VideoGenWorkflowResult struct {
	TaskID         string
	Status         string
	VideoURL       string
	AIGCMetaTagged bool
}

func VideoGenWorkflow(ctx workflow.Context, in VideoGenWorkflowInput) (VideoGenWorkflowResult, error) {

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    2 * time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    1 * time.Minute,
		MaximumAttempts:    3,
	}

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	var uploaded activity.UploadToStorageOutput
	if err := workflow.ExecuteActivity(ctx, activity.UploadToStorage, activity.UploadToStorageInput{
		FirstFrameImageURL: in.FirstFrameImageURL,
		TailFrameImageURL:  in.TailFrameImageURL,
		VideoURL:           in.VideoURL,
	}).Get(ctx, &uploaded); err != nil {
		return VideoGenWorkflowResult{}, err
	}

	var submitted_video activity.SubmitVideoAnalyzeOutput

	if err := workflow.ExecuteActivity(ctx, activity.SubmitVideoAnalyzeTask, activity.SubmitVideoAnalyzeInput{
		VideoDemoURL: in.VideoURL,
	}).Get(ctx, &submitted_video); err != nil {
		return VideoGenWorkflowResult{}, err
	}

	var submitted_image activity.SubmitVideoGenTaskOutput
	if err := workflow.ExecuteActivity(ctx, activity.SubmitVideoGenTask, activity.SubmitVideoGenTaskInput{
		ReqKey:             in.ReqKey,
		Prompt:             submitted_video.Prompt,
		FirstFrameImageURL: uploaded.FirstFrameImageURL,
		TailFrameImageURL:  uploaded.TailFrameImageURL,
		Seed:               in.Seed,
		Frames:             in.Frames,
	}).Get(ctx, &submitted_image); err != nil {
		return VideoGenWorkflowResult{}, err
	}

	var polled activity.PollTaskStatusOutput
	if err := workflow.ExecuteActivity(ctx, activity.PollTaskStatus, activity.PollTaskStatusInput{
		ReqKey:          in.ReqKey,
		TaskID:          submitted_image.TaskID,
		ReqJSON:         in.ReqJSON,
		MaxWait:         45 * time.Minute,
		InitialInterval: 5 * time.Second,
		MaxInterval:     60 * time.Second,
	}).Get(ctx, &polled); err != nil {
		return VideoGenWorkflowResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, activity.NotifyUser, activity.NotifyUserInput{
		UserID:   in.UserID,
		TaskID:   polled.TaskID,
		VideoURL: polled.VideoURL,
		Status:   polled.Status,
	}).Get(ctx, nil); err != nil {
		return VideoGenWorkflowResult{}, err
	}

	return VideoGenWorkflowResult{
		TaskID:   polled.TaskID,
		Status:   polled.Status,
		VideoURL: polled.VideoURL,
		//AIGCMetaTagged: polled.AIGCMetaTagged,
	}, nil
}
