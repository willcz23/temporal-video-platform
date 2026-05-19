package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
	"video_demo/apps/backend/internal/config/envloader"

	"github.com/volcengine/volc-sdk-golang/service/visual"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const (
	StatusInQueue    = "in_queue"
	StatusGenerating = "generating"
	StatusDone       = "done"
	StatusNotFound   = "not_found"
	StatusExpired    = "expired"
)

type UploadToStorageInput struct {
	FirstFrameImageURL string
	TailFrameImageURL  string
	VideoURL           string
}

type UploadToStorageOutput struct {
	FirstFrameImageURL string
	TailFrameImageURL  string
	VideoURL           string
}

type SubmitVideoAnalyzeInput struct {
	VideoDemoURL string
}

type SubmitVideoAnalyzeOutput struct {
	Prompt string
}

type SubmitVideoGenTaskInput struct {
	ReqKey             string
	Prompt             string
	FirstFrameImageURL string
	TailFrameImageURL  string
	Seed               int
	Frames             int
}

type SubmitVideoGenTaskOutput struct {
	TaskID    string
	RequestID string
}

type PollTaskStatusInput struct {
	ReqKey          string
	TaskID          string
	ReqJSON         string
	MaxWait         time.Duration
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

type PollTaskStatusOutput struct {
	TaskID   string
	Status   string
	VideoURL string
}

type NotifyUserInput struct {
	UserID   string
	TaskID   string
	VideoURL string
	Status   string
}

// UploadToStorage is a placeholder for MinIO/S3 upload. For now it passes through pre-signed URLs.
func UploadToStorage(ctx context.Context, in UploadToStorageInput) (UploadToStorageOutput, error) {
	if in.FirstFrameImageURL == "" || in.TailFrameImageURL == "" {
		return UploadToStorageOutput{}, errors.New("first frame image and tail frame image are required")
	}
	return UploadToStorageOutput{
		FirstFrameImageURL: in.FirstFrameImageURL,
		TailFrameImageURL:  in.TailFrameImageURL,
		VideoURL:           in.VideoURL,
	}, nil
}

func SubmitVideoAnalyzeTask(ctx context.Context, in SubmitVideoAnalyzeInput) (SubmitVideoAnalyzeOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("开始执行视频分析任务", "url", in.VideoDemoURL) // 👈 加一行这个
	if _, err := envloader.Load(); err != nil {
		log.Println("未找到 .env 文件，将使用系统环境变量")
	}

	apiKey := os.Getenv("ALIYUN_API_KEY")
	baseUrl := os.Getenv("ALIYUN_BASE_URL")
	video_prompt := "你是一个专业的视频内容分析助手。请根据提供的视频转录文本/画面描述/字幕，完成以下任务：1. 总结视频的核心内容（包括主题、主要观点、关键事实或步骤）。2. 将总结结果改写成一段\"可直接用于生成类似内容或描述视频用途\"的提示词。\n输出格式要求如下：\n---\n### 视频内容总结\n[用2-4句话概括视频主要内容]\n\n### 转化后的提示词（可直接复制使用）\n你是一个AI助手，请根据以下要求生成/理解内容：\n[在此处填入视频核心信息，以\"主题：xxx\"、\"关键点：1.xxx 2.xxx\"、\"风格/语气：xxx\"等形式组织，确保这段提示词能让另一个AI还原出相同主题和逻辑的内容]\n---\n\n请严格按照以上格式输出，不要添加额外解释。"

	video_demoURL := in.VideoDemoURL

	reqBody := map[string]interface{}{
		"model": "qwen3-vl-flash",
		"messages": []interface{}{
			map[string]interface{}{
				"role": "user",
				"content": []interface{}{
					map[string]interface{}{
						"type": "video_url",
						"video_url": map[string]interface{}{
							"url": video_demoURL,
						},
						"fps": 2,
					},
					map[string]interface{}{
						"type": "text",
						"text": video_prompt,
					},
				},
			},
		},
	}

	jsonData, err := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", baseUrl, bytes.NewBuffer(jsonData))

	req.Header.Set("Authorization", "Bearer"+" "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)

	if err != nil {
		// ❌ 千万不要用 log.Fatalf！要返回 error 让 Temporal 处理重试
		logger.Error("HTTP请求失败", "error", err)
		return SubmitVideoAnalyzeOutput{}, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("读取响应失败", "error", err)
		return SubmitVideoAnalyzeOutput{}, err
	}

	type AliyunResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`

		Usage struct {
			Total_tokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	var result AliyunResp

	if err := json.Unmarshal(body, &result); err != nil {
		log.Fatal("解析失败:", err)
	}

	logger.Info("视频理解成功", "content", result.Choices[0].Message.Content)
	return SubmitVideoAnalyzeOutput{Prompt: result.Choices[0].Message.Content}, nil
}

func SubmitVideoGenTask(ctx context.Context, in SubmitVideoGenTaskInput) (SubmitVideoGenTaskOutput, error) {
	logger := activity.GetLogger(ctx)

	if in.Prompt == "" {
		return SubmitVideoGenTaskOutput{}, temporal.NewNonRetryableApplicationError("prompt is required", "validation_error", nil)
	}

	frames := in.Frames
	if frames == 0 {
		frames = 121
	}
	if frames != 121 && frames != 241 {
		return SubmitVideoGenTaskOutput{}, temporal.NewNonRetryableApplicationError("frames must be 121 or 241", "validation_error", nil)
	}

	if _, err := envloader.Load(); err != nil {
		log.Println("未找到 .env 文件，将使用系统环境变量")
	}

	testAk := os.Getenv("VOLCENGINE_ACCESS_KEY_ID")
	testSk := os.Getenv("VOLCENGINE_SECRET_ACCESS_KEY")

	visual.DefaultInstance.Client.SetAccessKey(testAk)
	visual.DefaultInstance.Client.SetSecretKey(testSk)
	visual.DefaultInstance.SetRegion("cn-north-1")
	visual.DefaultInstance.SetSchema("https") // 协议
	visual.DefaultInstance.SetHost("visual.volcengineapi.com")

	reqBody := map[string]interface{}{
		"req_key": "jimeng_i2v_first_tail_v30_1080",
		"image_urls": []string{
			in.FirstFrameImageURL,
			in.TailFrameImageURL,
		},
		"prompt": in.Prompt,
		"seed":   in.Seed,
		"frames": in.Frames,
	}

	resp, status, err := visual.DefaultInstance.CVSync2AsyncSubmitTask(reqBody)

	fmt.Println(status, err)
	b, _ := json.Marshal(resp)

	fmt.Println(string(b))

	if status != 200 {
		fmt.Println("request err : ", err)
		return SubmitVideoGenTaskOutput{}, err
	}

	type VolcResp struct {
		Code      int    `json:"code"`
		RequestID string `json:"request_id"`
		Message   string `json:"message"`
		Data      struct {
			TaskID string `json:"task_id"`
		} `json:"data"`
	}
	var respData VolcResp
	if err = json.Unmarshal(b, &respData); err != nil {
		fmt.Println("unmarshal err:", err)
		return SubmitVideoGenTaskOutput{}, err
	}

	if respData.Code != 10000 {
		err := fmt.Errorf("wrong: %s", respData.Message)
		return SubmitVideoGenTaskOutput{}, err
	}

	taskID := respData.Data.TaskID
	reqID := respData.RequestID
	logger.Info("video task submitted", "task_id", taskID, "request_id", reqID)
	return SubmitVideoGenTaskOutput{TaskID: taskID, RequestID: reqID}, nil
}

func PollTaskStatus(ctx context.Context, in PollTaskStatusInput) (PollTaskStatusOutput, error) {
	logger := activity.GetLogger(ctx)

	if _, err := envloader.Load(); err != nil {
		log.Println("未找到 .env 文件，将使用系统环境变量")
	}

	testAk := os.Getenv("VOLCENGINE_ACCESS_KEY_ID")
	testSk := os.Getenv("VOLCENGINE_SECRET_ACCESS_KEY")

	visual.DefaultInstance.Client.SetAccessKey(testAk)
	visual.DefaultInstance.Client.SetSecretKey(testSk)

	wait := in.MaxWait
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	interval := in.InitialInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	maxInterval := in.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 60 * time.Second
	}

	deadline := time.Now().Add(wait)

	for {
		reqBody := map[string]interface{}{
			"req_key":  "jimeng_i2v_first_tail_v30_1080",
			"task_id":  in.TaskID,
			"req_json": "{\"aigc_meta\": {\"content_producer\": \"001191440300192203821610000\", \"producer_id\": \"producer_id_test123\", \"content_propagator\": \"001191440300192203821610000\", \"propagate_id\": \"propagate_id_test123\"}}",
		}
		resp, status, err := visual.DefaultInstance.CVSync2AsyncGetResult(reqBody)

		if err != nil {
			logger.Error("查询任务状态失败", "err", err)
			return PollTaskStatusOutput{}, err
		}

		if status != 200 {
			return PollTaskStatusOutput{}, fmt.Errorf("http status error: %d", status)
		}

		b, _ := json.Marshal(resp)

		type VolcGetResp struct {
			Code int `json:"code"`
			Data struct {
				AigcMetaTagged bool   `json:"aigc_meta_tagged"`
				Status         string `json:"status"`
				VideoURL       string `json:"video_url"`
			} `json:"data"`
			Message   string `json:"message"`
			RequsetID string `json:"request_id"`
		}

		var respData VolcGetResp
		if err := json.Unmarshal(b, &respData); err != nil {
			return PollTaskStatusOutput{}, fmt.Errorf("解析失败: %v", err)
		}

		if respData.Code != 10000 {
			return PollTaskStatusOutput{}, fmt.Errorf("api wrong: %d", respData.Code)
		}

		out := PollTaskStatusOutput{
			TaskID:   in.TaskID,
			Status:   respData.Data.Status,
			VideoURL: respData.Data.VideoURL,
		}

		switch respData.Data.Status {
		case "done":
			logger.Info("Completed", "task_id", in.TaskID, "video_url", respData.Data.VideoURL)
			return out, nil

		case "in_queue", "generating":
			logger.Info("waiting", "task_id", in.TaskID, "status", respData.Data.Status)

		case "not_found", "expired":
			logger.Info("wrong", "task_id", in.TaskID, "status", respData.Data.Status)
		}

		if time.Now().After(deadline) {
			return out, fmt.Errorf("time out")
		}

		if err := sleepWithContext(ctx, interval); err != nil {
			return out, nil
		}

		interval = interval * 2
		if interval > maxInterval {
			interval = maxInterval
		}
	}
}

func NotifyUser(ctx context.Context, in NotifyUserInput) error {
	logger := activity.GetLogger(ctx)
	logger.Info("video generation notification", "user_id", in.UserID, "task_id", in.TaskID, "status", in.Status, "video_url", in.VideoURL)
	// TODO: Integrate with message bus/webhook/email service.
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
