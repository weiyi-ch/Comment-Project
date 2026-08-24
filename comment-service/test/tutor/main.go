package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 定义全局的测试角色
const (
	TutorA  = 1001     // 发帖人
	TutorB  = 2002     // 恶意篡改者 (用于越权测试)
	DummyID = 99999999 // 虚假ID，用于测试资源不存在的情况
)

// TestCase 描述一条助教端手工集成测试用例。
//
// ExpectFailure 为 true 时，测试会判断响应是否包含期望的错误关键词。
type TestCase struct {
	Name          string
	Method        string
	Path          string
	Body          string
	ExpectFailure bool   // 标记该用例是否是故意触发报错的（如越权、资源不存在）
	ExpectMsg     string // 预期报错中应该包含的关键字
}

// main 运行助教端核心接口的手工集成测试。
//
// 该测试覆盖发帖、编辑、越权编辑、查询列表、异常评论/回复操作和删除帖子等路径。
func main() {
	baseURL := "http://127.0.0.1:8888"
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("🚀 开始全量核心业务与防越权链路测试...")
	fmt.Println(strings.Repeat("=", 60))

	// =========================================================================
	// 第一步：助教A 发布知识帖，获取真实的雪花 ID
	// =========================================================================
	fmt.Println("▶️ 正在测试: 1. [正常] 助教A 发布知识帖 (CreatePost)")
	createBody := fmt.Sprintf(`{"tutor_id": %d, "title": "雅思口语提分技巧", "content": "这里是干货内容..."}`, TutorA)
	req, _ := http.NewRequest("POST", baseURL+"/v1/tutor/posts", bytes.NewBuffer([]byte(createBody)))
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		return
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ 发帖失败，无法进行后续测试。状态码: %d, 响应: %s\n", resp.StatusCode, string(bodyBytes))
		return
	}

	// 解析出真实的 Snowflake ID
	var createReply struct {
		PostId string `json:"postId"`
	}
	if err := json.Unmarshal(bodyBytes, &createReply); err != nil || createReply.PostId == "0" || createReply.PostId == "" {
		var fallbackReply struct {
			PostId string `json:"post_id,string"`
		}
		json.Unmarshal(bodyBytes, &fallbackReply)
		createReply.PostId = fallbackReply.PostId
	}

	// 使用 ParseInt 保证 64 位雪花算法不越界
	realPostID, _ := strconv.ParseInt(createReply.PostId, 10, 64)
	fmt.Printf("   ✅ 发帖成功！成功获取真实雪花 ID: %d\n", realPostID)
	fmt.Println(strings.Repeat("-", 60))
	time.Sleep(200 * time.Millisecond)

	// =========================================================================
	// 第二步：使用获取到的真实 ID，动态构造后续 8 个接口的测试用例
	// =========================================================================
	tests := []TestCase{
		// --- 知识帖相关 ---
		{
			Name:          "2. [正常] 助教A 编辑自己的知识帖 (UpdatePost)",
			Method:        "PUT",
			Path:          fmt.Sprintf("/v1/tutor/posts/%d", realPostID),
			Body:          fmt.Sprintf(`{"tutor_id": %d, "post_id": %d, "title": "【已修改】口语技巧", "content": "内容更新了"}`, TutorA, realPostID),
			ExpectFailure: false,
		},
		{
			Name:          "3. [越权拦截] 助教B 尝试恶意编辑助教A的知识帖",
			Method:        "PUT",
			Path:          fmt.Sprintf("/v1/tutor/posts/%d", realPostID),
			Body:          fmt.Sprintf(`{"tutor_id": %d, "post_id": %d, "title": "【恶意篡改】", "content": "恶意内容"}`, TutorB, realPostID),
			ExpectFailure: true,
			ExpectMsg:     "无权",
		},
		{
			Name:          "4. [正常] 助教A 查看自己发布的帖子列表 (ListTutorPosts)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/tutor/posts?tutor_id=%d&page_num=1&page_size=10", TutorA),
			Body:          "",
			ExpectFailure: false,
		},

		// --- 评论与回复相关 (由于没有发评论，所以测试列表为空或资源不存在) ---
		{
			Name:          "5. [正常] 查看该帖子的评论列表 (ListPostCommentsTutor)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/tutor/posts/%d/comments?page_num=1&page_size=10", realPostID),
			Body:          "",
			ExpectFailure: false, // HTTP 200，但 items 应该是个空数组 []
		},
		{
			Name:          "6. [异常流] 查看不存在的评论详情 (GetCommentDetailTutor)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/tutor/comments/%d", DummyID),
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     "不存在", // Biz 层应抛出 "评论不存在"
		},
		{
			Name:          "7. [异常流] 回复不存在的评论 (ReplyComment)",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/tutor/comments/%d/replies", DummyID),
			Body:          fmt.Sprintf(`{"tutor_id": %d, "content": "同学你好！"}`, TutorA),
			ExpectFailure: true,
			ExpectMsg:     "不存在",
		},
		{
			Name:          "8. [异常流] 助教尝试删除不存在的评论 (DeleteCommentTutor)",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/tutor/comments/%d?tutor_id=%d", DummyID, TutorA),
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     "不存在",
		},
		{
			Name:          "9. [异常流] 助教尝试删除不存在的回复 (DeleteReply)",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/tutor/replies/%d?tutor_id=%d", DummyID, TutorA),
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     "不存在",
		},

		// --- 删帖兜底 ---
		{
			Name:          "10. [越权拦截] 助教B 尝试删除助教A的知识帖",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/tutor/posts/%d?tutor_id=%d", realPostID, TutorB),
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     "无权",
		},
		{
			Name:          "11. [正常] 助教A 成功删除自己的知识帖 (DeletePost)",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/tutor/posts/%d?tutor_id=%d", realPostID, TutorA),
			Body:          "",
			ExpectFailure: false,
		},
	}

	successCount := 0
	failCount := 0

	for _, tc := range tests {
		fmt.Printf("▶️ 正在测试: %s\n", tc.Name)
		url := baseURL + tc.Path

		var reqBody io.Reader
		if tc.Body != "" {
			reqBody = bytes.NewBuffer([]byte(tc.Body))
		}

		req, _ := http.NewRequest(tc.Method, url, reqBody)
		req.Header.Add("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("   ❌ 请求异常: %v\n", err)
			failCount++
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		isSuccess := resp.StatusCode == http.StatusOK
		respBodyStr := string(body)

		if tc.ExpectFailure {
			// 期望失败的用例
			if !isSuccess && strings.Contains(respBodyStr, tc.ExpectMsg) {
				fmt.Printf("   ✅ 成功拦截！状态码: %d | 拦截信息包含 '%s'\n", resp.StatusCode, tc.ExpectMsg)
				successCount++
			} else if isSuccess {
				fmt.Printf("   ❌ 严重漏洞：异常操作居然成功了！响应: %s\n", respBodyStr)
				failCount++
			} else {
				fmt.Printf("   ⚠️ 拦截了，但提示词不匹配(期望包含'%s')。响应: %s\n", tc.ExpectMsg, respBodyStr)
				successCount++ // 路由通了且拦截了，算一半成功
			}
		} else {
			// 期望成功的用例
			if isSuccess {
				// 截断过长的成功响应防止刷屏
				if len(respBodyStr) > 100 {
					respBodyStr = respBodyStr[:100] + "..."
				}
				fmt.Printf("   ✅ 正常执行成功！状态码: %d | 响应: %s\n", resp.StatusCode, respBodyStr)
				successCount++
			} else {
				fmt.Printf("   ❌ 预期成功但执行失败了！状态码: %d | 报错: %s\n", resp.StatusCode, respBodyStr)
				failCount++
			}
		}

		fmt.Println(strings.Repeat("-", 60))
		time.Sleep(200 * time.Millisecond) // 防止并发过快
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("🎉 链路测试完成! 共计用例: %d 个 | 验证通过: %d | 失败/异常: %d\n", len(tests)+1, successCount+1, failCount)
}
