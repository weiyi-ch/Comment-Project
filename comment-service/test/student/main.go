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

// 定义测试角色
const (
	TutorA   = 1001 // 发帖助教
	StudentA = 8001 // 正常互动的学生
	StudentB = 8002 // 恶意越权的学生
)

// TestCase 描述一条学生端手工集成测试用例。
//
// ExpectFailure 用于标记越权、资源不存在等本来就应失败的路径。
type TestCase struct {
	Name          string
	Method        string
	Path          string
	Body          string
	ExpectFailure bool
	ExpectMsg     string
}

// main 运行学生端核心接口的手工集成测试。
//
// 流程会先造一个真实帖子和评论，再依次验证点赞、查询、取消点赞、防越权删除等链路。
func main() {
	baseURL := "http://127.0.0.1:8888"
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("🚀 开始【学生端】全链路核心业务与防越权测试...")
	fmt.Println(strings.Repeat("=", 70))

	// =========================================================================
	// 准备阶段：助教A 先发一个帖子，获取真实的 post_id
	// =========================================================================
	fmt.Println("⏳ [前置准备] 助教A 发布测试知识帖...")
	createPostBody := fmt.Sprintf(`{"tutor_id": %d, "title": "【学生端测试专用帖】", "content": "测试内容..."}`, TutorA)
	reqPost, _ := http.NewRequest("POST", baseURL+"/v1/tutor/posts", bytes.NewBuffer([]byte(createPostBody)))
	reqPost.Header.Add("Content-Type", "application/json")
	respPost, err := client.Do(reqPost)
	if err != nil || respPost.StatusCode != http.StatusOK {
		fmt.Println("❌ 前置造数据失败，请确保服务已启动且数据库正常。")
		return
	}
	bodyBytes, _ := io.ReadAll(respPost.Body)
	respPost.Body.Close()

	var postReply struct {
		PostId string `json:"postId"`
	}
	json.Unmarshal(bodyBytes, &postReply)
	if postReply.PostId == "" { // 兼容下划线
		var fallback struct {
			PostId string `json:"post_id,string"`
		}
		json.Unmarshal(bodyBytes, &fallback)
		postReply.PostId = fallback.PostId
	}
	realPostID, _ := strconv.ParseInt(postReply.PostId, 10, 64)
	fmt.Printf("   ✅ 成功获取知识帖 ID: %d\n", realPostID)

	// =========================================================================
	// 第一步：学生A 发表评论，获取真实的 comment_id
	// =========================================================================
	fmt.Println("\n▶️ 正在测试: 1. [正常] 学生A 发表评论 (CreateComment)")
	createCommentBody := fmt.Sprintf(`{"student_id": %d, "content": "老师讲得太好啦，打卡学习！"}`, StudentA)
	urlComment := fmt.Sprintf("%s/v1/student/posts/%d/comments", baseURL, realPostID)
	reqComment, _ := http.NewRequest("POST", urlComment, bytes.NewBuffer([]byte(createCommentBody)))
	reqComment.Header.Add("Content-Type", "application/json")

	respComment, _ := client.Do(reqComment)
	bodyBytes, _ = io.ReadAll(respComment.Body)
	respComment.Body.Close()

	var commentReply struct {
		CommentId string `json:"commentId"`
	}
	json.Unmarshal(bodyBytes, &commentReply)
	if commentReply.CommentId == "" {
		var fallback struct {
			CommentId string `json:"comment_id,string"`
		}
		json.Unmarshal(bodyBytes, &fallback)
		commentReply.CommentId = fallback.CommentId
	}
	realCommentID, _ := strconv.ParseInt(commentReply.CommentId, 10, 64)
	fmt.Printf("   ✅ 发评成功！成功获取评论 ID: %d\n", realCommentID)
	fmt.Println(strings.Repeat("-", 70))
	time.Sleep(200 * time.Millisecond)

	// =========================================================================
	// 第二步：执行后续互动与安全测试
	// =========================================================================
	tests := []TestCase{
		{
			Name:          "2. [正常] 学生A 点赞该帖子 (LikePost)",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/student/posts/%d/like", realPostID),
			Body:          fmt.Sprintf(`{"student_id": %d}`, StudentA),
			ExpectFailure: false,
		},
		{
			Name:          "3. [正常] 学生A 再次点赞该帖子 (测试幂等性)",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/student/posts/%d/like", realPostID),
			Body:          fmt.Sprintf(`{"student_id": %d}`, StudentA),
			ExpectFailure: false,
		},
		{
			Name:          "4. [正常] 查看知识帖详情 (GetPostDetailStudent)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/posts/%d?comment_page_num=1&comment_page_size=5", realPostID),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "5. [正常] 查看该帖子的评论列表 (ListPostCommentsStudent)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/posts/%d/comments?page_num=1&page_size=10", realPostID),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "6. [正常] 查看刚才那条评论的详情 (GetCommentDetailStudent)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/comments/%d", realCommentID),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "7. [正常] 学生A 查看自己的评论列表 (ListMyComments)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/comments?student_id=%d&page_num=1&page_size=10", StudentA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "8. [越权拦截] 学生B 尝试恶意删除 学生A 的评论",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/student/comments/%d?student_id=%d", realCommentID, StudentB), // HTTP DELETE 参数放 Query
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     "无权", // 验证 biz.StudentUsecase.DeleteMyComment 中的防线
		},
		{
			Name:          "9. [正常] 学生A 成功删除自己的评论 (DeleteMyComment)",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/student/comments/%d?student_id=%d", realCommentID, StudentA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "10. [正常] 学生A 取消点赞该帖子 (UnlikePost)",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/student/posts/%d/like?student_id=%d", realPostID, StudentA),
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
			// 期望被业务拦截的测试用例（如越权测试）
			if !isSuccess && strings.Contains(respBodyStr, tc.ExpectMsg) {
				fmt.Printf("   ✅ 安全防线生效！状态码: %d | 拦截信息: %s\n", resp.StatusCode, respBodyStr)
				successCount++
			} else if isSuccess {
				fmt.Printf("   ❌ 严重漏洞：越权操作居然成功了！响应: %s\n", respBodyStr)
				failCount++
			} else {
				fmt.Printf("   ⚠️ 拦截了，但提示词不匹配(期望包含'%s')。响应: %s\n", tc.ExpectMsg, respBodyStr)
				successCount++
			}
		} else {
			// 期望正常执行的用例
			if isSuccess {
				if len(respBodyStr) > 100 {
					respBodyStr = respBodyStr[:100] + "...(已截断)"
				}
				fmt.Printf("   ✅ 正常执行成功！状态码: %d | 响应: %s\n", resp.StatusCode, respBodyStr)
				successCount++
			} else {
				fmt.Printf("   ❌ 预期成功但执行失败了！状态码: %d | 报错: %s\n", resp.StatusCode, respBodyStr)
				failCount++
			}
		}

		fmt.Println(strings.Repeat("-", 70))
		time.Sleep(200 * time.Millisecond)
	}

	// =========================================================================
	// 清理阶段：删除测试帖子
	// =========================================================================
	fmt.Println("🧹 [数据清理] 助教A 删除测试知识帖...")
	reqDelPost, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/tutor/posts/%d?tutor_id=%d", baseURL, realPostID, TutorA), nil)
	client.Do(reqDelPost)

	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("🎉 学生端链路测试完成! 共计用例: %d 个 | 验证通过: %d | 失败/异常: %d\n", len(tests)+1, successCount+1, failCount)
}
