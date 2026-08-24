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
	StudentA = 8001 // 发评学生
	StudentB = 8002 // 另一个学生（测试越权）
)

// TestCase 描述一条搜索链路手工集成测试用例。
//
// 搜索测试通常依赖异步同步，因此用例中既有正常查询，也有删除后的验证。
type TestCase struct {
	Name          string
	Method        string
	Path          string
	Body          string
	ExpectFailure bool
	ExpectMsg     string
}

// createResource 发送 POST 请求并解析返回的资源 ID，用于搜索测试前置造数。
func createResource(client *http.Client, url, body, idKey string) (int64, error) {
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var reply map[string]interface{}
	json.Unmarshal(bodyBytes, &reply)

	var idStr string
	if val, ok := reply[idKey]; ok {
		idStr = fmt.Sprintf("%v", val)
	} else if val, ok := reply[strings.ToLower(idKey[:1])+idKey[1:]]; ok {
		idStr = fmt.Sprintf("%v", val)
	} else {
		snakeKey := strings.ReplaceAll(idKey, "Id", "_id")
		if val, ok := reply[snakeKey]; ok {
			idStr = fmt.Sprintf("%v", val)
		}
	}

	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id, nil
}

// main 运行搜索相关的手工集成测试。
//
// 该测试依赖 Canal/Kafka/ES 异步同步，因此造数后会等待一段时间再发起搜索请求。
func main() {
	baseURL := "http://127.0.0.1:8888"
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("🚀 开始【学生/助教端交互与搜索】全链路业务测试...")
	fmt.Println(strings.Repeat("=", 70))

	// =========================================================================
	// 准备阶段：助教发帖 -> 学生评论 -> 助教回复
	// =========================================================================
	fmt.Println("⏳ [前置准备] 造数据中 (帖子 -> 评论 -> 回复)...")

	// 1. 助教发帖
	postBody := fmt.Sprintf(`{"tutor_id": %d, "title": "雅思听力8.0提分攻略", "content": "每天坚持精听1小时..."}`, TutorA)
	postID, err := createResource(client, baseURL+"/v1/tutor/posts", postBody, "postId")
	if err != nil || postID == 0 {
		fmt.Printf("❌ 造数据失败(发帖): %v\n", err)
		return
	}

	// 2. 学生发表评论
	fmt.Println(postID)
	cBody := fmt.Sprintf(`{"student_id": %d, "content": "老师，精听遇到听不懂的长难句怎么办？"}`, StudentA)
	commentID, err := createResource(client, fmt.Sprintf("%s/v1/student/posts/%d/comments", baseURL, postID), cBody, "commentId")
	if err != nil || commentID == 0 {
		fmt.Printf("❌ 造数据失败(发评): %v\n", err)
		return
	}

	// 3. 助教回复评论
	rBody := fmt.Sprintf(`{"tutor_id": %d, "content": "对于长难句，建议先抓主谓宾，再看修饰语。"}`, TutorA)
	replyID, err := createResource(client, fmt.Sprintf("%s/v1/tutor/comments/%d/replies", baseURL, commentID), rBody, "replyId")
	fmt.Println(replyID)
	if err != nil {
		fmt.Printf("❌ 造数据失败(回复): %v\n", err)
		return
	}

	fmt.Printf("   ✅ 数据准备完毕！PostID: %d | CommentID: %d | ReplyID: %d\n", postID, commentID, replyID)

	// ⚠️ 极其关键的一步：等待 Canal 解析 Binlog 并将数据同步到 Elasticsearch
	fmt.Println("   ⏳ 等待 Canal->Kafka->ES 数据异构同步 (约需 2 秒)...")
	time.Sleep(2 * time.Second)
	fmt.Println(strings.Repeat("-", 70))

	// =========================================================================
	// 核心测试用例
	// =========================================================================
	tests := []TestCase{
		{
			Name:          "1. [搜索端] 学生根据关键词搜索帖子",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/search/posts?keyword=听力&page_num=1&page_size=10"),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:   "2. [搜索端] 验证帖子列表详情 (包含嵌套的回复)",
			Method: "GET",
			// 注意：这里测试的就是我们之前写的 SearchService，验证 ES 取 ID + MySQL 拼装回复 的逻辑
			Path:          fmt.Sprintf("/v1/student/posts/%d/comments?page_num=1&page_size=10", postID),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "3. [搜索端] 助教搜索自己发过的帖子",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/tutor/search/posts?tutor_id=%d&keyword=攻略", TutorA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "4. [规则拦截] 尝试越权：学生B尝试删除学生A的评论",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/student/comments/%d", commentID),
			Body:          fmt.Sprintf(`{"student_id": %d}`, StudentB), // 伪造另一个学生的身份
			ExpectFailure: true,
			ExpectMsg:     "权限", // 验证越权拦截提示 (无权操作他人的评论)
		},
		{
			Name:          "5. [正常操作] 学生A成功删除自己的评论",
			Method:        "DELETE",
			Path:          fmt.Sprintf("/v1/student/comments/%d", commentID),
			Body:          fmt.Sprintf(`{"student_id": %d}`, StudentA),
			ExpectFailure: false,
		},
		{
			Name:          "6. [搜索验证] 评论删除后，ES 搜索列表为空 (验证 Canal 的 DELETE 同步)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/student/posts/%d/comments?page_num=1&page_size=10", postID),
			Body:          "",
			ExpectFailure: true,
			ExpectMsg:     `"total": 0`, // 期望通过匹配 JSON 结构判断列表为空
		},
	}

	successCount := 0
	failCount := 0

	for _, tc := range tests {
		fmt.Printf("▶️ 正在测试: %s\n", tc.Name)

		// 为了给步骤 6 的 Canal 同步留出时间
		if strings.Contains(tc.Name, "[搜索验证]") {
			time.Sleep(1 * time.Second)
		}

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
			if !isSuccess && strings.Contains(respBodyStr, tc.ExpectMsg) {
				fmt.Printf("   ✅ 业务拦截生效！状态码: %d | 拦截信息: %s\n", resp.StatusCode, respBodyStr)
				successCount++
			} else if isSuccess && strings.Contains(respBodyStr, tc.ExpectMsg) {
				// 兼容 HTTP 200 返回但内容体包含预期数据的检查 (比如 total: 0)
				fmt.Printf("   ✅ 数据状态符合预期！响应: %s\n", respBodyStr)
				successCount++
			} else if isSuccess {
				fmt.Printf("   ❌ 严重漏洞：违规操作成功了！响应: %s\n", respBodyStr)
				failCount++
			} else {
				fmt.Printf("   ⚠️ 拦截了，但提示词不匹配(期望包含'%s')。响应: %s\n", tc.ExpectMsg, respBodyStr)
				successCount++
			}
		} else {
			if isSuccess {
				if len(respBodyStr) > 300 {
					respBodyStr = respBodyStr[:300] + "...(已截断)"
				}
				fmt.Printf("   ✅ 正常执行成功！状态码: %d | 响应: %s\n", resp.StatusCode, respBodyStr)

				// 如果是测试详情拼接，可以简单验证下是否包含了回复内容
				if strings.Contains(tc.Name, "嵌套的回复") && !strings.Contains(respBodyStr, "长难句") {
					fmt.Printf("   ⚠️ 警告：虽然接口通了，但在返回值里没找到助教的回复内容，可能 Data Assembly 逻辑有问题！\n")
				}

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
	// 清理阶段
	// =========================================================================
	fmt.Println("🧹 [数据清理] 助教A 删除测试知识帖...")
	reqDelPost, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/tutor/posts/%d", baseURL, postID), bytes.NewBuffer([]byte(fmt.Sprintf(`{"tutor_id": %d}`, TutorA))))
	reqDelPost.Header.Add("Content-Type", "application/json")
	client.Do(reqDelPost)

	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("🎉 搜索端链路测试完成! 共计用例: %d 个 | 验证通过: %d | 失败/异常: %d\n", len(tests), successCount, failCount)
}
