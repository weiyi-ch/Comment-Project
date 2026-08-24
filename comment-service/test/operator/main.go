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
	TutorA    = 1001 // 发帖助教
	StudentA  = 8001 // 发评学生
	OperatorA = 9001 // 审核运营
)

// TestCase 描述一条手工集成测试用例。
//
// ExpectFailure/ExpectMsg 用来表达“这条请求本来就应该被业务规则拦截”。
type TestCase struct {
	Name          string
	Method        string
	Path          string
	Body          string
	ExpectFailure bool
	ExpectMsg     string
}

// createResource 发送 POST 请求并解析返回的资源 ID，用于测试前置造数。
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

	// 兼容大小写和下划线
	var reply map[string]interface{}
	json.Unmarshal(bodyBytes, &reply)

	var idStr string
	if val, ok := reply[idKey]; ok {
		idStr = fmt.Sprintf("%v", val)
	} else if val, ok := reply[strings.ToLower(idKey[:1])+idKey[1:]]; ok {
		idStr = fmt.Sprintf("%v", val)
	} else {
		// 尝试转成 snake_case
		snakeKey := strings.ReplaceAll(idKey, "Id", "_id")
		if val, ok := reply[snakeKey]; ok {
			idStr = fmt.Sprintf("%v", val)
		}
	}

	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id, nil
}

// main 运行运营端审核链路的手工集成测试。
//
// 流程会先创建帖子和两条评论，再分别测试待审列表、审核详情、通过、驳回和重复审核。
func main() {
	baseURL := "http://127.0.0.1:8888"
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("🚀 开始【运营端】全链路审核业务与异常拦截测试...")
	fmt.Println(strings.Repeat("=", 70))

	// =========================================================================
	// 准备阶段：助教发帖 -> 学生发2条评论
	// =========================================================================
	fmt.Println("⏳ [前置准备] 造数据中 (1个帖子, 2条评论)...")

	// 1. 发帖
	postBody := fmt.Sprintf(`{"tutor_id": %d, "title": "【运营审核专用帖】", "content": "测试内容"}`, TutorA)
	postID, err := createResource(client, baseURL+"/v1/tutor/posts", postBody, "postId")
	if err != nil || postID == 0 {
		fmt.Printf("❌ 造数据失败(发帖): %v\n", err)
		return
	}

	// 2. 发评论1 (用于测试审核通过)
	c1Body := fmt.Sprintf(`{"student_id": %d, "content": "这条评论很优秀，请让我通过！"}`, StudentA)
	comment1ID, err := createResource(client, fmt.Sprintf("%s/v1/student/posts/%d/comments", baseURL, postID), c1Body, "commentId")

	// 3. 发评论2 (用于测试违规被驳回)
	c2Body := fmt.Sprintf(`{"student_id": %d, "content": "这条是垃圾广告加我V..."}`, StudentA)
	comment2ID, err := createResource(client, fmt.Sprintf("%s/v1/student/posts/%d/comments", baseURL, postID), c2Body, "commentId")

	if comment1ID == 0 || comment2ID == 0 {
		fmt.Println("❌ 造数据失败(发评)，无法进行后续测试。")
		return
	}

	fmt.Printf("   ✅ 数据准备完毕！PostID: %d | C1_ID: %d | C2_ID: %d\n", postID, comment1ID, comment2ID)
	fmt.Println(strings.Repeat("-", 70))
	time.Sleep(200 * time.Millisecond)

	// =========================================================================
	// 核心测试用例
	// =========================================================================
	tests := []TestCase{
		{
			Name:          "1. [正常] 运营查看待审核列表 (AuditStatus = 0)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/operator/comments/pending?operator_id=%d&audit_status=0&page_num=1&page_size=10", OperatorA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "2. [正常] 运营查看评论1的审核详情",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/operator/comments/%d/audit?operator_id=%d", comment1ID, OperatorA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "3. [正常] 运营【通过】评论1",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/operator/comments/%d/audit", comment1ID),
			Body:          fmt.Sprintf(`{"operator_id": %d, "comment_id": %d, "action": 1}`, OperatorA, comment1ID),
			ExpectFailure: false,
		},
		{
			Name:          "4. [规则拦截] 尝试重复审核评论1",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/operator/comments/%d/audit", comment1ID),
			Body:          fmt.Sprintf(`{"operator_id": %d, "comment_id": %d, "action": 1}`, OperatorA, comment1ID),
			ExpectFailure: true,
			ExpectMsg:     "重复", // 验证 Biz 层的拦截 (该评论已被审核，请勿重复操作)
		},
		{
			Name:          "5. [规则拦截] 尝试【驳回】评论2，但不填写原因",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/operator/comments/%d/audit", comment2ID),
			Body:          fmt.Sprintf(`{"operator_id": %d, "comment_id": %d, "action": 2}`, OperatorA, comment2ID),
			ExpectFailure: true,
			ExpectMsg:     "原因", // 验证 Biz 层的拦截 (驳回必须填写原因)
		},
		{
			Name:          "6. [正常] 运营填好原因后，成功【驳回】评论2",
			Method:        "POST",
			Path:          fmt.Sprintf("/v1/operator/comments/%d/audit", comment2ID),
			Body:          fmt.Sprintf(`{"operator_id": %d, "comment_id": %d, "action": 2, "manual_review_reason": "包含敏感引流广告"}`, OperatorA, comment2ID),
			ExpectFailure: false,
		},
		{
			Name:          "7. [正常] 运营查看已通过列表 (AuditStatus = 1)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/operator/comments/pending?operator_id=%d&audit_status=1&page_num=1&page_size=10", OperatorA),
			Body:          "",
			ExpectFailure: false,
		},
		{
			Name:          "8. [正常] 运营查看已驳回列表 (AuditStatus = 2)",
			Method:        "GET",
			Path:          fmt.Sprintf("/v1/operator/comments/pending?operator_id=%d&audit_status=2&page_num=1&page_size=10", OperatorA),
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
			if !isSuccess && strings.Contains(respBodyStr, tc.ExpectMsg) {
				fmt.Printf("   ✅ 业务规则防线生效！状态码: %d | 拦截信息: %s\n", resp.StatusCode, respBodyStr)
				successCount++
			} else if isSuccess {
				fmt.Printf("   ❌ 严重漏洞：违规操作居然成功了！响应: %s\n", respBodyStr)
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
	fmt.Println("🧹 [数据清理] 助教A 删除测试知识帖 (级联隐藏测试评论)...")
	reqDelPost, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/tutor/posts/%d?tutor_id=%d", baseURL, postID, TutorA), nil)
	client.Do(reqDelPost)

	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("🎉 运营端链路测试完成! 共计用例: %d 个 | 验证通过: %d | 失败/异常: %d\n", len(tests), successCount, failCount)
}
