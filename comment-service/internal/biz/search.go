package biz

import (
	"context"
	"strings"

	"comment-service/dal/model"

	"github.com/go-kratos/kratos/v2/log"
)

// 搜索分页默认值和上限。
//
// usecase 层再次规范化分页，保证即使绕过 service 调用也不会把异常分页传到 data 层。
const (
	// 默认分页参数。
	defaultSearchPageNum  int32 = 1
	defaultSearchPageSize int32 = 5

	// 搜索接口最大 page_size，防止一次查询过大。
	maxSearchPageSize int32 = 100
)

// 帖子搜索状态边界。
//
// 不同角色通过这些常量表达“查全部”或“只查已发布”。
const (
	// PostStatusAll 表示不限制帖子状态。
	// 助教端查看自己帖子时可以使用。
	PostStatusAll int32 = 0

	// PostStatusPublished 表示已发布帖子。
	// 学生端只能搜索已发布帖子。
	PostStatusPublished int32 = 1
)

// 评论可见状态搜索边界。
//
// 学生/助教通常只看可见评论，运营端可以不限制可见性。
const (
	// VisibleStatusAll 表示不限制评论可见状态。
	// 运营端审核、回溯时使用。
	VisibleStatusAll int32 = 0

	// VisibleStatusVisible 表示只查询可见评论。
	// 学生端、助教端默认只能看可见评论。
	VisibleStatusVisible int32 = 1
)

// 评论审核状态搜索边界。
//
// 这些值会进入 ES filter，也会用于运营审核列表筛选。
const (
	// AuditStatusAll 表示不限制审核状态。
	AuditStatusAll int32 = -1

	// AuditStatusPending 表示待审核。
	AuditStatusPending int32 = 0

	// AuditStatusApproved 表示审核通过。
	AuditStatusApproved int32 = 1

	// AuditStatusRejected 表示审核驳回。
	AuditStatusRejected int32 = 2
)

// Post 是搜索场景下的帖子业务实体。
//
// 设计说明：
// 1. 搜索列表页数据直接来自 ES doc；
// 2. 详情页、删除、修改、审核等强一致场景仍然查 MySQL；
// 3. 不直接返回 dal/model.Post，避免 service 层依赖数据库模型。
type Post struct {
	PostID       int64
	AuthorID     int64
	Title        string
	Content      string
	Status       int32
	LikeCount    int32
	CommentCount int32

	// CreatedAt 统一使用毫秒时间戳，方便 service 层和前端处理。
	CreatedAt int64
}

// CommentAgg 是评论聚合实体。
//
// 当前搜索评论结果直接来自 ES doc。
// 目前只包装评论本体；如果后续需要展示助教回复，有两种方案：
// 1. 将回复冗余到 ES comment doc 中；
// 2. 根据 comment_id 额外查询 reply 表。
type CommentAgg struct {
	Comment *model.StudyComment
}

// PostSearchParam 是帖子搜索参数。
//
// 使用场景：
// 1. 学生端搜索已发布知识帖；
// 2. 助教端搜索自己发布的知识帖。
type PostSearchParam struct {
	// Keyword 用于匹配 title/content。
	Keyword string

	// AuthorID 用于限定助教。
	// 学生端如果不限定助教，可以传 0。
	AuthorID int64

	// Status 为帖子状态。
	// 0 表示不限制，1 表示只查已发布。
	Status int32

	PageNum  int32
	PageSize int32
}

// CommentSearchParam 是评论搜索参数。
//
// 学生端、助教端、运营端复用该结构。
// 不同端的权限边界由 SearchUsecase 设置。
type CommentSearchParam struct {
	// Keyword 用于匹配评论内容。
	Keyword string

	// PostID 限定某个知识帖下的评论。
	PostID int64

	// VisibleStatus 评论可见状态。
	// 0 不限制，1 只查询可见评论。
	VisibleStatus int32

	// AuditStatus 审核状态。
	// -1 不限制，0 待审，1 通过，2 驳回。
	AuditStatus int32

	// ManualOperatorID 人工审核员 ID。
	// 运营端按审核员筛选时使用。
	ManualOperatorID int64

	// StartTime / EndTime 支持秒级或毫秒级时间戳。
	// data 层负责兼容并转换成 ES 可查询的时间格式。
	StartTime int64
	EndTime   int64

	PageNum  int32
	PageSize int32
}

// SearchRepo 定义搜索用例依赖的数据访问能力。
//
// 设计说明：
// 1. biz 层只依赖接口，不关心 data 层是否走缓存；
// 2. 是否使用 Redis、singleflight、ES 查询细节都由 data 层决定；
// 3. 搜索列表页直接返回 ES doc 转换后的业务对象，不再返回 ID 后回源 MySQL。
type SearchRepo interface {
	// SearchPostsFromES 从 ES 检索帖子，并直接从 _source 返回帖子列表。
	SearchPostsFromES(ctx context.Context, param *PostSearchParam) ([]*Post, int64, error)

	// SearchCommentsFromES 从 ES 检索评论，并直接从 _source 返回评论列表。
	SearchCommentsFromES(ctx context.Context, param *CommentSearchParam) ([]*model.StudyComment, int64, error)
}

// SearchUsecase 搜索业务用例。
//
// 职责边界：
// 1. 设置不同端的搜索权限边界；
// 2. 规范化搜索参数；
// 3. 调用 SearchRepo 获取搜索结果；
// 4. 对评论结果做轻量聚合包装。
//
// 注意：
// SearchUsecase 不直接操作 ES、Redis、MySQL。
type SearchUsecase struct {
	repo SearchRepo
	log  *log.Helper
}

// NewSearchUsecase 创建搜索业务用例。
//
// 返回的 usecase 只依赖 SearchRepo 接口，方便 data 层替换缓存/ES 实现。
func NewSearchUsecase(repo SearchRepo, logger log.Logger) *SearchUsecase {
	return &SearchUsecase{
		repo: repo,
		log:  log.NewHelper(log.With(logger, "module", "usecase/search")),
	}
}

// SearchStudentPosts 学生端搜索知识帖。
//
// 规则：
// 1. 学生端只能搜索已发布帖子；
// 2. tutorID > 0 时表示限定某个助教；
// 3. tutorID = 0 时表示不限定助教。
func (uc *SearchUsecase) SearchStudentPosts(
	ctx context.Context,
	keyword string,
	tutorID int64,
	pageNum, pageSize int32,
) ([]*Post, int64, error) {
	// 构造帖子搜索参数；Status=Published 用来限制学生只看已发布帖子。
	param := &PostSearchParam{
		Keyword:  keyword,
		AuthorID: tutorID,
		Status:   PostStatusPublished,
		PageNum:  pageNum,
		PageSize: pageSize,
	}

	// fetchPosts 会规范化参数并调用 repo；返回帖子列表和 ES total。
	return uc.fetchPosts(ctx, param)
}

// SearchTutorPosts 助教端搜索自己的知识帖。
//
// 规则：
// 1. 必须限定 author_id 为当前助教 ID；
// 2. 不限制帖子状态，方便助教查看已发布、草稿或其他状态的帖子。
func (uc *SearchUsecase) SearchTutorPosts(
	ctx context.Context,
	tutorID int64,
	keyword string,
	pageNum, pageSize int32,
) ([]*Post, int64, error) {
	// 构造助教搜索参数；AuthorID 用于限定当前助教，StatusAll 表示不限制帖子状态。
	param := &PostSearchParam{
		Keyword:  keyword,
		AuthorID: tutorID,
		Status:   PostStatusAll,
		PageNum:  pageNum,
		PageSize: pageSize,
	}

	// fetchPosts 统一处理 trim、分页默认值、repo 调用和空结果。
	return uc.fetchPosts(ctx, param)
}

// SearchStudentComments 学生端搜索某帖子下的评论。
//
// 规则：
// 1. 必须限定 post_id；
// 2. 学生端只能看到可见评论；
// 3. 不限制审核状态，由 visible_status 控制最终可见性。
func (uc *SearchUsecase) SearchStudentComments(
	ctx context.Context,
	keyword string,
	postID int64,
	pageNum, pageSize int32,
) ([]*CommentAgg, int64, error) {
	// 学生只能搜索某个帖子下可见评论，不允许跨帖子全局搜索。
	param := &CommentSearchParam{
		Keyword:       keyword,
		PostID:        postID,
		VisibleStatus: VisibleStatusVisible,
		AuditStatus:   AuditStatusAll,
		PageNum:       pageNum,
		PageSize:      pageSize,
	}

	// fetchCommentAggs 返回评论聚合列表和 ES total。
	return uc.fetchCommentAggs(ctx, param)
}

// SearchTutorComments 助教端搜索某帖子下的学生评论。
//
// 规则：
// 1. 默认只查看可见评论；
// 2. 当前方法只负责搜索，不做“该帖子是否属于当前助教”的权限校验；
// 3. 帖子归属校验建议在 service 或调用前的业务流程中完成。
func (uc *SearchUsecase) SearchTutorComments(
	ctx context.Context,
	postID int64,
	keyword string,
	pageNum, pageSize int32,
) ([]*CommentAgg, int64, error) {
	// 助教当前搜索某帖子下可见评论；帖子归属校验由上游业务保证。
	param := &CommentSearchParam{
		Keyword:       keyword,
		PostID:        postID,
		VisibleStatus: VisibleStatusVisible,
		AuditStatus:   AuditStatusAll,
		PageNum:       pageNum,
		PageSize:      pageSize,
	}

	// fetchCommentAggs 负责规范化参数并调用 repo.SearchCommentsFromES。
	return uc.fetchCommentAggs(ctx, param)
}

// SearchOperatorComments 运营端审核工作台多维检索。
//
// 规则：
// 1. 运营端用于审核、回溯、排查；
// 2. 不强制 visible_status=1；
// 3. 可按 audit_status、manual_operator_id、时间范围、关键词等条件检索。
func (uc *SearchUsecase) SearchOperatorComments(
	ctx context.Context,
	param *CommentSearchParam,
) ([]*CommentAgg, int64, error) {
	if param == nil {
		// 参数为空时默认不限制审核状态，避免空指针并给调用方一个宽松查询。
		param = &CommentSearchParam{
			AuditStatus: AuditStatusAll,
		}
	}

	// 运营端不限制可见状态。
	param.VisibleStatus = VisibleStatusAll

	// 返回评论聚合和总数；data 层负责 ES bool query 和搜索缓存。
	return uc.fetchCommentAggs(ctx, param)
}

// fetchPosts 是帖子搜索通用流程。
//
// 当前链路：
// usecase 设置搜索边界
//
//	↓
//
// 参数规范化
//
//	↓
//
// repo.SearchPostsFromES
//
//	↓
//
// data 层决定是否查缓存、是否查 ES
//
//	↓
//
// 返回帖子列表
//
// 不再执行：
// ES 查 post_id → MySQL 查帖子详情。
func (uc *SearchUsecase) fetchPosts(
	ctx context.Context,
	param *PostSearchParam,
) ([]*Post, int64, error) {
	// normalizePostSearchParam 会 trim keyword，并修正分页默认值/上限。
	normalizePostSearchParam(param)

	// repo.SearchPostsFromES 返回 ES _source 转换后的帖子列表和 total。
	posts, total, err := uc.repo.SearchPostsFromES(ctx, param)
	if err != nil {
		uc.log.WithContext(ctx).Errorf("search posts from es failed: %v", err)
		return nil, 0, err
	}

	if len(posts) == 0 {
		// 返回空切片而不是 nil，service 层组装 DTO 时更稳定。
		return []*Post{}, total, nil
	}

	return posts, total, nil
}

// fetchCommentAggs 是评论搜索通用流程。
//
// 当前链路：
// usecase 设置搜索边界
//
//	↓
//
// 参数规范化
//
//	↓
//
// repo.SearchCommentsFromES
//
//	↓
//
// data 层决定是否查缓存、是否查 ES
//
//	↓
//
// ES comment doc 转 model.StudyComment
//
//	↓
//
// 包装成 CommentAgg 返回
//
// 不再执行：
// ES 查 comment_id → MySQL 查评论详情。
func (uc *SearchUsecase) fetchCommentAggs(
	ctx context.Context,
	param *CommentSearchParam,
) ([]*CommentAgg, int64, error) {
	// normalizeCommentSearchParam 会 trim keyword，并修正分页默认值/上限。
	normalizeCommentSearchParam(param)

	// repo.SearchCommentsFromES 返回评论模型列表和 total；data 层决定是否命中缓存。
	comments, total, err := uc.repo.SearchCommentsFromES(ctx, param)
	if err != nil {
		uc.log.WithContext(ctx).Errorf("search comments from es failed: %v", err)
		return nil, 0, err
	}

	if len(comments) == 0 {
		// 返回空切片而不是 nil，避免 service 层遍历时做额外 nil 判断。
		return []*CommentAgg{}, total, nil
	}

	aggs := make([]*CommentAgg, 0, len(comments))
	for _, comment := range comments {
		if comment == nil {
			continue
		}

		// CommentAgg 当前只包一层 comment，保留扩展回复/帖子摘要的空间。
		aggs = append(aggs, &CommentAgg{
			Comment: comment,
		})
	}

	return aggs, total, nil
}

// normalizePostSearchParam 规范化帖子搜索参数。
//
// 主要处理：
// 1. keyword 去掉首尾空格；
// 2. page_num/page_size 设置默认值和上限。
func normalizePostSearchParam(param *PostSearchParam) {
	if param == nil {
		return
	}

	param.Keyword = strings.TrimSpace(param.Keyword)
	param.PageNum, param.PageSize = normalizePage(param.PageNum, param.PageSize)
}

// normalizeCommentSearchParam 规范化评论搜索参数。
//
// 主要处理：
// 1. keyword 去掉首尾空格；
// 2. page_num/page_size 设置默认值和上限。
func normalizeCommentSearchParam(param *CommentSearchParam) {
	if param == nil {
		return
	}

	param.Keyword = strings.TrimSpace(param.Keyword)
	param.PageNum, param.PageSize = normalizePage(param.PageNum, param.PageSize)
}

// normalizePage 统一分页参数。
//
// 注意：
// Go 是值传递，所以这里必须返回规范化后的 pageNum/pageSize，
// 调用方再赋值回结构体字段。
func normalizePage(pageNum, pageSize int32) (int32, int32) {
	if pageNum <= 0 {
		pageNum = defaultSearchPageNum
	}

	if pageSize <= 0 {
		pageSize = defaultSearchPageSize
	}

	if pageSize > maxSearchPageSize {
		pageSize = maxSearchPageSize
	}

	return pageNum, pageSize
}
