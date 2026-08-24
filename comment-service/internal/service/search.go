package service

import (
	"context"

	pb "comment-service/api/comment/v1"
	"comment-service/internal/biz"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
)

// 搜索接口分页默认值和上限。
//
// service 层先兜底分页，避免非法 page_num/page_size 进入 ES 查询。
const (
	defaultSearchPageNum  int32 = 1
	defaultSearchPageSize int32 = 5
	maxSearchPageSize     int32 = 100
)

// SearchService 搜索服务。
//
// 该服务主要负责：
// 1. 接收 HTTP/gRPC 请求；
// 2. 做基础参数校验；
// 3. 调用 biz 层搜索用例；
// 4. 将 biz 实体组装成 proto DTO。
type SearchService struct {
	pb.UnimplementedSearchServiceServer
	uc  *biz.SearchUsecase
	log *log.Helper
}

// NewSearchService 创建搜索 RPC/HTTP 服务处理器。
func NewSearchService(uc *biz.SearchUsecase, logger log.Logger) *SearchService {
	return &SearchService{
		uc:  uc,
		log: log.NewHelper(log.With(logger, "module", "service/search")),
	}
}

// SearchStudentPosts 学生端搜索知识帖。
//
// 学生端只允许搜索已发布帖子，具体状态限制由 biz 层控制。
func (s *SearchService) SearchStudentPosts(ctx context.Context, req *pb.SearchStudentPostsRequest) (*pb.SearchPostsReply, error) {
	// service 层先修正分页，返回的 pageNum/pageSize 会继续传给 Biz/ES 查询。
	pageNum, pageSize := normalizeSearchPage(req.PageNum, req.PageSize)

	// Biz 层会设置“只查已发布帖子”的权限边界；返回搜索帖子列表和总数。
	posts, total, err := s.uc.SearchStudentPosts(ctx, req.Keyword, req.TutorId, pageNum, pageSize)
	if err != nil {
		s.log.WithContext(ctx).Errorf("SearchStudentPosts failed: %v", err)
		return nil, err
	}

	return &pb.SearchPostsReply{
		Total: total,
		Items: s.assemblePostDTOs(posts),
	}, nil
}

// SearchStudentComments 学生端搜索某知识帖下的评论。
//
// 学生搜索评论时必须限定 post_id，避免全站评论搜索导致权限边界不清晰。
func (s *SearchService) SearchStudentComments(ctx context.Context, req *pb.SearchStudentCommentsRequest) (*pb.SearchCommentsReply, error) {
	if req.PostId <= 0 {
		return nil, kerrors.BadRequest("POST_ID_REQUIRED", "请在对应帖子下搜索评论")
	}

	// 规范化分页后，Biz 会限制 post_id 和 visible_status，避免越权搜索。
	pageNum, pageSize := normalizeSearchPage(req.PageNum, req.PageSize)

	// 返回 CommentAgg 是为了以后扩展评论+回复等聚合信息；当前只包含评论本体。
	aggs, total, err := s.uc.SearchStudentComments(ctx, req.Keyword, req.PostId, pageNum, pageSize)
	if err != nil {
		s.log.WithContext(ctx).Errorf("SearchStudentComments failed: %v", err)
		return nil, err
	}

	return &pb.SearchCommentsReply{
		Total: total,
		Items: s.assembleCommentDTOs(aggs),
	}, nil
}

// SearchTutorPosts 助教端搜索自己的知识帖。
//
// 助教端由 biz 层限定 author_id，只能搜索自己的帖子。
func (s *SearchService) SearchTutorPosts(ctx context.Context, req *pb.SearchTutorPostsRequest) (*pb.SearchPostsReply, error) {
	// 助教搜索自己的帖子，可以查不同状态；分页仍在 service 层统一处理。
	pageNum, pageSize := normalizeSearchPage(req.PageNum, req.PageSize)

	// Biz 层把 tutor_id 写进 author_id 查询条件，返回帖子列表和总数。
	posts, total, err := s.uc.SearchTutorPosts(ctx, req.TutorId, req.Keyword, pageNum, pageSize)
	if err != nil {
		s.log.WithContext(ctx).Errorf("SearchTutorPosts failed: %v", err)
		return nil, err
	}

	return &pb.SearchPostsReply{
		Total: total,
		Items: s.assemblePostDTOs(posts),
	}, nil
}

// SearchTutorComments 助教端搜索某知识帖下的学生评论。
//
// 助教端搜索评论也必须限定 post_id。
func (s *SearchService) SearchTutorComments(ctx context.Context, req *pb.SearchTutorCommentsRequest) (*pb.SearchCommentsReply, error) {
	if req.PostId <= 0 {
		return nil, kerrors.BadRequest("POST_ID_REQUIRED", "请在对应帖子下搜索评论")
	}

	// 助教端评论搜索必须限定帖子，避免全站评论搜索带来权限问题。
	pageNum, pageSize := normalizeSearchPage(req.PageNum, req.PageSize)

	// Biz 层会设置只查可见评论；返回聚合评论和 total。
	aggs, total, err := s.uc.SearchTutorComments(ctx, req.PostId, req.Keyword, pageNum, pageSize)
	if err != nil {
		s.log.WithContext(ctx).Errorf("SearchTutorComments failed: %v", err)
		return nil, err
	}

	return &pb.SearchCommentsReply{
		Total: total,
		Items: s.assembleCommentDTOs(aggs),
	}, nil
}

// SearchOperatorComments 运营端审核工作台多维检索。
//
// 运营端可以按关键词、审核状态、审核员、时间范围检索评论。
// 注意：audit_status 如果不传，proto3 默认是 0，即默认查待审核。
// 如果需要查全部，前端应明确传 audit_status=-1。
func (s *SearchService) SearchOperatorComments(ctx context.Context, req *pb.SearchOperatorCommentsRequest) (*pb.SearchCommentsReply, error) {
	// 运营端支持多条件组合搜索，仍先统一处理分页边界。
	pageNum, pageSize := normalizeSearchPage(req.PageNum, req.PageSize)

	// 把 proto 请求转换为 Biz 查询参数；data 层会把这些字段翻译为 ES bool query。
	param := &biz.CommentSearchParam{
		Keyword:          req.Keyword,
		AuditStatus:      req.AuditStatus,
		ManualOperatorID: req.ManualOperatorId,
		StartTime:        req.StartTime,
		EndTime:          req.EndTime,
		PageNum:          pageNum,
		PageSize:         pageSize,
	}

	// 返回 CommentAgg 列表和总数；service 只负责 DTO 转换。
	aggs, total, err := s.uc.SearchOperatorComments(ctx, param)
	if err != nil {
		s.log.WithContext(ctx).Errorf("SearchOperatorComments failed: %v", err)
		return nil, err
	}

	return &pb.SearchCommentsReply{
		Total: total,
		Items: s.assembleCommentDTOs(aggs),
	}, nil
}

// normalizeSearchPage 统一处理 service 层分页参数。
//
// 原来的写法是 page_num 和 page_size 同时为 0 才设置默认值，
// 这会导致 page_num=0&page_size=10 这种请求穿透到 ES 后出现 from 为负数的问题。
func normalizeSearchPage(pageNum, pageSize int32) (int32, int32) {
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

// assemblePostDTOs 将 biz.Post 组装为 proto DTO。
//
// 返回值只包含可直接对外展示的帖子字段，不回查 MySQL。
func (s *SearchService) assemblePostDTOs(posts []*biz.Post) []*pb.PostDTO {
	items := make([]*pb.PostDTO, 0, len(posts))

	for _, p := range posts {
		if p == nil {
			continue
		}

		items = append(items, &pb.PostDTO{
			PostId:       p.PostID,
			AuthorId:     p.AuthorID,
			Title:        p.Title,
			Content:      p.Content,
			Status:       p.Status,
			LikeCount:    p.LikeCount,
			CommentCount: p.CommentCount,
			CreatedAt:    p.CreatedAt,
		})
	}

	return items
}

// assembleCommentDTOs 将评论聚合实体组装为 proto DTO。
//
// 返回值是搜索结果列表；如果 agg/comment 为空会跳过，避免空指针。
func (s *SearchService) assembleCommentDTOs(aggs []*biz.CommentAgg) []*pb.CommentDTO {
	items := make([]*pb.CommentDTO, 0, len(aggs))

	for _, agg := range aggs {
		if agg == nil || agg.Comment == nil {
			continue
		}

		items = append(items, commentDTOWithoutReplies(agg.Comment))
	}

	return items
}
