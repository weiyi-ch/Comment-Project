package data

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"comment-service/dal/model"
	"comment-service/internal/biz"
	"comment-service/internal/cachecontrol"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Elasticsearch 索引名常量。
//
// Canal 或同步任务写入 ES 时必须和这里的索引名保持一致。
const (
	// ES 索引名。
	postIndex         = "post"
	studyCommentIndex = "study_comment"
)

// Elasticsearch 字段名常量。
//
// 查询构造统一使用这些字段，避免同一字段在不同查询里手写出错。
const (
	// ES 字段名。
	fieldCreatedAt        = "created_at"
	fieldStatus           = "status"
	fieldAuthorID         = "author_id"
	fieldPostID           = "post_id"
	fieldVisibleStatus    = "visible_status"
	fieldAuditStatus      = "audit_status"
	fieldManualOperatorID = "manual_operator_id"
	fieldContent          = "content"
)

// 搜索缓存和预留 MySQL 缓存相关常量。
//
// 搜索结果组合条件多，评论搜索仍使用短 TTL 缓存；帖子搜索直接依赖 ES。
const (
	// ES 搜索结果缓存 key 前缀。
	//
	// 说明：
	// 1. 帖子搜索不做应用层整页缓存，避免长尾关键词低命中和失效困难；
	// 2. es:comment_search:{hash} 缓存评论搜索结果；
	// 3. hash 由搜索参数生成，避免 key 过长。
	esCommentSearchCachePrefix = "es:comment_search:"

	// MySQL 对象缓存 key 前缀。
	//
	// 当前文件主要实现 ES 搜索缓存。
	// mysql:* 相关 key 先预留，后续在按 ID 查 MySQL 的 repo 中使用。
	mysqlPostCachePrefix    = "mysql:post:"
	mysqlCommentCachePrefix = "mysql:comment:"
	mysqlReplyCachePrefix   = "mysql:comment_reply:"

	// Bloom Filter key。
	//
	// 注意：
	// Bloom Filter 用于按 ID 查 MySQL 的场景，不用于关键词搜索。
	bloomPostKey    = "bf:post"
	bloomCommentKey = "bf:comment"

	// ES 搜索结果缓存 TTL。
	//
	// 搜索结果受关键词、分页、状态、时间等条件影响，组合很多。
	// 不建议精准删除，使用短 TTL 自动过期即可。
	esSearchCacheTTL = 2 * time.Minute

	// MySQL 对象缓存 TTL。
	//
	// 当前文件暂不使用，后续按 ID 查 MySQL 时使用。
	mysqlDataCacheTTL = 10 * time.Minute
	nullCacheTTL      = 30 * time.Second
	nullCacheValue    = "__nil__"
)

// searchRepo 是搜索数据仓储。
//
// 当前职责：
// 1. 对外提供搜索能力；
// 2. 帖子搜索直接查 ES，评论搜索先查 Redis 短缓存；
// 3. 评论缓存未命中时通过 singleflight 合并相同请求；
// 4. 最终查询 ES，并把 ES 的 _source 转换为业务对象。
type searchRepo struct {
	data *Data
	log  *log.Helper

	// esGroup 用于防止 ES 缓存击穿。
	//
	// 同一个搜索条件缓存失效时，只允许一个 goroutine 真正查询 ES，
	// 其他 goroutine 等待并复用结果。
	esGroup singleflight.Group

	// mysqlGroup 预留给 MySQL 按 ID 查询。
	//
	// 当前 search.go 主要做 ES 搜索缓存，MySQL 缓存建议放在具体 post/comment repo 中实现。
	mysqlGroup singleflight.Group
}

// NewSearchRepo 创建搜索仓储。
//
// singleflight.Group 不需要从外部注入，直接作为 searchRepo 的内部状态即可。
// 这样 Wire 注入更简单。
func NewSearchRepo(data *Data, logger log.Logger) biz.SearchRepo {
	return &searchRepo{
		data: data,
		log:  log.NewHelper(log.With(logger, "module", "data/search")),
	}
}

// commentSearchCache 是评论搜索结果缓存结构。
type commentSearchCache struct {
	Items []*model.StudyComment `json:"items"`
	Total int64                 `json:"total"`
}

// postESDoc 对应 ES 中 post 索引的 _source。
//
// 注意：
// 1. ES 中只需要存搜索列表展示所需字段；
// 2. 字段名必须和 ES 文档字段保持一致；
// 3. 如果 Canal 同步字段名变化，这里的 json tag 也要同步改。
type postESDoc struct {
	PostID    int64       `json:"post_id"`
	AuthorID  int64       `json:"author_id"`
	Title     string      `json:"title"`
	Content   string      `json:"content"`
	Status    int32       `json:"status"`
	CreatedAt interface{} `json:"created_at"`
	UpdatedAt interface{} `json:"updated_at"`
}

// commentESDoc 对应 ES 中 study_comment 索引的 _source。
//
// 注意：
// 1. 这里直接从 ES doc 解析评论搜索列表；
// 2. 强一致操作，例如审核、删除、修改，仍然应该查 MySQL；
// 3. 如果 model.StudyComment 字段名和这里不一致，需要同步调整 convertCommentESDocToModel。
type commentESDoc struct {
	CommentID     int64  `json:"comment_id"`
	PostID        int64  `json:"post_id"`
	StudentID     int64  `json:"student_id"`
	Content       string `json:"content"`
	VisibleStatus int32  `json:"visible_status"`
	AuditStatus   int32  `json:"audit_status"`

	// 如果后续需要返回人工审核原因，可以打开该字段。
	// ManualReviewReason string `json:"manual_review_reason"`

	ManualOperatorID int64       `json:"manual_operator_id"`
	CreatedAt        interface{} `json:"created_at"`
	UpdatedAt        interface{} `json:"updated_at"`
}

// SearchPostsFromES 是帖子搜索入口。
//
// 调用链：
// 1. 直接查询 ES post 索引；
// 2. 从 ES _source 转换帖子列表；
// 3. 返回前补 post_counter 和 Redis pending/processing delta。
//
// 说明：
// 帖子关键词和分页组合分散，整页 Redis 缓存命中率低且很难精准失效。
// 这里让 ES 负责搜索层缓存与排序，应用层只补动态计数。
func (r *searchRepo) SearchPostsFromES(ctx context.Context, param *biz.PostSearchParam) ([]*biz.Post, int64, error) {
	if param == nil {
		param = &biz.PostSearchParam{}
	}

	items, total, err := r.searchPostsFromESNoCache(ctx, param)
	if err != nil {
		return nil, 0, err
	}
	if err := r.data.attachBizPostCounters(ctx, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// SearchCommentsFromES 是评论搜索入口。
//
// 调用链和 SearchPostsFromES 一致：
// Redis 缓存 → singleflight → ES → 写缓存 → 返回。
func (r *searchRepo) SearchCommentsFromES(ctx context.Context, param *biz.CommentSearchParam) ([]*model.StudyComment, int64, error) {
	if param == nil {
		param = &biz.CommentSearchParam{
			AuditStatus: biz.AuditStatusAll,
		}
	}

	// 评论搜索缓存 key 会纳入关键词、post_id、可见状态、审核状态、审核员和时间窗口。
	key := buildCommentSearchCacheKey(param)

	// 第一层读取 Redis 搜索结果缓存，命中后直接返回评论模型列表和 total。
	cache, ok := r.getCommentSearchCache(ctx, key)
	if ok {
		return cache.Items, cache.Total, nil
	}

	// 未命中时用 singleflight 合并同一查询条件的 ES 请求。
	val, err, _ := r.esGroup.Do(key, func() (interface{}, error) {
		// Double Check：
		// 防止多个相同请求等待 singleflight 时重复查询 ES。
		cache, ok := r.getCommentSearchCache(ctx, key)
		if ok {
			return cache, nil
		}

		items, total, err := r.searchCommentsFromESNoCache(ctx, param)
		if err != nil {
			return nil, err
		}

		// 写入短 TTL 搜索缓存；评论新增、删除、审核后的 ES 同步延迟由短 TTL 和 binlog 同步共同兜底。
		cache = &commentSearchCache{
			Items: items,
			Total: total,
		}

		if data, err := json.Marshal(cache); err == nil {
			if err := r.setCache(ctx, key, data, esSearchCacheTTL); err != nil {
				r.log.WithContext(ctx).Warnf("set es comment cache failed, key=%s, err=%v", key, err)
			}
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// 确认 singleflight 返回的是评论搜索缓存结构。
	cache, ok = val.(*commentSearchCache)
	if !ok {
		return nil, 0, fmt.Errorf("invalid comment search cache result")
	}

	return cache.Items, cache.Total, nil
}

// searchPostsFromESNoCache 真正查询 ES，不读写缓存。
//
// 注意：
// 1. 该方法只被 SearchPostsFromES 调用；
// 2. 不要在 usecase 层直接调用该方法；
// 3. 搜索列表直接从 ES _source 返回，不再回源 MySQL。
func (r *searchRepo) searchPostsFromESNoCache(ctx context.Context, param *biz.PostSearchParam) ([]*biz.Post, int64, error) {
	if param == nil {
		param = &biz.PostSearchParam{}
	}

	// bool query 用 filter 表达精确条件，用 must 表达关键词相关性查询。
	// filter 不参与打分，适合 status/author_id 这类权限边界。
	boolQuery := types.NewBoolQuery()

	// 按帖子状态过滤。
	// 学生端通常传 PostStatusPublished，只看已发布帖子。
	if param.Status > biz.PostStatusAll {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldStatus: {Value: param.Status},
			},
		})
	}

	// 按助教 ID 过滤。
	// 助教端搜索自己的帖子时会使用该条件。
	if param.AuthorID > 0 {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldAuthorID: {Value: param.AuthorID},
			},
		})
	}

	// 关键词搜索 title/content。
	// title 权重更高，所以使用 title^3。
	keyword := strings.TrimSpace(param.Keyword)
	if keyword != "" {
		tieBreaker := 0.3
		boolQuery.Must = append(boolQuery.Must, types.Query{
			MultiMatch: &types.MultiMatchQuery{
				Query:      keyword,
				Fields:     []string{"title^3", "content"},
				TieBreaker: (*types.Float64)(&tieBreaker),
			},
		})
	}

	req := r.data.es.Search().
		Index(postIndex).
		Query(&types.Query{Bool: boolQuery}).
		From(calcFrom(param.PageNum, param.PageSize)).
		Size(calcSize(param.PageSize)).
		ErrorTrace(true)

	// 构造 ES Search 请求后再根据是否有关键词决定排序策略。
	// 有关键词时让 ES 相关度主导；无关键词时用创建时间保证列表稳定。
	// 无关键词时，按发布时间倒序。
	// 有关键词时，保留 ES 默认相关度排序。
	if keyword == "" {
		order := sortorder.Desc
		req = req.Sort(types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				fieldCreatedAt: {
					Order: &order,
				},
			},
		})
	}

	resp, err := req.Do(ctx)
	if err != nil {
		r.log.WithContext(ctx).Errorf("search posts from es failed, param=%+v, err=%+v", param, err)
		return nil, 0, err
	}

	return extractPostsFromHits(&resp.Hits)
}

// searchCommentsFromESNoCache 真正查询 ES，不读写缓存。
//
// 注意：
// 1. 学生端/助教端普通列表可以直接使用 ES doc；
// 2. 运营端审核、删除、修改前仍建议查 MySQL 做强一致校验；
// 3. created_at 必须是 ES date 类型，否则 range/sort 会报 all shards failed。
func (r *searchRepo) searchCommentsFromESNoCache(ctx context.Context, param *biz.CommentSearchParam) ([]*model.StudyComment, int64, error) {
	if param == nil {
		param = &biz.CommentSearchParam{
			AuditStatus: biz.AuditStatusAll,
		}
	}

	// 评论搜索同样使用 bool query：
	// filter 表达角色权限和状态边界，must 表达内容关键词匹配。
	boolQuery := types.NewBoolQuery()

	// 学生端/助教端只看可见评论。
	if param.VisibleStatus == biz.VisibleStatusVisible {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldVisibleStatus: {Value: biz.VisibleStatusVisible},
			},
		})
	}

	// 限定某个知识帖下的评论。
	if param.PostID > 0 {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldPostID: {Value: param.PostID},
			},
		})
	}

	// 审核状态过滤。
	if param.AuditStatus != biz.AuditStatusAll {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldAuditStatus: {Value: param.AuditStatus},
			},
		})
	}

	// 人工审核员过滤，运营端使用。
	if param.ManualOperatorID > 0 {
		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Term: map[string]types.TermQuery{
				fieldManualOperatorID: {Value: param.ManualOperatorID},
			},
		})
	}

	// 评论内容关键词搜索。
	keyword := strings.TrimSpace(param.Keyword)
	if keyword != "" {
		boolQuery.Must = append(boolQuery.Must, types.Query{
			Match: map[string]types.MatchQuery{
				fieldContent: {Query: keyword},
			},
		})
	}

	// 评论创建时间范围过滤。
	//
	// 前提：
	// ES mapping 中 created_at 必须是 date 类型。
	if param.StartTime > 0 || param.EndTime > 0 {
		rangeQuery := types.DateRangeQuery{}

		if param.StartTime > 0 {
			v := formatESDateTime(param.StartTime)
			r.log.WithContext(ctx).Debugf("created_at gte raw=%d formatted=%s", param.StartTime, v)
			rangeQuery.Gte = &v
		}

		if param.EndTime > 0 {
			v := formatESDateTime(param.EndTime)
			r.log.WithContext(ctx).Debugf("created_at lte raw=%d formatted=%s", param.EndTime, v)
			rangeQuery.Lte = &v
		}

		boolQuery.Filter = append(boolQuery.Filter, types.Query{
			Range: map[string]types.RangeQuery{
				fieldCreatedAt: rangeQuery,
			},
		})
	}

	req := r.data.es.Search().
		Index(studyCommentIndex).
		Query(&types.Query{Bool: boolQuery}).
		From(calcFrom(param.PageNum, param.PageSize)).
		Size(calcSize(param.PageSize)).
		ErrorTrace(true)

	// 排序在请求构造后集中处理，避免每个 filter 分支里重复设置 sort。
	// 待审列表按时间升序是为了优先处理积压最久的评论。
	// 排序规则：
	// 1. 待审任务按时间升序，先积压先审核；
	// 2. 非关键词搜索按时间倒序；
	// 3. 有关键词时保留 ES 相关度排序。
	if param.AuditStatus == biz.AuditStatusPending {
		order := sortorder.Asc
		req = req.Sort(types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				fieldCreatedAt: {
					Order: &order,
				},
			},
		})
	} else if keyword == "" {
		order := sortorder.Desc
		req = req.Sort(types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				fieldCreatedAt: {
					Order: &order,
				},
			},
		})
	}

	resp, err := req.Do(ctx)
	if err != nil {
		r.log.WithContext(ctx).Errorf("search comments from es failed, param=%+v, err=%+v", param, err)
		return nil, 0, err
	}

	return extractCommentsFromHits(&resp.Hits)
}

// extractPostsFromHits 从 ES hits 中解析帖子列表。
func extractPostsFromHits(hits *types.HitsMetadata) ([]*biz.Post, int64, error) {
	total := hits.Total.Value
	posts := make([]*biz.Post, 0, len(hits.Hits))

	for _, hit := range hits.Hits {
		if len(hit.Source_) == 0 {
			return nil, total, fmt.Errorf("es post hit _source is empty")
		}

		var doc postESDoc
		if err := json.Unmarshal(hit.Source_, &doc); err != nil {
			return nil, total, fmt.Errorf("unmarshal post _source failed: %w", err)
		}

		// 兜底：
		// 如果 _source 中没有 post_id，则尝试从 ES _id 解析。
		if doc.PostID <= 0 && hit.Id_ != nil {
			id, err := parseHitID(*hit.Id_)
			if err != nil {
				return nil, total, err
			}
			doc.PostID = id
		}

		posts = append(posts, &biz.Post{
			PostID:    doc.PostID,
			AuthorID:  doc.AuthorID,
			Title:     doc.Title,
			Content:   doc.Content,
			Status:    doc.Status,
			CreatedAt: parseESTimeMilli(doc.CreatedAt),
		})
	}

	return posts, total, nil
}

// extractCommentsFromHits 从 ES hits 中解析评论列表。
func extractCommentsFromHits(hits *types.HitsMetadata) ([]*model.StudyComment, int64, error) {
	total := hits.Total.Value
	comments := make([]*model.StudyComment, 0, len(hits.Hits))

	for _, hit := range hits.Hits {
		if len(hit.Source_) == 0 {
			return nil, total, fmt.Errorf("es comment hit _source is empty")
		}

		var doc commentESDoc
		if err := json.Unmarshal(hit.Source_, &doc); err != nil {
			return nil, total, fmt.Errorf("unmarshal comment _source failed: %w", err)
		}

		// 兜底：
		// 如果 _source 中没有 comment_id，则尝试从 ES _id 解析。
		if doc.CommentID <= 0 && hit.Id_ != nil {
			id, err := parseHitID(*hit.Id_)
			if err != nil {
				return nil, total, err
			}
			doc.CommentID = id
		}

		comments = append(comments, convertCommentESDocToModel(doc))
	}

	return comments, total, nil
}

// convertCommentESDocToModel 将 ES comment doc 转为 dal/model.StudyComment。
func convertCommentESDocToModel(doc commentESDoc) *model.StudyComment {
	return &model.StudyComment{
		CommentID:     doc.CommentID,
		PostID:        doc.PostID,
		StudentID:     doc.StudentID,
		Content:       doc.Content,
		VisibleStatus: doc.VisibleStatus,
		AuditStatus:   doc.AuditStatus,

		// 如果 model 中 ManualReviewReason 是 *string，
		// 后续需要时可以单独处理 nil 和空字符串。
		// ManualReviewReason: &reason,

		ManualOperatorID: doc.ManualOperatorID,
		CreatedAt:        parseESTime(doc.CreatedAt),
		UpdatedAt:        parseESTime(doc.UpdatedAt),
	}
}

// getCommentSearchCache 读取评论搜索缓存。
func (r *searchRepo) getCommentSearchCache(ctx context.Context, key string) (*commentSearchCache, bool) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get es comment cache failed, key=%s, err=%v", key, err)
		return nil, false
	}

	if len(data) == 0 {
		return nil, false
	}

	var cache commentSearchCache
	if err := json.Unmarshal(data, &cache); err != nil {
		r.log.WithContext(ctx).Warnf("unmarshal es comment cache failed, key=%s, err=%v", key, err)

		// 缓存内容损坏时删除，避免反复解析失败。
		_ = r.delCache(ctx, key)
		return nil, false
	}

	return &cache, true
}

// commentSearchCacheParam 用于生成评论搜索缓存 key。
type commentSearchCacheParam struct {
	Keyword          string `json:"keyword"`
	PostID           int64  `json:"post_id"`
	VisibleStatus    int32  `json:"visible_status"`
	AuditStatus      int32  `json:"audit_status"`
	ManualOperatorID int64  `json:"manual_operator_id"`
	StartTime        int64  `json:"start_time"`
	EndTime          int64  `json:"end_time"`
	PageNum          int32  `json:"page_num"`
	PageSize         int32  `json:"page_size"`
}

// buildCommentSearchCacheKey 生成评论搜索缓存 key。
func buildCommentSearchCacheKey(param *biz.CommentSearchParam) string {
	p := commentSearchCacheParam{
		Keyword:          strings.TrimSpace(param.Keyword),
		PostID:           param.PostID,
		VisibleStatus:    param.VisibleStatus,
		AuditStatus:      param.AuditStatus,
		ManualOperatorID: param.ManualOperatorID,

		// 时间统一转成秒，避免同一个时间窗口因为秒/毫秒不同导致缓存 key 不一致。
		StartTime: normalizeUnixSecond(param.StartTime),
		EndTime:   normalizeUnixSecond(param.EndTime),

		PageNum:  normalizeCachePageNum(param.PageNum),
		PageSize: normalizeCachePageSize(param.PageSize),
	}

	return esCommentSearchCachePrefix + hashStruct(p)
}

// getCache 从 Redis 获取缓存。
func (r *searchRepo) getCache(ctx context.Context, key string) ([]byte, error) {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil, nil
	}

	data, err := r.data.cache.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return data, nil
}

// setCache 写入 Redis 缓存。
func (r *searchRepo) setCache(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Set(ctx, key, data, cacheTTLWithJitter(ttl)).Err()
}

// delCache 删除 Redis 缓存。
func (r *searchRepo) delCache(ctx context.Context, keys ...string) error {
	if r.data.cache == nil || len(keys) == 0 {
		return nil
	}

	return r.data.cache.Del(ctx, keys...).Err()
}

// calcFrom 计算 ES 分页起点。
func calcFrom(pageNum, pageSize int32) int {
	if pageNum <= 0 {
		pageNum = 1
	}

	if pageSize <= 0 {
		pageSize = 5
	}

	return int((pageNum - 1) * pageSize)
}

// calcSize 计算 ES 分页大小。
func calcSize(pageSize int32) int {
	if pageSize <= 0 {
		return 5
	}

	if pageSize > 100 {
		return 100
	}

	return int(pageSize)
}

// normalizeCachePageNum 规范化缓存 key 中的 page_num。
func normalizeCachePageNum(pageNum int32) int32 {
	if pageNum <= 0 {
		return 1
	}

	return pageNum
}

// normalizeCachePageSize 规范化缓存 key 中的 page_size。
func normalizeCachePageSize(pageSize int32) int32 {
	if pageSize <= 0 {
		return 5
	}

	if pageSize > 100 {
		return 100
	}

	return pageSize
}

// normalizeUnixSecond 兼容秒级和毫秒级时间戳。
//
// 秒级时间戳通常是 10 位，毫秒级时间戳通常是 13 位。
// 前端 JS 的 Date.getTime() 返回毫秒级时间戳。
func normalizeUnixSecond(ts int64) int64 {
	if ts > 9999999999 {
		return ts / 1000
	}

	return ts
}

// formatESDateTime 将秒级或毫秒级时间戳转换为 ES date 查询字符串。
//
// 前提：
// ES mapping 中 created_at 必须支持 yyyy-MM-dd HH:mm:ss。
func formatESDateTime(ts int64) string {
	sec := normalizeUnixSecond(ts)
	return time.Unix(sec, 0).Format("2006-01-02 15:04:05")
}

// parseESTimeMilli 将 ES _source 中的时间字段转为毫秒时间戳。
//
// 支持：
// 1. 秒级时间戳；
// 2. 毫秒级时间戳；
// 3. "2006-01-02 15:04:05"；
// 4. RFC3339 / RFC3339Nano。
func parseESTimeMilli(v interface{}) int64 {
	if v == nil {
		return 0
	}

	switch t := v.(type) {
	case float64:
		ts := int64(t)
		if ts > 9999999999 {
			return ts
		}
		return ts * 1000

	case int64:
		if t > 9999999999 {
			return t
		}
		return t * 1000

	case int:
		ts := int64(t)
		if ts > 9999999999 {
			return ts
		}
		return ts * 1000

	case json.Number:
		ts, err := t.Int64()
		if err != nil {
			return 0
		}
		if ts > 9999999999 {
			return ts
		}
		return ts * 1000

	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}

		// 兼容字符串形式的秒级 / 毫秒级时间戳。
		if ts, err := strconv.ParseInt(t, 10, 64); err == nil {
			if ts > 9999999999 {
				return ts
			}
			return ts * 1000
		}

		layouts := []string{
			"2006-01-02 15:04:05",
			time.RFC3339,
			time.RFC3339Nano,
		}

		for _, layout := range layouts {
			parsed, err := time.ParseInLocation(layout, t, time.Local)
			if err == nil {
				return parsed.UnixMilli()
			}
		}
	}

	return 0
}

// parseESTime 将 ES _source 中的时间字段转为 time.Time。
func parseESTime(v interface{}) time.Time {
	milli := parseESTimeMilli(v)
	if milli <= 0 {
		return time.Time{}
	}

	return time.UnixMilli(milli)
}

// parseHitID 将 ES hit._id 解析为业务 ID。
func parseHitID(hitID string) (int64, error) {
	id, err := strconv.ParseInt(hitID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid es hit _id=%q: %w", hitID, err)
	}

	return id, nil
}

// hashStruct 将结构体序列化后取 sha1，生成短缓存 key。
func hashStruct(v interface{}) string {
	data, _ := json.Marshal(v)
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}
