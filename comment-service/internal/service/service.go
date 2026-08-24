package service

import "github.com/google/wire"

// ProviderSet 声明 service 层所有 gRPC/HTTP handler 的 wire 构造集合。
var ProviderSet = wire.NewSet(NewOperatorService, NewStudentService, NewTutorService, NewSearchService)
