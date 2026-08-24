package biz

import "github.com/google/wire"

// ProviderSet 声明 biz 层所有 usecase 的 wire 构造集合。
var ProviderSet = wire.NewSet(NewAuthUsecase, NewOperatorUsecase, NewTutorUsecase, NewStudentUsecase, NewSearchUsecase)
