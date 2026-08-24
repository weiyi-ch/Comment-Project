package main

// gorm gen configure.
//
// 该工具用于根据数据库表结构重新生成 dal/model 和 dal/query 代码。
// 生成代码属于机器产物，业务注释应写在手写代码中，避免下次生成被覆盖。

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"gorm.io/gen"
)

// connectDB 创建供 gorm/gen 读取表结构使用的数据库连接。
func connectDB(dsn string) *gorm.DB {
	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		panic(fmt.Errorf("connect db fail: %w", err))
	}
	return db
}

// main 配置 gorm/gen 并生成所有表的 model/query 代码。
func main() {
	// 指定生成代码的具体相对目录(相对当前文件)，默认为：./query
	// 默认生成需要使用WithContext之后才可以查询的代码，但可以通过设置gen.WithoutContext禁用该模式
	g := gen.NewGenerator(gen.Config{
		// 默认会在 OutPath 目录生成CRUD代码，并且同目录下生成 model 包
		// 所以OutPath最终package不能设置为model，在有数据库表同步的情况下会产生冲突
		// 若一定要使用可以通过ModelPkgPath单独指定model package的名称
		OutPath: "../../dal/query",
		/* ModelPkgPath: "dal/model"*/

		// gen.WithoutContext：禁用WithContext模式
		// gen.WithDefaultQuery：生成一个全局Query对象Q
		// gen.WithQueryInterface：生成Query接口
		Mode:          gen.WithDefaultQuery | gen.WithQueryInterface,
		FieldNullable: true,
	})

	// 通常复用项目中已有的SQL连接配置db(*gorm.DB)
	// 非必需，但如果需要复用连接时的gorm.Config或需要连接数据库同步表信息则必须设置
	g.UseDB(connectDB("root:root1234@tcp(127.0.0.1:3306)/comment?parseTime=True&loc=Local"))

	// 从连接的数据库为所有表生成Model结构体和CRUD代码
	// 也可以手动指定需要生成代码的数据表
	//g.ApplyBasic(g.GenerateModel("review_appeal_info"))
	//g.ApplyBasic(g.GenerateModel("review_info"))
	//g.ApplyBasic(g.GenerateModel("review_reply_info"))
	//自定义方法
	//g.ApplyInterface(func(model.Querier) {},g.GenerateModel(""))
	// 执行并生成代码
	// g.GenerateAllTable() 会返回所有数据库表的配置
	allTables := g.GenerateAllTable()

	// ApplyBasic 会为这些表生成对应的 Model 和基础查询代码
	g.ApplyBasic(allTables...)
	g.Execute()
}
