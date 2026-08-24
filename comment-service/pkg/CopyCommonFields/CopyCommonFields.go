package cf

import (
	"reflect"
)

// CopyCommonFields 将 src 指向的结构体中与 dst 同名且同类型的字段复制过去。
//
// 该函数主要用于 service 层 DTO/model 简单字段映射。
// 对于 ID、时间戳或字段名不一致的场景，仍建议在调用处手动补齐，避免反射隐藏业务含义。
func CopyCommonFields(dst, src interface{}) {
	// 获取反射值对象
	dstVal := reflect.ValueOf(dst)
	srcVal := reflect.ValueOf(src)

	// 必须是指针且指向结构体
	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() || srcVal.Kind() != reflect.Ptr || srcVal.IsNil() {
		return
	}

	// 获取指针指向的实际结构体元素
	dstElem := dstVal.Elem()
	srcElem := srcVal.Elem()

	// 确保底层确实是结构体
	if dstElem.Kind() != reflect.Struct || srcElem.Kind() != reflect.Struct {
		return
	}

	srcType := srcElem.Type()

	// 遍历源结构体的每一个字段
	for i := 0; i < srcElem.NumField(); i++ {
		fieldName := srcType.Field(i).Name
		srcFieldValue := srcElem.Field(i)

		// 在目标结构体中寻找同名字段
		dstFieldValue := dstElem.FieldByName(fieldName)

		// 验证目标字段是否存在、是否可写、且类型是否一致
		if dstFieldValue.IsValid() && dstFieldValue.CanSet() {
			if dstFieldValue.Type() == srcFieldValue.Type() {
				dstFieldValue.Set(srcFieldValue)
			}
		}
	}
}
