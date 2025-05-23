package shardmaster

import (
	"fmt"
	"log"
	"reflect"
	"strings"
)

var StructDebug = 0

func StructDPrintf(format string, a ...interface{}) (n int, err error) {
	if StructDebug == 0 {
		return
	}

	// 检查是否存在结构体类型参数
	onlyStructs := true
	for _, arg := range a {
		if !isStructOrPtrToStruct(arg) {
			onlyStructs = false
			break
		}
	}

	// 如果全是结构体（或指针），则结构化打印
	if onlyStructs && len(a) > 0 {
		log.Println("[STRUCT DUMP MULTI]")
		for i, arg := range a {
			log.Printf("Arg[%d]:", i)
			printStruct(reflect.ValueOf(arg), 1)
		}
		log.Println("[END STRUCT DUMP MULTI]")
		return
	}

	// 混合打印（逐个处理）
	var printedAny bool
	for _, arg := range a {
		if isStructOrPtrToStruct(arg) {
			log.Println("[STRUCT ARG]")
			printStruct(reflect.ValueOf(arg), 1)
			printedAny = true
		}
	}
	if !printedAny {
		// 普通日志输出
		log.Printf(format, a...)
	}
	return
}

func isStructOrPtrToStruct(v interface{}) bool {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val.Kind() == reflect.Struct
}

func printStruct(val reflect.Value, indent int) {
	prefix := strings.Repeat("  ", indent)

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			fmt.Printf("%s<nil>\n", prefix)
			return
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Struct:
		t := val.Type()
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			fieldName := t.Field(i).Name
			if isSimple(field) {
				fmt.Printf("%s%s: %v\n", prefix, fieldName, field.Interface())
			} else {
				fmt.Printf("%s%s:\n", prefix, fieldName)
				printStruct(field, indent+1)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			fmt.Printf("%s[%d]:\n", prefix, i)
			printStruct(val.Index(i), indent+1)
		}
	case reflect.Map:
		for _, key := range val.MapKeys() {
			fmt.Printf("%s%v:\n", prefix, key.Interface())
			printStruct(val.MapIndex(key), indent+1)
		}
	default:
		fmt.Printf("%s%v\n", prefix, val.Interface())
	}
}

func isSimple(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Interface, reflect.Ptr:
		return false
	default:
		return true
	}
}
