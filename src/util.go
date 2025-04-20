package src

import "log"

const flag = 0

func DPrint(message string, args []interface{}) (n int, err error) {
	if flag == 1 {
		log.Printf(message, args)
	}
	return
}
