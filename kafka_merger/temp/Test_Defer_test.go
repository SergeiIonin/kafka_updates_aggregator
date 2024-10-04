package temp

import (
	"fmt"
	"testing"
	//"time"
)

func Test_Defer_test(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic: ", r)
		}
	}()

	//time.Sleep(10 * time.Second)

	panic("bad things")
}