package mr

func main() {
	c := make(chan bool, 1)
	<-c
	c <- true
}
