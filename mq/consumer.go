package mq

import "log"

var done chan bool

//循环监听（默认为poll模式）
//堵塞
func StartConsume(qName, cName string, callback func(msg []byte) bool) {
	//1通过channel.Consume获得信息通道
	msgs, err := channel.Consume(
		qName,
		cName,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		log.Println(err.Error())
		return
	}

	done = make(chan bool)
	//2.循环获取队列信息
	go func() {
		for msg := range msgs {
			//3.调用callback来处理新的消息
			processSuc := callback(msg.Body)
			if !processSuc {
				//TODO:将任务写到另一个队列，用于异常情况的重试
			}
		}
	}()
	//done没有新的消息过来，则会一直堵塞
	<-done
	//关闭通道
	channel.Close()
}
