package main

import (
	"bufio"
	"cloud/config"
	dblayer "cloud/db"
	"cloud/mq"
	"cloud/store/oss"
	"encoding/json"
	"log"
	"os"
)

//将本地暂时存储转移到oss
func processTransfer(msg []byte) bool {
	//1解析msg
	pubData := mq.TransferData{}
	err := json.Unmarshal(msg, pubData)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	//2根据临时存储文件路径，床架文件句柄
	filed, err := os.Open(pubData.CurLocation)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	//3通过文件句柄将文件内容读出来并且上传到oss
	err = oss.Bucket().PutObject(
		pubData.DestLocation,
		bufio.NewReader(filed),
	)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	//4跟新文件元信息（修改为oss上的存储路径）
	suc := dblayer.UpdateFileLocation(
		pubData.FileHash,
		pubData.DestLocation)
	if !suc {
		return false
	}
	return true
}

func main() {
	log.Println("开始监听转移任务队列")
	mq.StartConsume(
		config.TransOSSQueueName,
		"transfer_oss",
		processTransfer,
	)
}
