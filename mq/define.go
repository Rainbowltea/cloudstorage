package mq

import (
	cmn "cloud/common"
)

// TransferData : 将要写到rabbitmq的数据的结构体
type TransferData struct {
	FileHash      string
	CurLocation   string
	DestLocation  string
	DestStoreType cmn.StoreType
}
