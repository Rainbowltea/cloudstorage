package handler

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		data, err := ioutil.ReadFile("static/view/index.html")
		if err != nil {
			io.WriteString(w, "internel server error")
			return
		}
		io.WriteString(w, string(data))
	} else if r.Method == "POST" {
		//接受文件流及存储到本地目录
		file, head, err := r.FormFile("file")
		if err != nil {
			fmt.Printf("Failed to get data, err:%s\n", err.Error())
			return
		}
		defer file.Close()
		//创建本地文件来接受文件流
		newFile, err := os.Create("tmp/" + head.Filename)
		if err != nil {
			fmt.Print("Failed to create file,err:%s\n", err.Error())
			return
		}
		defer newFile.Close()
		//将内存中的文件拷贝到新文件的buffer区中
		_, err = io.Copy(newFile, file)
		if err != nil {
			fmt.Printf("Failed to save data into file,err:%s\n", err.Error())
			return
		}
		http.Redirect(w, r, "/file/upload/suc", http.StatusFound)
	}
}

func UploadSuccHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Up finished!")
}
