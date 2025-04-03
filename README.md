# FastDB

**FastDB** 是一款简单轻量的 `Go` 语言`kv`型数据库。它提供了一组易于使用的 API，允许用户在应用程序中存储和检索数据。

## 简介

**FastDB** 是一个快速且易于使用的基于`bitcask`的`kv`型数据库，旨在轻量和简单。使用 **FastDB**，您可以轻松地在 `Go` 应用程序中存储和检索数据。**FastDB** 优化了速度，这使得它非常适合需要快速数据访问的应用程序。

## 特点

**FastDB** 的一些特点包括：

- `易于使用`：`FastDB` 提供了一个简单直观的 API，使得存储和检索数据非常容易。
- `轻量`：`FastDB` 设计为轻量级和高效，这使得它非常适合在资源受限的环境中使用。
- `可靠`：`FastDB` 支持事务操作，确保在存储过程中不会丢失或损坏数据。
- `快速`：`FastDB` 使用内存数据结构，这使得它快速响应，特别适合需要快速读写速度的应用程序。
- `可扩展`：`FastDB` 提供了许多可配置选项，允许用户调整其性能和功能以适应不同的使用情况。

## 安装

您可以使用 Go 命令行工具安装 FastDB：

```go
go get github.com/qishenonly/FastDB
```

或者从github上clone本项目：

```bash
git clone https://github.com/qishenonly/FastDB
```

## 用法

以下是一个简单的使用示例：

```go
package main

import (
	"fmt"
	"github.com/qishenonly/FastDB"
)

func main() {
	options := fastdb.DefaultOptions
	options.DirPath = "/tmp/fastdb"
	db, err := fastdb.NewFastDB(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("fastdb-example"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("name value => ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```


