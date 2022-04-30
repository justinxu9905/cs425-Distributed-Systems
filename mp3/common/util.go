package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type NodeInfo struct {
	Addr string
	Port int
}

func ParseCfgFile(cfgFilePath string) map[string]NodeInfo {
	nodes := map[string]NodeInfo{}
	f, err := os.Open(cfgFilePath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cannot open file %v\n", cfgFilePath)
		os.Exit(1)
	}
	bfRd := bufio.NewReader(f)

	for {
		line, err := bfRd.ReadBytes('\n')
		if err != nil && err != io.EOF {
			os.Exit(1)
		}
		if len(string(line)) == 0 {
			break
		}
		var strLine string
		if string(line)[len(string(line))-1] == '\n' {
			strLine = string(line)[:len(string(line))-1]
		} else {
			strLine = string(line)
		}
		nodeInfo := strings.Split(strLine, " ")
		port, _ := strconv.Atoi(nodeInfo[2])
		nodes[nodeInfo[0]] = struct {
			Addr string
			Port int
		}{
			Addr: nodeInfo[1],
			Port: port,
		}
	}
	return nodes
}