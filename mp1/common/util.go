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
	line, err := bfRd.ReadBytes('\n')
	if err != nil {
		os.Exit(1)
	}
	strLine := string(line)[:len(string(line))-1]
	lineNum, err := strconv.Atoi(strLine)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "first line of cfg file should be an integer but received %v\n", strLine)
		os.Exit(1)
	}
	for i := 0; i < lineNum; i++ {
		line, err := bfRd.ReadBytes('\n')
		if err != nil && err != io.EOF {
			os.Exit(1)
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

func ParseTx(cmd string) (op, acct1, acct2 string, amt int64) {
	if cmd[len(cmd)-1] == '\n' {
		cmd = cmd[0 : len(cmd)-1]
	}
	args := strings.Split(cmd, " ")
	switch args[0] {
	case "DEPOSIT":
		op = "DEPOSIT"
		acct1 = args[1]
		amtInt, err := strconv.Atoi(args[2])
		if err != nil {
			panic(err)
		}
		amt = int64(amtInt)
	case "TRANSFER":
		op = "TRANSFER"
		acct1 = args[1]
		acct2 = args[3]
		amtInt, err := strconv.Atoi(args[4])
		if err != nil {
			panic(err)
		}
		amt = int64(amtInt)
	default:
		break
	}
	return
}
