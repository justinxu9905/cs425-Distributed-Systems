package storage

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

type TxStorage struct {
	Balance map[string]int64
	Lock    sync.Mutex
	LogFile *os.File
}

func NewTxStorage(logFilename string) *TxStorage {
	f, _ := os.Create(logFilename)
	return &TxStorage{
		Balance: make(map[string]int64, 0),
		Lock:    sync.Mutex{},
		LogFile: f,
	}
}

func (s *TxStorage) Deliver(op, acct1, acct2 string, amt int64) (res bool) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if amt <= 0 {
		return false
	}
	switch op {
	case "DEPOSIT":
		blc, _ := s.Balance[acct1]
		s.Balance[acct1] = blc + amt
		s.logBalance()
		return true
	case "TRANSFER":
		blc1, ok := s.Balance[acct1]
		if !ok || blc1 < amt {
			return false
		}
		blc2, _ := s.Balance[acct2]
		s.Balance[acct1] = blc1 - amt
		s.Balance[acct2] = blc2 + amt
		s.logBalance()
		return true
	default:
		break
	}
	return false
}

func (s *TxStorage) logBalance() {
	log := "BALANCES "
	var ks []string
	for k := range s.Balance {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		if s.Balance[k] == 0 {
			continue
		}
		log += fmt.Sprintf("%v: %v ", k, s.Balance[k])
	}
	log += "\n"
	fmt.Print(log)
	_, err := io.WriteString(s.LogFile, log)
	if err != nil {
		panic(err)
	}
}
