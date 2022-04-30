package transaction

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestTransaction(t *testing.T) {
	mgr := NewTransactionMgr()
	var tx1 []Operation

	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  20,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "B",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  20,
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "A",
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "B",
	})

	mgr.Transaction(tx1)
}

func TestConsistency(t *testing.T) {
	mgr := NewTransactionMgr()
	var tx1 []Operation

	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  20,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "B",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "WITHDRAW",
		Acct: "A",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "A",
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "B",
	})

	if mgr.Transaction(tx1) {
		t.Fail()
	}
}

func TestAbort(t *testing.T) {
	mgr := NewTransactionMgr()
	var tx1 []Operation

	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  20,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "B",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "WITHDRAW",
		Acct: "A",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "A",
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "B",
	})

	mgr.Transaction(tx1)
}

func TestConcurrency2Tx(t *testing.T) {
	mgr := NewTransactionMgr()
	var tx1, tx2 []Operation

	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  20,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "B",
		Amt:  30,
	})
	tx1 = append(tx1, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  40,
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "A",
	})
	tx1 = append(tx1, Operation{
		Op:   "BALANCE",
		Acct: "B",
	})


	tx2 = append(tx2, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  100,
	})
	tx2 = append(tx2, Operation{
		Op:   "DEPOSIT",
		Acct: "B",
		Amt:  200,
	})
	tx2 = append(tx2, Operation{
		Op:   "DEPOSIT",
		Acct: "A",
		Amt:  300,
	})
	tx2 = append(tx2, Operation{
		Op:   "BALANCE",
		Acct: "A",
	})
	tx2 = append(tx2, Operation{
		Op:   "BALANCE",
		Acct: "B",
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		ok := mgr.Transaction(tx1) // A +60 B + 30
		for !ok {
			ok = mgr.Transaction(tx1)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx2) // A +400 B +200
		for !ok {
			ok = mgr.Transaction(tx2)
		}
		wg.Done()
	}()
	wg.Wait()
	if mgr.perObjectState["A"].committedValue != 460 || mgr.perObjectState["B"].committedValue != 230 {
		t.Fail()
	}
}

func TestConcurrency3Tx(t *testing.T) {
	mgr := NewTransactionMgr()
	tx1, exp1 := generateValidTransaction(100)
	tx2, exp2 := generateValidTransaction(200)
	tx3, exp3 := generateValidTransaction(500)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		ok := mgr.Transaction(tx1)
		for !ok {
			ok = mgr.Transaction(tx1)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx2)
		for !ok {
			ok = mgr.Transaction(tx2)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx3)
		for !ok {
			ok = mgr.Transaction(tx3)
		}
		wg.Done()
	}()
	wg.Wait()

	fmt.Println(exp1)
	fmt.Println(exp2)
	fmt.Println(exp3)

	flag := true
	for k := range exp1 {
		if mgr.perObjectState[k].committedValue != exp1[k] + exp2[k] + exp3[k] {
			flag = false
			fmt.Printf("diff on %v exp: %v, committed val: %v\n", k, exp1[k] + exp2[k] + exp3[k], mgr.perObjectState[k].committedValue)
		}
	}
	if !flag {
		t.Fail()
	}
}

func TestConcurrency10Tx(t *testing.T) {
	mgr := NewTransactionMgr()
	tx1, exp1 := generateValidTransaction(10000)
	tx2, exp2 := generateValidTransaction(20000)
	tx3, exp3 := generateValidTransaction(50000)
	tx4, exp4 := generateValidTransaction(10000)
	tx5, exp5 := generateValidTransaction(20000)
	tx6, exp6 := generateValidTransaction(50000)
	tx7, exp7 := generateValidTransaction(10000)
	tx8, exp8 := generateValidTransaction(20000)
	tx9, exp9 := generateValidTransaction(50000)
	tx10, exp10 := generateValidTransaction(10000)

	var wg sync.WaitGroup
	wg.Add(10)
	go func() {
		ok := mgr.Transaction(tx1)
		for !ok {
			ok = mgr.Transaction(tx1)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx2)
		for !ok {
			ok = mgr.Transaction(tx2)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx3)
		for !ok {
			ok = mgr.Transaction(tx3)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx4)
		for !ok {
			ok = mgr.Transaction(tx4)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx5)
		for !ok {
			ok = mgr.Transaction(tx5)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx6)
		for !ok {
			ok = mgr.Transaction(tx6)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx7)
		for !ok {
			ok = mgr.Transaction(tx7)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx8)
		for !ok {
			ok = mgr.Transaction(tx8)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx9)
		for !ok {
			ok = mgr.Transaction(tx9)
		}
		wg.Done()
	}()
	go func() {
		ok := mgr.Transaction(tx10)
		for !ok {
			ok = mgr.Transaction(tx10)
		}
		wg.Done()
	}()
	wg.Wait()

	flag := true
	for k := range exp1 {
		if mgr.perObjectState[k].committedValue != exp1[k] + exp2[k] + exp3[k] + exp4[k] + exp5[k] + exp6[k] + exp7[k] + exp8[k] + exp9[k] + exp10[k] {
			flag = false
			fmt.Printf("diff on %v exp: %v, committed val: %v\n", k, exp1[k] + exp2[k] + exp3[k] + exp4[k] + exp5[k] + exp6[k] + exp7[k] + exp8[k] + exp9[k] + exp10[k], mgr.perObjectState[k].committedValue)
		}
	}
	if !flag {
		t.Fail()
	}
}

func generateValidTransaction(n int) ([]Operation, map[string]int) {
	var tx []Operation
	rand.Seed(time.Now().Unix())
	oprts := []string{"DEPOSIT", "WITHDRAW"}
	accts := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"}
	amts := make(map[string]int)
	for i := 0; i < n; i++ {
		r := rand.Int()
		oprt := oprts[i%len(oprts)]
		acct := accts[r%len(accts)]
		_, ok := amts[acct]
		if !ok && (oprt == "BALANCE" || oprt == "WITHDRAW") {
			oprt = "DEPOSIT"
		}
		amt := 0
		if oprt == "WITHDRAW" && amts[acct] != 0 {
			amt = rand.Intn(amts[acct])
			amts[acct] -= amt
		} else if oprt == "DEPOSIT"{
			amt = rand.Intn(100)
			amts[acct] += amt
		}
		tx = append(tx, Operation{
			Op:   oprt,
			Acct: acct,
			Amt:  amt,
		})
	}
	return tx, amts
}