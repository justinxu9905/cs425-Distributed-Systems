import random
from collections import defaultdict
import sys
from string import ascii_lowercase
from time import sleep

# There will be 26**ACCOUNT_LEN accounts
ACCOUNT_LEN = 1

# probability that a transaction is a deposit
# initially more transaction will be deposits since transfers from non-existent accounts will not be placed
DEP_PROB = 0.1

# probability that a transfer tries to empty out the balance
# recommend leaving this at 0 until you want to test illegal transaction detection
ILLEGAL_TRANSFER_PROB = 0.0

def random_account(): 
    return ''.join(random.choice(ascii_lowercase) for _ in range(ACCOUNT_LEN))
# 0 balance by default
balances = defaultdict(int)

if len(sys.argv) > 1:
    rate = float(sys.argv[1])
else:
    rate = 1.0

while True:
    if random.random() < DEP_PROB:
        account = random_account()
        amount = random.randrange(1,101)
        print(f"DEPOSIT {account} {amount}")
        balances[account] += amount
    else:
        illegal = random.random() < ILLEGAL_TRANSFER_PROB
        account = random_account()
        if balances[account] == 0 and not illegal:
            continue
        if illegal:
            amount = random.randrange(balances[account]+1, balances[account]+101)
        else:
            amount = random.randrange(1, balances[account]+1)

        while True:
            # ensure _different_ account is the destination
            dest = random_account()
            if dest != account:
                break

        print(f"TRANSFER {account} -> {dest} {amount}")
        if not illegal: # update local balances
            balances[account] -= amount
            balances[dest] += amount
    sleep(random.expovariate(rate))