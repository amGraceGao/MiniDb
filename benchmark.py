import os
import time

from main import MiniDB

TEST_PATH = "benchmark.log"
COUNT = 1_000

def throughput(count, elapsed_seconds):
    return count / elapsed_seconds

#every benchmark started with empty log

with open(TEST_PATH, "wb"):
    pass

db = MiniDB(TEST_PATH)

#Benchmark: put throughput
start = time.perf_counter()

for i in range(COUNT):
    db.put(f"user:{i}", f"value:{i}")

put_seconds = time.perf_counter() - start

print(f"Put time: {put_seconds:.4f} seconds")
print(f"Put throughputL {throughput(COUNT, put_seconds):.2f} ops/sec")

#Warm-up: 先读取一次，让 OS page cache 预热
for i in range(COUNT):
    db.get(f"user:{i}")

#Benchmark: indexed get throughput
start = time.perf_counter()

for i in range(COUNT):
    value = db.get(f"user:{i}")
    assert value == f"value:{i}"

get_seconds = time.perf_counter() - start

print(f"Get time: {get_seconds:.4f} seconds")
print(f"Get throughput:{throughput(COUNT, get_seconds):.2f} ops/sec")