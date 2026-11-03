import os
import sys

from main import MiniDB

def estimate_index_bytes(index):
    total = sys.getsizeof(index)

    for key, offset in index.items():
        total += sys.getsizeof(key)
        total += sys.getsizeof(offset)

    return total

sizes = [100, 500, 1_000]

for count in sizes:
    test_path = f"index_memory_{count}.log"

    #Every data use independent empty log
    with open(test_path, "wb"):
        pass

    db = MiniDB(test_path)

    #Use One transaction write batchly to avoid every put operation fsync db.begin() independently
    
    db.begin()

    for i in range(count):
        db.put(f"user:{i}", f"value:{i}")

    db.commit()

    index_bytes = estimate_index_bytes(db.index)
    bytes_per_entry = index_bytes / count

    print(
        f"Entries: {count:,} | "
        f"Index memory: {index_bytes:,} bytes | "
        f"Per entry: {bytes_per_entry:.2f} bytes"
    )