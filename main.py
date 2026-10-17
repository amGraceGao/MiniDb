import json
import os

class MiniDB:
    def __init__(self, path):
        self.path = path
        self.index = {}
        self.pending = None
        self._recover()

    def begin(self):
        if self.pending is not None:
            raise RuntimeError("A transaction is already active")

        self.pending = []

    def rollback(self):
        if self.pending is None:
            raise RuntimeError("No active transaction")

        self.pending = None

    def put(self, key, value):
        # 1. Serialize record
        # 2. Append to log
        # 3. Flush according to durability policy
        # 4. Update in-memory index

        # record = {"key" : key, "value" : value}
        # text = json.dumps(record, ensure_ascii = False)

        # with open(self.path, "a", encoding="utf-8") as file:
        #     file.write(text + "\n")

        if self.pending is not None:
            self.pending.append({
                "key": key,
                "value": value,
            })
            return
        record = {"key" : key, "value" : value}
        text = json.dumps(record, ensure_ascii=False)

        with open(self.path, "ab") as file:
            offset = file.tell()
            file.write((text + "\n").encode("utf-8"))
            file.flush()
            os.fsync(file.fileno())

        self.index[key] = offset


       # pass

    def get(self, key):
        # 1. Find offset from index
        # 2. Seek to record
        # 3. Decode and return value
        # result = None

        # with open(self.path, "r", encoding="utf-8") as file:
        #     for line in file:
        #         record = json.loads(line)

        #         if record["key"] == key:
        #             result = record["value"]

        # return result
        if self.pending is not None:
            for record in reversed(self.pending):
                if record["key"] == key:
                    if record.get("deleted", False):
                        return None
                    return record["value"]
        if key not in self.index:
            return None
        offset = self.index[key]

        with open(self.path, "rb") as file:
            file.seek(offset)
            line  = file.readline()
            record = json.loads(line)

        return record["value"]

        #pass

    def delete(self, key):
        # Append tombstone record
        if self.pending is not None:
            self.pending.append({
                "key" : key,
                "deleted" : True
            })
            return
        record = {"key" : key, "deleted" : True}
        text = json.dumps(record, ensure_ascii=False)

        with open(self.path, "ab") as file:
            file.write((text + "\n").encode("utf-8"))
            file.flush()
            os.fsync(file.fileno())

        self.index.pop(key, None)
        #pass

    def _recover(self):
        self.index.clear()
        # Replay valid committed records
        #with open(self.path, "r", encoding="utf-8") as file:
        with open(self.path, "a+b") as file:
            file.seek(0)

            while True:
                offset = file.tell()
                line = file.readline()

                if not line:
                    break

                if not line.endswith(b"\n"):
                    file.truncate(offset)
                    file.flush()
                    os.fsync(file.fileno())
                    break
                
                record = json.loads(line)
                #self.index[record["key"]] = offset
                if record.get("deleted", False):
                    self.index.pop(record["key"], None)
                else:
                    self.index[record["key"]] = offset
        #pass

# db = MiniDB("data.log")
# #print(db.path)
# #print(db.index)

# #with open(db.path, "a", encoding="utf-8") as file:
# #  file.write("hello\n")

# #db.put("name", "Alice")
# #db.put("name", "Bob")
# #db.put("city", "Beijing")

# db.put("name", "Charlie")
# print(db.index)
# print(db.get("name"))

# db.delete("name")
# print(db.get("name"))

# db = MiniDB("data.log")
# print(db.get("name"))
# print(db.get("missing"))

# db = MiniDB("data.log")

# # 重复写入：最新值生效
# db.put("user:1", "Alice")
# db.put("user:1", "Bob")
# assert db.get("user:1") == "Bob"

# # 删除
# db.put("user:2", "Charlie")
# db.delete("user:2")
# assert db.get("user:2") is None

# # 重新打开：从日志重建 index
# reopened = MiniDB("data.log")
# assert reopened.get("user:1") == "Bob"
# assert reopened.get("user:2") is None

# print("All checks passed!")

# 测试首次启动：运行前确保这个文件不存在
# db = MiniDB("recovery_test.log")
# assert db.get("name") is None

# db.put("name", "Alice")

# # 模拟崩溃留下半条记录
# with open("recovery_test.log", "ab") as file:
#     file.write(b'{"key": "name", "value": ')

# # 重新打开，应该保留之前的完整记录
# db = MiniDB("recovery_test.log")
# assert db.get("name") == "Alice"

# # 确认清理尾部后，还能正常写入和恢复
# db.put("name", "Bob")
# db = MiniDB("recovery_test.log")
# assert db.get("name") == "Bob"

# print("Recovery checks passed!")
db = MiniDB("data.log")
db.put("name", "Alice")

db.begin()

db.delete("name")
print(db.get("name"))

db.put("name", "Bob")

#db.put("name", "Charlie")

print(db.get("name"))

db.delete("name")
print(db.get("name"))
print(db.pending)

db.rollback()
print(db.get("name"))