import json
import os
import struct
import zlib
import uuid

class MiniDB:
    def __init__(self, path):
        self.path = path
        self.index = {}
        self.pending = None
        self.transaction_id = None
        self._recover()

    def begin(self):
        if self.pending is not None:
            raise RuntimeError("A transaction is already active")

        self.pending = []
        self.transaction_id = uuid.uuid4().hex
        

    def rollback(self):
        if self.pending is None:
            raise RuntimeError("No active transaction")

        self.pending = None
        self.transaction_id = None

    def commit(self):
        if self.pending is None:
            raise RuntimeError("No active transaction")

        if not self.pending:
            self.pending = None
            self.transaction_id = None
            return
        
        #batch = {"op" : "batch", "records" : self.pending}
        #text = json.dumps(batch, ensure_ascii=False)

        written_records = []

        with open(self.path, "ab") as file:
            # 先写入所有操作记录
            for record in self.pending:
                offset = file.tell()
                frame = self._encode_record(record)

                file.write(frame)
                written_records.append((record, offset))
            # offset = file.tell()
            # file.write((text + "\n").encode("utf-8"))
            # file.flush()
            # os.fsync(file.fileno())

            # 最后才写 COMMIT marker
            commit_record = {
                "op" : "commit",
                "txid" : self.transaction_id,
            }
            file.write(self._encode_record(commit_record))

            # 只有操作和 COMMIT 都写完，才同步到磁盘
            file.flush()
            os.fsync(file.fileno())

        # fsync 成功后，更新内存 index
        for record, offset in written_records:
            key = record["key"]

            if record.get("deleted", False):
                self.index.pop(key, None)
            else:
                self.index[key] = offset

        # for record in self.pending:
        #     key = record["key"]

        #     if record.get("deleted", False):
        #         self.index.pop(key, None)
        #     else:
        #         self.index[key] = offset

        self.pending = None
        self.transaction_id = None

    def _encode_record(self, record):
        payload = json.dumps(
            record, ensure_ascii=False
        ).encode("utf-8")

        length = len(payload)
        checksum = zlib.crc32(payload)

        header = struct.pack(">II", length, checksum)

        return header + payload

    def _read_frame(self, file):
        offset = file.tell()
        header = file.read(8)

        if header == b"":
            return None

        length, expected_checksum = struct.unpack(">II", header)

        payload = file.read(length)

        if len(payload) != length:
            raise EOFError("Incomplete frame payload")

        actual_checksum = zlib.crc32(payload)

        if actual_checksum != expected_checksum:
            raise ValueError("Checksum mismatch")

        return offset, json.loads(payload)
        
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
                "op" : "put",
                "txid" : self.transaction_id,
                "key": key,
                "value": value,
            })
            return
        record = {"key" : key, "value" : value}
        #text = json.dumps(record, ensure_ascii=False)
        frame = self._encode_record(record)

        with open(self.path, "ab") as file:
            offset = file.tell()
            file.write(frame)
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
            #line  = file.readline()
            #record = json.loads(line)
            _, record = self._read_frame(file)
        #return record["value"]
        if record.get("op") == "batch":
            for item in reversed(record["records"]):
                if item["key"] == key:
                    return item.get("value")

            raise RuntimeError("Index points to the wrong batch")

        return record["value"]

        #pass

    def delete(self, key):
        # Append tombstone record
        if self.pending is not None:
            self.pending.append({
                "op" : "delete",
                "txid" : self.transaction_id,
                "key" : key,
                "deleted" : True
            })
            return
        record = {"key" : key, "deleted" : True}
        #text = json.dumps(record, ensure_ascii=False)
        frame = self._encode_record(record)

        with open(self.path, "ab") as file:
            file.write(frame)
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

                try:
                    result = self._read_frame(file)
                except EOFError:
                    # 最后一条 Frame 没写完整：删除它
                    file.truncate(offset)
                    file.flush()
                    os.fsync(file.fileno())
                    break

                if result is None:
                    break

                _, record = result
                # line = file.readline()

                # if not line:
                #     break

                # if not line.endswith(b"\n"):
                #     file.truncate(offset)
                #     file.flush()
                #     os.fsync(file.fileno())
                #     break
                
                # # record = json.loads(line)
                # # #self.index[record["key"]] = offset
                # # if record.get("deleted", False):
                # #     self.index.pop(record["key"], None)
                # # else:
                # #     self.index[record["key"]] = offset

                # record = json.loads(line)
                if record.get("op") == "batch":
                    records = record["records"]
                else:
                    records = [record]

                for item in records:
                    key = item["key"]

                    if item.get("deleted", False):
                        self.index.pop(key, None)
                    else:
                        self.index[key] = offset
        #pass

if __name__ == "__main__":
    test_path = "transation_test_log"

    with open(test_path, "wb"):
        pass

    db = MiniDB(test_path)

    db.begin()
    db.put("name", "Alice")
    db.put("city", "Boston")
    db.commit()
    #db.delete("city")

    #print(db.pending)

    #db.rollback()
    print(db.get("name"))
    print(db.get("city"))

    # db.put("name", "Alice")
    # db.put("city", "Boston")
    # db.delete("city")

    # # 创建新对象，模拟程序重启
    # reopened = MiniDB(test_path)

    # print(reopened.get("name"))
    # print(reopened.get("city"))

    # db = MiniDB("framed_recovery_test.log")

    # print(db.transaction_id)  # None

    # db.begin()
    # print(db.transaction_id)  # 一串随机 ID

    # db.rollback()
    # print(db.transaction_id)  # None

    # db.put("name", "Alice")
    # print(db.get("name"))

    # db.delete("name")
    # print(db.get("name"))
    # db.put("name", "Alice")
    # db.put("city", "Boston")

    # print(db.get("name"))
    # print(db.get("city"))
    # print(db.get("missing"))

    # print(db.index)
    # print(os.path.getsize(test_path))

    # with open(test_path, "wb"):
    #     pass

    # db = MiniDB(test_path)

    # db.put("name", "Alice")
    # db.put("city", "Beijing")

    # # 模拟只写入了一部分 Batch record
    # with open(test_path, "ab") as file:
    #     file.write(
    #         b'{"op":"batch","records":['
    #         b'{"key":"name","value":"Bob"},'
    #     )

    # # 重新打开：半条 Batch 应该被丢弃
    # db = MiniDB(test_path)

    # assert db.get("name") == "Alice"
    # assert db.get("city") == "Beijing"

    # # 确认恢复之后，仍能正常提交事务
    # db.begin()
    # db.put("name", "Charlie")
    # db.delete("city")
    # db.commit()

    # db = MiniDB(test_path)

    # assert db.get("name") == "Charlie"
    # assert db.get("city") is None

    # print("Transaction recovery checks passed!")

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
    # db = MiniDB("data.log")
    # db.put("name", "Alice")

    # db.begin()

    # db.delete("name")
    # print(db.get("name"))

    # db.put("name", "Bob")

    # #db.put("name", "Charlie")

    # print(db.get("name"))

    # db.delete("name")
    # print(db.get("name"))
    # print(db.pending)

    # db.rollback()
    # print(db.get("name"))

    # db = MiniDB("data.log")
    # db.put("name", "Alice")
    # db.put("city", "Beijing")

    # db.begin()
    # db.put("name", "Bob")
    # db.delete("city")
    # db.commit()

    # assert db.get("name") == "Bob"
    # assert db.get("city") is None

    # # 验证重新打开后，提交结果仍然存在
    # reopened = MiniDB("data.log")
    # assert reopened.get("name") == "Bob"
    # assert reopened.get("city") is None

    # print("Commit checks passed!")

    # 使用独立的测试文件，每次测试都从空文件开始
    # db = MiniDB("data.log")

    # record = {"key": "name", "value": "Alice"}
    # frame = db._encode_record(record)

    # print("Header:", frame[:8])
    # print("Payload:", frame[8:].decode("utf-8"))
    # print("Total bytes:", len(frame))

    # db = MiniDB("data.log")

    # record = {"key": "name", "value": "Alice"}
    # frame = db._encode_record(record)

    # with open("frame_test.log", "wb") as file:
    #     file.write(frame)

    # with open("frame_test.log", "rb") as file:
    #     offset, decoded = db._read_frame(file)

    #     print(offset)
    #     print(decoded)
    #     print(db._read_frame(file))