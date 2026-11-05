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
            # Write all operation records first.
            for record in self.pending:
                offset = file.tell()
                frame = self._encode_record(record)

                file.write(frame)
                written_records.append((record, offset))
            # offset = file.tell()
            # file.write((text + "\n").encode("utf-8"))
            # file.flush()
            # os.fsync(file.fileno())

            # Append the COMMIT marker after all operation records.
            commit_record = {
                "op" : "commit",
                "txid" : self.transaction_id,
            }
            file.write(self._encode_record(commit_record))

            # Sync to disk only after writing all operations and the COMMIT marker.
            file.flush()
            os.fsync(file.fileno())

        # Update the in-memory index after fsync succeeds.
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

        if len(header) != 8:
            raise EOFError("Incomplete frame header")

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

        # Buffer uncommitted transaction operations:
        # txid -> [(record, offset), ...]
        pending_transactions = {}

        # Replay valid committed records
        #with open(self.path, "r", encoding="utf-8") as file:

        def apply_record(record, offset):
            key = record["key"]

            if record.get("deleted", False):
                self.index.pop(key, None)
            else:
                self.index[key] = offset

        with open(self.path, "a+b") as file:
            file.seek(0)

            while True:
                offset = file.tell()

                try:
                    result = self._read_frame(file)
                except EOFError:
                    # Truncate the incomplete frame at the end of the log.
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
                
                
                # if record.get("op") == "batch":
                #     records = record["records"]
                # else:
                #     records = [record]

                # for item in records:
                #     key = item["key"]

                #     if item.get("deleted", False):
                #         self.index.pop(key, None)
                #     else:
                #         self.index[key] = offset
                
                _, record = result
                operation = record.get("op")

                # Buffer transactional put/delete operations until a COMMIT marker is found.
                if operation in ("put", "delete") and "txid" in record:
                    txid = record["txid"]

                    if txid not in pending_transactions:
                        pending_transactions[txid] = []

                    pending_transactions[txid].append((record, offset))

                # Apply all transaction operations only after reading the COMMIT marker.
                elif operation == "commit":
                    txid = record["txid"]

                    if txid not in pending_transactions:
                        raise ValueError("COMMIT without transaction operations")

                    for transaction_record, transaction_offset in pending_transactions[txid]:
                        apply_record(transaction_record, transaction_offset)

                    del pending_transactions[txid]

                # Support the legacy batch format.
                elif operation == "batch":
                    for item in record["records"]:
                        apply_record(item, offset)

                # Apply non-transactional put/delete records directly.
                else:
                    apply_record(record, offset)


if __name__ == "__main__":

    test_path = "phase2_test.log"

    # Start each test run with an empty log.
    with open(test_path, "wb"):
        pass

    db = MiniDB(test_path)

    # 1. Rollback: discard buffered changes.
    db.put("name", "Alice")

    db.begin()
    db.put("name", "Temporary")
    assert db.get("name") == "Temporary"

    db.rollback()
    assert db.get("name") == "Alice"

    # 2. Commit: persist the buffered operations.
    db.begin()
    db.put("name", "Bob")
    db.put("city", "Boston")
    db.commit()

    assert db.get("name") == "Bob"
    assert db.get("city") == "Boston"

    # 3. Commit a deletion.
    db.begin()
    db.delete("city")
    db.commit()

    assert db.get("city") is None

    # 4. Simulate a complete operation followed by an incomplete COMMIT frame.
    unfinished_txid = uuid.uuid4().hex

    unfinished_operation = {
        "op": "put",
        "txid": unfinished_txid,
        "key": "name",
        "value": "Should not appear",
    }
    incomplete_commit = {
        "op": "commit",
        "txid": unfinished_txid,
    }

    with open(test_path, "ab") as file:
        file.write(db._encode_record(unfinished_operation))

        # Write only part of the COMMIT frame.
        file.write(db._encode_record(incomplete_commit)[:4])

    # 5. Simulate a restart.
    reopened = MiniDB(test_path)

    assert reopened.get("name") == "Bob"
    assert reopened.get("city") is None

    print("Phase 2 checks passed!")
   
