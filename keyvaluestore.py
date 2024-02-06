from dataclasses import dataclass
import os
import hashlib
import struct


@dataclass
class Record:
    key: str
    value: str
    next: int = 0
    max_key_len: int = 8
    max_value_len: int = 256
    max_next_len: int = 8

    # post init to verify input
    def __post_init__(self):
        if len(self.key) > self.max_key_len:
            raise ValueError("key too long")
        if len(self.value) > self.max_value_len:
            raise ValueError("value too long")
        if self.next > 1 << self.max_next_len * 8:
            raise ValueError("next too big")

    @property
    def size(self):
        return self.max_key_len + self.max_value_len + self.max_next_len

    def to_bytes(self) -> bytes:
        b_key = self.key.encode().ljust(self.max_key_len, b"\0")
        b_value = self.value.encode().ljust(self.max_value_len, b"\0")
        b_next = self.next.to_bytes(self.max_next_len, "little")

        return b_key + b_value + b_next

    @classmethod
    def from_bytes(cls, b: bytes) -> "Record":
        record = cls("", "")
        key, value, next = struct.unpack(f"{cls.max_key_len}s{cls.max_value_len}sQ", b)
        record.key = key.decode().rstrip("\0")
        record.value = value.decode().rstrip("\0")
        record.next = next
        return record

    def is_empty(self) -> bool:
        return self.key == "" and self.value == "" and self.next == 0


# initiate empty file and inserting record
def init_file(file_name: str, max_records: int) -> None:
    with open(file_name, "wb") as f:
        for _ in range(max_records):
            f.write(Record("", "").to_bytes())


class DB:
    def __init__(self, file_name: str, max_buckets: int, overwrite: bool = False):
        if not os.path.exists(file_name) or overwrite:
            init_file(file_name, max_buckets)
        self.file_name = file_name
        self.max_buckets = max_buckets
        self._file = open(file_name, "r+b")
        self._record_size = Record("", "").size

    def get_hash_index(self, key: str) -> int:
        hash_result = hashlib.md5(key.encode())
        return int(hash_result.hexdigest(), 16) % self.max_buckets

    def write_record(self, record: Record, record_id: int) -> None:
        self._file.seek(record_id * self._record_size)
        self._file.write(record.to_bytes())

    def __setitem__(self, key: str, value: str) -> None:
        new_record = Record(key, value)
        new_record_id = self.get_hash_index(key)

        while True:
            self._file.seek(new_record_id * self._record_size)
            current_record = Record.from_bytes(self._file.read(self._record_size))
            if current_record.is_empty():
                self.write_record(new_record, new_record_id)
                break
            elif current_record.key == key:
                new_record.next = current_record.next
                self.write_record(new_record, new_record_id)
                break
            elif current_record.next == 0:
                prev = new_record_id
                new_record_id = self._file.tell() // self._record_size
                current_record.next = new_record_id
                new_record.next = 0
                self.write_record(current_record, prev)
                self.write_record(new_record, new_record_id)
                break
            else:
                new_record_id = current_record.next

    def __getitem__(self, key: str) -> str:
        record_id = self.get_hash_index(key)
        while True:
            self._file.seek(record_id * self._record_size)
            current_record = Record.from_bytes(self._file.read(self._record_size))

            if current_record.key == key:
                return current_record.value
            elif current_record.next == 0:
                return None
            else:
                record_id = current_record.next


TEST_DB = DB("filesystem.db", 2, overwrite=True)
TEST_DB["eve"] = "lala"
value = TEST_DB["eve"]
print(value)
