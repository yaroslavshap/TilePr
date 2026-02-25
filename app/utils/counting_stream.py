# app/utils/counting_stream.py

from typing import BinaryIO

class CountingReader:
    def __init__(self, base: BinaryIO):
        self.base = base
        self.count = 0

    def read(self, size: int = -1) -> bytes:
        b = self.base.read(size)
        self.count += len(b)
        return b



