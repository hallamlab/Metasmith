from __future__ import annotations
import time
from pathlib import Path
from datetime import datetime as dt

class StdTime:
    FORMAT = '%Y-%m-%d_%H-%M-%S'

    @classmethod
    def Timestamp(cls, timestamp: dt|None = None):
        ts = dt.now() if timestamp is None else timestamp
        return f"{ts.strftime(StdTime.FORMAT)}"
    
    @classmethod
    def Parse(cls, timestamp: str|int):
        if isinstance(timestamp, str):
            return dt.strptime(timestamp, StdTime.FORMAT)
        else:
            return dt.fromtimestamp(timestamp/1000)
    
    @classmethod
    def CurrentTimeMillis(cls):
        return round(time.time() * 1000)

# https://stackoverflow.com/a/1446870/13690762
def IsText(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"file not found [{file_path}]")
    with open(file_path, "r", encoding="latin1") as f:
        s = f.read(512)
    text_characters = "".join([chr(x) for x in range(32, 127)] + list("\n\r\t\b"))
    translation_table = str.maketrans("", "", text_characters)
    if not s:
        # Empty files are considered text
        return True
    if "\0" in s:
        # Files with null bytes are likely binary
        return False
    # Get the non-text characters (maps a character to itself then
    # use the 'remove' option to get rid of the text characters.)
    t = s.translate(translation_table)
    # If more than 30% non-text characters, then
    # this is considered a binary file
    return float(len(t))/float(len(s)) <= 0.30
