import hashlib

def md5_id(text: str, length: int = 16) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:length].upper()
