import re

RECORD = re.compile(r"\bMRN[\s#:-]*\d{6,10}\b", re.IGNORECASE)
PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)")


def mask(text: str) -> str:
    return PHONE.sub("[phone number]", RECORD.sub("[record number]", text))


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
