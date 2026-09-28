import pytest
from pydantic import TypeAdapter

from rfnry_thread.errors import InputError
from rfnry_thread.models import Document, Identity, Image, Part, Text

PARTS: TypeAdapter[list[Part]] = TypeAdapter(list[Part])


def test_parts_parse_by_their_type() -> None:
    parsed = PARTS.validate_python(
        [
            {"type": "text", "text": "hi"},
            {"type": "image", "media_type": "image/png", "url": "https://x/a.png"},
            {
                "type": "document",
                "media_type": "application/pdf",
                "name": "a.pdf",
                "url": "https://x/a.pdf",
            },
        ]
    )
    assert [type(p) for p in parsed] == [Text, Image, Document]


@pytest.mark.parametrize("fields", [{}, {"data": b"x", "url": "https://x"}])
def test_a_media_part_carries_exactly_one_of_data_or_url(fields: dict[str, object]) -> None:
    with pytest.raises(InputError, match="^models: image carries exactly one of data or url$"):
        Image.model_validate({"media_type": "image/png", **fields})


def test_bytes_travel_as_standard_base64() -> None:
    image = Image(media_type="image/png", data=b"\xfb\xff")
    assert image.model_dump(mode="json")["data"] == "+/8="
    assert Image.model_validate_json(image.model_dump_json()).data == b"\xfb\xff"


def test_an_identity_is_one_class_with_a_role() -> None:
    ana = Identity(role="user", id="ana", name="Ana")
    assert ana.metadata == {}
    assert Identity.model_validate(ana.model_dump()) == ana
    with pytest.raises(ValueError):
        Identity.model_validate({"role": "robot", "id": "x"})
