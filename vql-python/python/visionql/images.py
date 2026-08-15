"""Helpers for vectorized IMAGE Python UDFs."""

from io import BytesIO


def decode_batch(images):
    """Decode the encoded rows of an IMAGE StructArray with Pillow.

    VisionQL materializes referenced rows before invoking a Python UDF.
    """
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("decode_batch requires Pillow") from exc
    encoded = images.field("encoded").to_pylist()
    return [None if value is None else Image.open(BytesIO(value)).copy() for value in encoded]
