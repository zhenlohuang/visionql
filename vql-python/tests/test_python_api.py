import base64
import sys
import types

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import visionql


def test_collect_and_vectorized_python_udf(tmp_path):
    module = types.ModuleType("vql_test_udfs")
    module.double = lambda values: pc.multiply(values, 2)
    sys.modules[module.__name__] = module

    session = visionql.connect(tmp_path / "catalog.db")
    session.sql(
        "CREATE FUNCTION double(x BIGINT) RETURNS BIGINT "
        "LANGUAGE PYTHON AS 'vql_test_udfs:double'"
    ).collect()
    handle = session.sql("SELECT double(column1) AS value FROM (VALUES (1), (2))")
    assert not hasattr(handle, "metrics")
    table = handle.collect()
    assert table.column("value").to_pylist() == [2, 4]
    assert "value" in session.sql("SELECT 1 AS value")._repr_html_()


def test_run_script_show_and_cancel(tmp_path):
    session = visionql.connect(tmp_path / "catalog.db")

    first, second = session.run_script("SELECT 1 AS one; SELECT 2 AS two;")

    assert "one" in first.show(1)
    assert second.collect().column("two").to_pylist() == [2]
    second.cancel()


def test_decode_batch_helper_preserves_null_images(monkeypatch):
    class FakeImage:
        def __init__(self, payload):
            self.payload = payload

        def copy(self):
            return self

    pillow = types.ModuleType("PIL")
    pillow.Image = types.SimpleNamespace(
        open=lambda stream: FakeImage(stream.read())
    )
    monkeypatch.setitem(sys.modules, "PIL", pillow)
    encoded = b"encoded-image"
    images = pa.StructArray.from_arrays(
        [pa.array([encoded, None], type=pa.binary())], names=["encoded"]
    )

    decoded = visionql.images.decode_batch(images)

    assert decoded[0].payload == encoded
    assert decoded[1] is None


def test_image_filtering_python_udf_then_model(tmp_path):
    photos = tmp_path / "photos"
    photos.mkdir()
    # 1x1 RGB PNG; media fixtures stay text/generated in the repository.
    photos.joinpath("one.png").write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )
    )
    module = types.ModuleType("scenario_ops")
    module.quality = lambda images: pc.cast(pc.is_valid(images.field("encoded")), pa.float32())
    sys.modules[module.__name__] = module
    session = visionql.connect(tmp_path / "catalog.db")
    session.sql(
        f"CREATE TABLE photos USING IMAGES LOCATION '{photos}'"
    ).collect()
    session.sql(
        "CREATE FUNCTION quality(img IMAGE) RETURNS FLOAT "
        "LANGUAGE PYTHON AS 'scenario_ops:quality'"
    ).collect()
    session.sql(
        "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person'"
    ).collect()
    session.sql("RESOLVE MODEL detector").collect()
    table = session.sql(
        "SELECT CARDINALITY("
        "detector(image, "
        "classes => ['person'], min_confidence => 0.8)) AS people "
        "FROM photos WHERE quality(img => image) >= 0"
    ).collect()
    assert table.column("people").to_pylist() == [1]


def test_session_memory_limit_override(tmp_path):
    session = visionql.connect(
        tmp_path / "catalog.db", session_memory_limit_bytes=128
    )

    with pytest.raises(visionql.VisionQLError, match="session memory limit") as caught:
        session.sql(f"SELECT '{'x' * 1024}' AS value").collect()

    error = caught.value
    assert isinstance(error, RuntimeError)
    assert error.code == "VQL-53001"
    assert error.symbol == "RESOURCE_EXHAUSTED"
    assert "session memory limit" in error.message
    assert error.target_version is None


def test_feature_error_preserves_target_version(tmp_path):
    session = visionql.connect(tmp_path / "catalog.db")

    with pytest.raises(visionql.VisionQLError) as caught:
        session.sql("CREATE INDEX future_index")

    error = caught.value
    assert error.code == "VQL-0A001"
    assert error.symbol == "FEATURE_NOT_AVAILABLE"
    assert error.message == "vector indexes are not available"
    assert error.target_version == "v0.3"
    assert str(error).endswith("(target: v0.3)")
