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
    table = handle.collect()
    assert table.column("value").to_pylist() == [2, 4]
    metrics = handle.metrics()
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 2
    assert metrics["resources"]["arrow"]["peak_bytes"] > 0
    assert "value" in session.sql("SELECT 1 AS value")._repr_html_()


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
        "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' "
        "USING ONNX_RUNTIME WITH (output={labels=['person']})"
    ).collect()
    session.sql("RESOLVE MODEL detector").collect()
    table = session.sql(
        "SELECT CARDINALITY("
        "IMAGE_DETECTION('detector', image, "
        "classes => ['person'], min_confidence => 0.8)) AS people "
        "FROM photos WHERE quality(img => image) >= 0"
    ).collect()
    assert table.column("people").to_pylist() == [1]


def test_session_memory_limit_override(tmp_path):
    session = visionql.connect(
        tmp_path / "catalog.db", session_memory_limit_bytes=128
    )

    with pytest.raises(RuntimeError, match="session memory limit"):
        session.sql(f"SELECT '{'x' * 1024}' AS value").collect()
