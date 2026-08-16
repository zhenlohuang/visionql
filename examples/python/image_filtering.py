"""Filter local images with an Arrow Python UDF and typed ONNX inference."""

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import visionql


def quality_score(images):
    """Return one vectorized quality value per encoded IMAGE."""
    return pc.cast(pc.is_valid(images.field("encoded")), pa.float32())


if __name__ == "__main__":
    repo = Path(__file__).resolve().parents[2]
    images = (repo / "data/datasets/images/coco128/images").resolve()
    model = (repo / "data/models/yolo26n.onnx").resolve()
    if not model.is_file():
        raise SystemExit("Run 'python scripts/export_yolo26.py --size n' first")
    session = visionql.connect()
    session.sql(
        f"CREATE TABLE product_photos USING IMAGES LOCATION '{images}' WITH (recursive=true)"
    ).collect()
    session.sql(
        "CREATE FUNCTION quality(img IMAGE) RETURNS FLOAT "
        "LANGUAGE PYTHON AS '__main__:quality_score'"
    ).collect()
    session.sql(
        f"CREATE MODEL yolo26n TYPE OBJECT_DETECTION "
        f"FROM 'file://{model}' USING ONNX_RUNTIME "
        "WITH ("
        "input={name='images', width=640, height=640}, "
        "output={name='output0', format='yolo_e2e', labels='coco80'}"
        ")"
    ).collect()
    session.sql("RESOLVE MODEL yolo26n").collect()
    result = session.sql(
        "SELECT uri, CARDINALITY("
        "IMAGE_DETECTION('yolo26n', image, "
        "classes => ['person'], min_confidence => 0.6)) AS people "
        "FROM product_photos WHERE quality(image) >= 0 ORDER BY uri"
    )
    print(result.show(20))
