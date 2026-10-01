"""Generate a real Python/notebook oracle for Workbench browser acceptance."""

import argparse
import json
from pathlib import Path
import shutil

import visionql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runtime_root", type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    image = repo / "data/datasets/images/coco128/images/000000000049.jpg"
    model = repo / "data/models/yolo26n.onnx"
    for path in (image, model):
        if not path.is_file():
            raise FileNotFoundError(f"Missing visual acceptance artifact: {path}")

    photos = args.runtime_root / "parity-photos"
    photos.mkdir()
    shutil.copyfile(image, photos / image.name)
    resource_root = Path(__file__).parent
    setup = (resource_root / "setup.sql").read_text()
    setup = setup.replace("${IMAGES_LOCATION}", str(photos).replace("'", "''"))
    setup = setup.replace("${MODEL}", str(model).replace("'", "''"))
    query = (resource_root / "detect.sql").read_text()
    session = visionql.connect(args.runtime_root / "python-catalog.db")
    for statement in session.run_script(setup):
        statement.collect()
    table = session.sql(query).collect()
    for column, extension in (("image", b"vql.image"), ("box", b"vql.box2d")):
        assert table.schema.field(column).metadata[b"ARROW:extension:name"] == extension
    rows = table.to_pylist()
    if not rows:
        raise AssertionError("The real detector must return person detections")
    for row in rows:
        assert row["label"] == "person"
        assert 0.25 <= row["confidence"] <= 1
        assert row["image"]["width"] > 0 and row["image"]["height"] > 0
        assert row["image"]["uri"] == row["uri"]
        box = row["box"]
        assert all(0 <= value <= 1 for value in box.values())
        assert box["w"] > 0 and box["h"] > 0
        # IMAGE may retain a local reference in Python; Flight supplies a thumbnail.
        row["image"] = {key: row["image"][key] for key in ("width", "height")}

    html = session.sql(query)._repr_html_()
    assert 'class="visionql-result"' in html and "person" in html
    (args.runtime_root / "python-notebook.html").write_text(html)
    reference = {
        "setupSql": setup,
        "querySql": query,
        "fields": [
            {
                "name": field.name,
                "extensionName": (field.metadata or {}).get(
                    b"ARROW:extension:name", b""
                ).decode(),
            }
            for field in table.schema
        ],
        "rows": rows,
    }
    (args.runtime_root / "visual-reference.json").write_text(
        json.dumps(reference, indent=2, allow_nan=False) + "\n"
    )
    print(f"Python/notebook visual reference: {len(rows)} person detections")


if __name__ == "__main__":
    main()
