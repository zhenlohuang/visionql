# VisionQL Python package

VisionQL is an embedded batch and streaming SQL engine for images, recorded video, and live RTSP streams. The Python package exposes the synchronous `visionql.connect()` API and returns query results as PyArrow tables.

```bash
python -m pip install visionql
```

```python
import visionql

session = visionql.connect()
table = session.sql("SELECT 1 AS value").collect()
print(table)
```

See the [project README](https://github.com/zhenlohuang/visionql#readme) for prerequisites, visual SQL examples, model setup, configuration, and the complete v0.1 scope.
