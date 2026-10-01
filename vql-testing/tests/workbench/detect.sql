SELECT p.uri,
       p.image,
       det.label AS label,
       det.confidence AS confidence,
       det.box AS box
FROM parity_photos AS p,
     UNNEST(parity_detector(p.image,
       classes => ['person'],
       min_confidence => 0.25
     )) AS u(det)
ORDER BY det.confidence DESC, det.box.x, det.box.y;
