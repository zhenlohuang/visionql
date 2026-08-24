WITH inferred AS (
  SELECT
    VQL_CLASSIFY(
      image,
      ['football_helmet'],
      output_mode => 'multi',
      min_score => 0.5
    ) AS multi_result,
    VQL_CLASSIFY(
      image,
      ['football_helmet']
    ) AS single_result
  FROM photos
  WHERE uri LIKE '%/000000000049.jpg'
)
SELECT
  CARDINALITY(multi_result) > 0
  AND CARDINALITY(single_result) = 1
  AND single_result[1]['label'] = 'football_helmet'
  AND single_result[1]['score'] BETWEEN 0.0 AND 1.0 AS valid
FROM inferred
