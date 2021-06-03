SELECT
    MAX(
            IF
                (o.label != (SELECT p.tables.value FROM UNNEST(o.predicted_label) p ORDER BY p.tables.score DESC LIMIT 1),
                 0,
                 1)) AS is_correct,
    MAX(label) AS label,
    MAX((SELECT p.tables.value FROM UNNEST(o.predicted_label) p ORDER BY p.tables.score DESC LIMIT 1)) as predicted_label,
    COUNT(o.label) AS count,
  MAX((SELECT MAX(p.tables.score) FROM UNNEST(o.predicted_label) p)) score,
  o.wifi_available,
  o.location_available,
  o.cell_available,
  o.gps_available
FROM
    `shl-2021-315220.export_evaluated_examples_dataset_15_cell_f_202106010911_2021_06_01T14_48_45_943Z.evaluated_examples` o
    # WHERE o.label = '5'
    # AND '7' = (SELECT p.tables.value FROM UNNEST(o.predicted_label) p ORDER BY p.tables.score DESC LIMIT 1)
GROUP BY
    o.label,
    (SELECT p.tables.value FROM UNNEST(o.predicted_label) p ORDER BY p.tables.score DESC LIMIT 1),
    o.wifi_available,
    o.location_available,
    o.cell_available,
    o.gps_available
ORDER BY
    MAX(o.label != (SELECT p.tables.value FROM UNNEST(o.predicted_label) p ORDER BY p.tables.score DESC LIMIT 1)),
    count DESC,
    o.label
;