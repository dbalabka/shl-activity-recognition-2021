SELECT

      epoch_time,
      --array_agg(tables.score order by tables.score desc limit 1) as max_score,
      array_agg(tables.value order by tables.score desc limit 1) as predicted_label


FROM `shl-2021-315220.prediction_dataset_19_distan_20210605062802_2021_06_05T16_11_08_924Z.predictions`,
      unnest(predicted_label) as pred_label

group by epoch_time
order by epoch_time
