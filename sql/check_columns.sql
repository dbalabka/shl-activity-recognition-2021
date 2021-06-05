select model_dataset.column_name as model_column,
       test_dataset.column_name as test_column

from (

SELECT column_name
FROM `shl-2021-315220.model_datasets`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'dataset_19_fix_cols'

) model_dataset

full outer join (

    SELECT column_name
    FROM `shl-2021-315220.model_datasets`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'test_dataset_19'
) test_dataset on test_dataset.column_name = model_dataset.column_name

where model_dataset.column_name is null or test_dataset.column_name is null

order by 1, 2
