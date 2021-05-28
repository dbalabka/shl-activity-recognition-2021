with cells_raw as (

    SELECT  label.data_type,
            label.epoch_time,
            label.label,

            -- Cells
            cells.lte_available,
            cells.gsm_available,
            cells.wcdma_available,
            cells.lte_isRegistered,
            cells.gsm_isRegistered,
            cells.wcdma_isRegistered,
            cells.MCC,
            cells.MNC,
            cells.ci_cid,
            cells.TAC_LAC,
            cells.PCI_PSC,
            cells.lte_asuLevel,
            cells.gsm_asuLevel,
            cells.wcdma_asuLevel,
            cells.lte_dBm,
            cells.gsm_dBm,
            cells.wcdma_dBm,
            cells.lte_level,
            cells.gsm_level,
            cells.wcdma_level,
            PERCENTILE_DISC(cells.lte_available, 0.5) OVER(PARTITION BY label.epoch_time) as lte_available_median,
            PERCENTILE_DISC(cells.gsm_available, 0.5) OVER(PARTITION BY label.epoch_time) as gsm_available_median,
            PERCENTILE_DISC(cells.wcdma_available, 0.5) OVER(PARTITION BY label.epoch_time) as wcdma_available_median,
            PERCENTILE_DISC(cells.lte_isRegistered, 0.5) OVER(PARTITION BY label.epoch_time) as lte_isRegistered_median,
            PERCENTILE_DISC(cells.gsm_isRegistered, 0.5) OVER(PARTITION BY label.epoch_time) as gsm_isRegistered_median,
            PERCENTILE_DISC(cells.wcdma_isRegistered, 0.5) OVER(PARTITION BY label.epoch_time) as wcdma_isRegistered_median,
            PERCENTILE_DISC(cells.MCC, 0.5) OVER(PARTITION BY label.epoch_time) as MCC_median,
            PERCENTILE_DISC(cells.MNC, 0.5) OVER(PARTITION BY label.epoch_time) as MNC_median,
            PERCENTILE_DISC(cells.ci_cid, 0.5) OVER(PARTITION BY label.epoch_time) as ci_cid_median,
            PERCENTILE_DISC(cells.TAC_LAC, 0.5) OVER(PARTITION BY label.epoch_time) as TAC_LAC_median,
            PERCENTILE_DISC(cells.PCI_PSC, 0.5) OVER(PARTITION BY label.epoch_time) as PCI_PSC_median,
            PERCENTILE_DISC(cells.lte_dBm, 0.5) OVER(PARTITION BY label.epoch_time) as lte_dBm_median,
            PERCENTILE_DISC(cells.lte_asuLevel, 0.5) OVER(PARTITION BY label.epoch_time) as lte_asuLevel_median,
            PERCENTILE_DISC(cells.lte_level, 0.5) OVER(PARTITION BY label.epoch_time) as lte_level_median,
            PERCENTILE_DISC(cells.gsm_dBm, 0.5) OVER(PARTITION BY label.epoch_time) as gsm_dBm_median,
            PERCENTILE_DISC(cells.gsm_asuLevel, 0.5) OVER(PARTITION BY label.epoch_time) as gsm_asuLevel_median,
            PERCENTILE_DISC(cells.gsm_level, 0.5) OVER(PARTITION BY label.epoch_time) as gsm_level_median,
            PERCENTILE_DISC(cells.wcdma_dBm, 0.5) OVER(PARTITION BY label.epoch_time) as wcdma_dBm_median,
            PERCENTILE_DISC(cells.wcdma_asuLevel, 0.5) OVER(PARTITION BY label.epoch_time) as wcdma_asuLevel_median,
            PERCENTILE_DISC(cells.wcdma_level, 0.5) OVER(PARTITION BY label.epoch_time) as wcdma_level_median,

    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.label_train`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.label_test`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.label`
    ) label

    left join (

        select 'TRAIN' as data_type,
               cast(round(coalesce(lte.epoch_time, gsm.epoch_time, wcdma.epoch_time), -3) as int64) as epoch_time,
               if(lte.cell_type is not null, 1, 0) as lte_available,
               if(gsm.cell_type is not null, 1, 0) as gsm_available,
               if(wcdma.cell_type is not null, 1, 0) as wcdma_available,
               ifnull(lte.isRegistered, 0) as lte_isRegistered,
               ifnull(gsm.isRegistered, 0) as gsm_isRegistered,
               ifnull(wcdma.isRegistered, 0) as wcdma_isRegistered,
               coalesce(lte.MCC, gsm.MCC, wcdma.MCC) as MCC,
               coalesce(lte.MNC, gsm.MNC, wcdma.MNC) as MNC,
               coalesce(lte.ci, gsm.cid, wcdma.cid) as ci_cid,
               coalesce(lte.TAC, gsm.LAC, wcdma.LAC) as TAC_LAC,
               coalesce(lte.PCI, gsm.PSC, wcdma.PSC) as PCI_PSC,
               ifnull(lte.asuLevel, 0) as lte_asuLevel,
               ifnull(gsm.asuLevel, 0) as gsm_asuLevel,
               ifnull(wcdma.asuLevel, 0) as wcdma_asuLevel,
               ifnull(lte.dBm, -1000) as lte_dBm,
               ifnull(gsm.dBm, -1000) as gsm_dBm,
               ifnull(wcdma.dBm, -1000) as wcdma_dBm,
               ifnull(lte.level, 0) as lte_level,
               ifnull(gsm.level, 0) as gsm_level,
               ifnull(wcdma.level, 0) as wcdma_level,

        from (select * from `shl-2021.train.cells` where cell_type='LTE') lte
                 full outer join (select * from `shl-2021.train.cells` where cell_type='GSM') gsm on cast(round(gsm.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)
                 full outer join (select * from `shl-2021.train.cells` where cell_type='WCDMA') wcdma on cast(round(wcdma.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)

        union all

        select 'VALIDATE' as data_type,
               cast(round(coalesce(lte.epoch_time, gsm.epoch_time, wcdma.epoch_time), -3) as int64) as epoch_time,
               if(lte.cell_type is not null, 1, 0) as lte_available,
               if(gsm.cell_type is not null, 1, 0) as gsm_available,
               if(wcdma.cell_type is not null, 1, 0) as wcdma_available,
               ifnull(lte.isRegistered, 0) as lte_isRegistered,
               ifnull(gsm.isRegistered, 0) as gsm_isRegistered,
               ifnull(wcdma.isRegistered, 0) as wcdma_isRegistered,
               coalesce(lte.MCC, gsm.MCC, wcdma.MCC) as MCC,
               coalesce(lte.MNC, gsm.MNC, wcdma.MNC) as MNC,
               coalesce(lte.ci, gsm.cid, wcdma.cid) as ci_cid,
               coalesce(lte.TAC, gsm.LAC, wcdma.LAC) as TAC_LAC,
               coalesce(lte.PCI, gsm.PSC, wcdma.PSC) as PCI_PSC,
               ifnull(lte.asuLevel, 0) as lte_asuLevel,
               ifnull(gsm.asuLevel, 0) as gsm_asuLevel,
               ifnull(wcdma.asuLevel, 0) as wcdma_asuLevel,
               ifnull(lte.dBm, -1000) as lte_dBm,
               ifnull(gsm.dBm, -1000) as gsm_dBm,
               ifnull(wcdma.dBm, -1000) as wcdma_dBm,
               ifnull(lte.level, 0) as lte_level,
               ifnull(gsm.level, 0) as gsm_level,
               ifnull(wcdma.level, 0) as wcdma_level,

        from (select * from `shl-2021.train.cells` where cell_type='LTE') lte
                 full outer join (select * from `shl-2021.train.cells` where cell_type='GSM') gsm on cast(round(gsm.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)
                 full outer join (select * from `shl-2021.train.cells` where cell_type='WCDMA') wcdma on cast(round(wcdma.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)

        union all

        select 'TEST' as data_type,
               cast(round(coalesce(lte.epoch_time, gsm.epoch_time, wcdma.epoch_time), -3) as int64) as epoch_time,
               if(lte.cell_type is not null, 1, 0) as lte_available,
               if(gsm.cell_type is not null, 1, 0) as gsm_available,
               if(wcdma.cell_type is not null, 1, 0) as wcdma_available,
               ifnull(lte.isRegistered, 0) as lte_isRegistered,
               ifnull(gsm.isRegistered, 0) as gsm_isRegistered,
               ifnull(wcdma.isRegistered, 0) as wcdma_isRegistered,
               coalesce(lte.MCC, gsm.MCC, wcdma.MCC) as MCC,
               coalesce(lte.MNC, gsm.MNC, wcdma.MNC) as MNC,
               coalesce(lte.ci, gsm.cid, wcdma.cid) as ci_cid,
               coalesce(lte.TAC, gsm.LAC, wcdma.LAC) as TAC_LAC,
               coalesce(lte.PCI, gsm.PSC, wcdma.PSC) as PCI_PSC,
               ifnull(lte.asuLevel, 0) as lte_asuLevel,
               ifnull(gsm.asuLevel, 0) as gsm_asuLevel,
               ifnull(wcdma.asuLevel, 0) as wcdma_asuLevel,
               ifnull(lte.dBm, -1000) as lte_dBm,
               ifnull(gsm.dBm, -1000) as gsm_dBm,
               ifnull(wcdma.dBm, -1000) as wcdma_dBm,
               ifnull(lte.level, 0) as lte_level,
               ifnull(gsm.level, 0) as gsm_level,
               ifnull(wcdma.level, 0) as wcdma_level,

        from (select * from `shl-2021.validate.cells` where cell_type='LTE') lte
                 full outer join (select * from `shl-2021.validate.cells` where cell_type='GSM') gsm on cast(round(gsm.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)
                 full outer join (select * from `shl-2021.validate.cells` where cell_type='WCDMA') wcdma on cast(round(wcdma.epoch_time, -3) as int64) = cast(round(lte.epoch_time, -3) as int64)

    ) cells on cells.epoch_time = label.epoch_time and cells.data_type = label.data_type

--where label.epoch_time = 1493282527000
),
cell_agg as (
    -- Aggregated data
    select  data_type,
    epoch_time as epoch_time_id,
    label,

    -- Cells aggregated
    count(lte_available) as cells_count,
    min(lte_available) as lte_available_min,
    max(lte_available) as lte_available_max,
    min(gsm_available) as gsm_available_min,
    max(gsm_available) as gsm_available_max,
    min(wcdma_available) as wcdma_available_min,
    max(wcdma_available) as wcdma_available_max,

    min(lte_isRegistered) as lte_isRegistered_min,
    max(lte_isRegistered) as lte_isRegistered_max,
    min(gsm_isRegistered) as gsm_isRegistered_min,
    max(gsm_isRegistered) as gsm_isRegistered_max,
    min(wcdma_isRegistered) as wcdma_isRegistered_min,
    max(wcdma_isRegistered) as wcdma_isRegistered_max,
    min(MCC) as MCC_min,
    max(MCC) as MCC_max,
    min(MNC) as MNC_min,
    max(MNC) as MNC_max,
    min(ci_cid) as ci_cid_min,
    max(ci_cid) as ci_cid_max,
    min(TAC_LAC) as TAC_LAC_min,
    max(TAC_LAC) as TAC_LAC_max,
    min(PCI_PSC) as PCI_PSC_min,
    max(PCI_PSC) as PCI_PSC_max,
    min(lte_asuLevel) as lte_asuLevel_min,
    max(lte_asuLevel) as lte_asuLevel_max,
    avg(lte_asuLevel) as lte_asuLevel_avg,
    min(lte_dBm) as lte_dBm_min,
    max(lte_dBm) as lte_dBm_max,
    avg(lte_dBm) as lte_dBm_avg,
    min(lte_level) as lte_level_min,
    max(lte_level) as lte_level_max,
    avg(lte_level) as lte_level_avg,

    min(gsm_asuLevel) as gsm_asuLevel_min,
    max(gsm_asuLevel) as gsm_asuLevel_max,
    avg(gsm_asuLevel) as gsm_asuLevel_avg,
    min(gsm_dBm) as gsm_dBm_min,
    max(gsm_dBm) as gsm_dBm_max,
    avg(gsm_dBm) as gsm_dBm_avg,
    min(gsm_level) as gsm_level_min,
    max(gsm_level) as gsm_level_max,
    avg(gsm_level) as gsm_level_avg,

    min(wcdma_asuLevel) as wcdma_asuLevel_min,
    max(wcdma_asuLevel) as wcdma_asuLevel_max,
    avg(wcdma_asuLevel) as wcdma_asuLevel_avg,
    min(wcdma_dBm) as wcdma_dBm_min,
    max(wcdma_dBm) as wcdma_dBm_max,
    avg(wcdma_dBm) as wcdma_dBm_avg,
    min(wcdma_level) as wcdma_level_min,
    max(wcdma_level) as wcdma_level_max,
    avg(wcdma_level) as wcdma_level_avg,

    avg(ci_cid_median) as ci_cid_median,
    avg(MCC_median) as MCC_median,
    avg(MNC_median) as MNC_median,
    avg(PCI_PSC_median) as PCI_PSC_median,
    avg(TAC_LAC_median) as TAC_LAC_median,

    avg(lte_available_median) as lte_available_median,
    avg(lte_isRegistered_median) as lte_isRegistered_median,
    avg(lte_dBm_median) as lte_dBm_median,
    avg(lte_asuLevel_median) as lte_asuLevel_median,
    avg(lte_level_median) as lte_level_median,

    avg(gsm_available_median) as gsm_available_median,
    avg(gsm_isRegistered_median) as gsm_isRegistered_median,
    avg(gsm_dBm_median) as gsm_dBm_median,
    avg(gsm_asuLevel_median) as gsm_asuLevel_median,
    avg(gsm_level_median) as gsm_level_median,

    avg(wcdma_available_median) as wcdma_available_median,
    avg(wcdma_isRegistered_median) as wcdma_isRegistered_median,
    avg(wcdma_dBm_median) as wcdma_dBm_median,
    avg(wcdma_asuLevel_median) as wcdma_asuLevel_median,
    avg(wcdma_level_median) as wcdma_level_median,

    from cells_raw
    group by data_type,
    epoch_time,
    label
),
location as (
    SELECT
        cast(round(epoch_time, -3) as int64) as epoch_time_id,

        location.*,

        PERCENTILE_CONT(Latitude, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as location_Latitude_median,
        PERCENTILE_CONT(Longitude, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as location_Longitude_median,
        PERCENTILE_CONT(Altitude, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as location_Altitude_median,
        PERCENTILE_CONT(accuracy, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as location_accuracy_median
    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.location`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.location`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.location`
    ) location
),
location_agg as (
    SELECT
        data_type,
        epoch_time_id,
         -- Location aggregated
        count(Latitude) as location_count,
        min(Latitude) as location_Latitude_min,
        max(Latitude) as location_Latitude_max,
        avg(Latitude) as location_Latitude_avg,
        min(Longitude) as location_Longitude_min,
        max(Longitude) as location_Longitude_max,
        avg(Longitude) as location_Longitude_avg,
        min(Altitude) as location_Altitude_min,
        max(Altitude) as location_Altitude_max,
        avg(Altitude) as location_Altitude_avg,
        min(accuracy) as location_accuracy_min,
        max(accuracy) as location_accuracy_max,
        avg(accuracy) as location_accuracy_avg,
        avg(location_Latitude_median) as location_Latitude_median,
        avg(location_Longitude_median) as location_Longitude_median,
        avg(location_Altitude_median) as location_Altitude_median,
        avg(location_accuracy_median) as location_accuracy_median
    FROM location
    GROUP BY data_type, epoch_time_id
),
wifi as (
    SELECT
        cast(round(Epoch_time__ms_, -3) as int64) as epoch_time_id,

        wifi.*,

        PERCENTILE_DISC(wifi.Frequency__MHz_, 0.5) OVER (PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as wifi_Frequency_median,
        PERCENTILE_DISC(wifi.RSSI, 0.5) OVER(PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as wifi_RSSI_median
    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.wifi`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.wifi`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.wifi`
    ) wifi
),
wifi_agg as (
    SELECT
        data_type,
        epoch_time_id,
        -- WiFi aggregated
        count(RSSI) as wifi_count,
        any_value(BSSID) as wifi_BSSID_any,
        any_value(SSID) as wifi_SSID_any,
        any_value(Capabilities) as wifi_Capabilities_any,
        min(Frequency__MHz_) as wifi_Frequency_min,
        max(Frequency__MHz_) as wifi_Frequency_max,
        avg(wifi_Frequency_median) as wifi_Frequency_median,
        min(RSSI) as wifi_RSSI_min,
        max(RSSI) as wifi_RSSI_max,
        avg(wifi_RSSI_median) as wifi_RSSI_median
    FROM wifi
    GROUP BY data_type, epoch_time_id
),
gps as (
    SELECT
        cast(round(Epoch_time__ms_, -3) as int64) as epoch_time_id,

        gps.*,

        PERCENTILE_DISC(gps.ID, 0.5) OVER(PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as gps_ID_median,
        PERCENTILE_DISC(gps.Azimuth__degrees_, 0.5) OVER(PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as gps_Azimuth_median,
        PERCENTILE_DISC(gps.Elevation__degrees_, 0.5) OVER(PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as gps_Elevation_median,
        PERCENTILE_DISC(gps.SNR, 0.5) OVER(PARTITION BY cast(round(Epoch_time__ms_, -3) as int64)) as gps_SNR_median,
    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.gps`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.gps`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.gps`
    ) gps
),
gps_agg as (
    SELECT
        data_type,
        cast(round(Epoch_time__ms_, -3) as int64) as epoch_time_id,
         -- GPS aggregated
        count(Azimuth__degrees_) as gps_count,
        min(Azimuth__degrees_) as gps_Azimuth_min,
        max(Azimuth__degrees_) as gps_Azimuth_max,
        avg(Azimuth__degrees_) as gps_Azimuth_avg,
        min(Elevation__degrees_) as gps_Elevation_min,
        max(Elevation__degrees_) as gps_Elevation_max,
        avg(Elevation__degrees_) as gps_Elevation_avg,
        min(SNR) as gps_SNR_min,
        max(SNR) as gps_SNR_max,
        avg(SNR) as gps_SNR_avg,
        avg(gps_ID_median) as gps_ID_median,
        avg(gps_Azimuth_median) as gps_Azimuth_median,
        avg(gps_Elevation_median) as gps_Elevation_median,
        avg(gps_SNR_median) as gps_SNR_median
    FROM gps
    GROUP BY data_type, epoch_time_id
),
aggregated_with_features as (
    SELECT

        label.*,
--         wifi_names.* EXCEPT (data_type, epoch_time_id),
--         wifi_ssid.* EXCEPT (data_type, epoch_time_id),
        wifi_ssid_concat.* EXCEPT (data_type, epoch_time_id),
        features_location.* EXCEPT (data_type, epoch_time, accuracy, Altitude, Latitude, Longitude),
        location_agg.* EXCEPT (data_type, epoch_time_id),
        wifi_agg.* EXCEPT (data_type, epoch_time_id, wifi_SSID_any),
        gps_agg.* EXCEPT (data_type, epoch_time_id),
        cell_agg.* EXCEPT (data_type, epoch_time_id)

    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.label_train`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.label_test`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.label`
    ) label

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_wifi_names`
    ) wifi_names on wifi_names.epoch_time_id = label.epoch_time and wifi_names.data_type = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_wifi_ssid`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_wifi_ssid`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_wifi_ssid`
    ) wifi_ssid on wifi_ssid.epoch_time_id = label.epoch_time and wifi_ssid.data_type = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_wifi_ssid_concat`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_wifi_ssid_concat`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_wifi_ssid_concat`
    ) wifi_ssid_concat on wifi_ssid_concat.epoch_time_id = label.epoch_time and wifi_ssid_concat.data_type = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_denys`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_denys`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_denys`
    ) features_location on features_location.epoch_time = label.epoch_time and features_location.data_type = label.data_type

    LEFT JOIN location_agg ON location_agg.epoch_time_id = label.epoch_time and location_agg.data_type = label.data_type
    LEFT JOIN wifi_agg ON wifi_agg.epoch_time_id = label.epoch_time and wifi_agg.data_type = label.data_type
    LEFT JOIN gps_agg ON gps_agg.epoch_time_id = label.epoch_time and gps_agg.data_type = label.data_type
    LEFT JOIN cell_agg ON cell_agg.epoch_time_id = label.epoch_time and cell_agg.data_type = label.data_type

    --where label.epoch_time = 1493282527000
)

select * EXCEPT (epoch_time) FROM aggregated_with_features
