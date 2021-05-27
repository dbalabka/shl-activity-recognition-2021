-- Raw data
with cells_lte as (
    SELECT
        cast(round(epoch_time, -3) as int64) as epoch_time_id,

        cells.*,

        PERCENTILE_DISC(cells.isRegistered, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_isRegistered_median,
        PERCENTILE_DISC(cells.MCC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MCC_median,
        PERCENTILE_DISC(cells.MNC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MNC_median,
        PERCENTILE_DISC(cells.ci, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_ci_cid_median,
        PERCENTILE_DISC(cells.TAC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_TAC_LAC_median,
        PERCENTILE_DISC(cells.PCI, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_PCI_PSC_median,
        PERCENTILE_DISC(cells.dBm, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_dBm_median,
        PERCENTILE_DISC(cells.asuLevel, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_asuLevel_median,
        PERCENTILE_DISC(cells.level, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_level_median,
    from (
        select *, 'TRAIN' as data_type from `shl-2021.train.cells` where cell_type='LTE'
        union all
        select *, 'TEST' as data_type from `shl-2021.train.cells` where cell_type='LTE'
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.validate.cells` where cell_type='LTE'
    ) cells
),
cells_gsm as (
    SELECT
    cast(round(epoch_time, -3) as int64) as epoch_time_id,

    cells.*,

    PERCENTILE_DISC(cells.isRegistered, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_isRegistered_median,
    PERCENTILE_DISC(cells.MCC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MCC_median,
    PERCENTILE_DISC(cells.MNC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MNC_median,
    PERCENTILE_DISC(cells.ci, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_ci_cid_median,
    PERCENTILE_DISC(cells.TAC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_TAC_LAC_median,
    PERCENTILE_DISC(cells.PCI, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_PCI_PSC_median,
    PERCENTILE_DISC(cells.dBm, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_dBm_median,
    PERCENTILE_DISC(cells.asuLevel, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_asuLevel_median,
    PERCENTILE_DISC(cells.level, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_level_median,
    from (
    select *, 'TRAIN' as data_type from `shl-2021.train.cells` where cell_type='GSM'
    union all
    select *, 'TEST' as data_type from `shl-2021.train.cells` where cell_type='GSM'
    union all
    select *, 'VALIDATE' as data_type from `shl-2021.validate.cells` where cell_type='GSM'
    ) cells
),
cells_wcdma as (
    SELECT
        cast(round(epoch_time, -3) as int64) as epoch_time_id,

        cells.*,

        PERCENTILE_DISC(cells.isRegistered, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_isRegistered_median,
        PERCENTILE_DISC(cells.MCC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MCC_median,
        PERCENTILE_DISC(cells.MNC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_MNC_median,
        PERCENTILE_DISC(cells.ci, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_ci_cid_median,
        PERCENTILE_DISC(cells.TAC, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_TAC_LAC_median,
        PERCENTILE_DISC(cells.PCI, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_PCI_PSC_median,
        PERCENTILE_DISC(cells.dBm, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_dBm_median,
        PERCENTILE_DISC(cells.asuLevel, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_asuLevel_median,
        PERCENTILE_DISC(cells.level, 0.5) OVER(PARTITION BY cast(round(epoch_time, -3) as int64)) as cell_level_median,
    from (
        select *, 'TRAIN' as data_type from `shl-2021.train.cells` where cell_type='WCDMA'
        union all
        select *, 'TEST' as data_type from `shl-2021.train.cells` where cell_type='WCDMA'
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.validate.cells` where cell_type='WCDMA'
    ) cells
),
cell_agg as (
    SELECT
        label.data_type,
        label.epoch_time as epoch_time_id,

        -- Cells aggregated
        max(if(cells_lte.cell_type is not null, 1, 0)) as lte_available,
        SUM(if(cells_lte.cell_type is not null, 1, 0)) as lte_count,
        min(cells_lte.isRegistered) as lte_isRegistered_min,
        max(cells_lte.isRegistered) as lte_isRegistered_max,
        min(cells_lte.MCC) as lte_MCC_min,
        max(cells_lte.MCC) as lte_MCC_max,
        min(cells_lte.MNC) as lte_MNC_min,
        max(cells_lte.MNC) as lte_MNC_max,
        min(cells_lte.ci) as lte_ci_cid_min,
        max(cells_lte.ci) as lte_ci_cid_max,
        min(cells_lte.TAC) as lte_TAC_LAC_min,
        max(cells_lte.TAC) as lte_TAC_LAC_max,
        min(cells_lte.PCI) as lte_PCI_PSC_min,
        max(cells_lte.PCI) as lte_PCI_PSC_max,
        min(cells_lte.asuLevel) as lte_asuLevel_min,
        max(cells_lte.asuLevel) as lte_asuLevel_max,
        avg(cells_lte.asuLevel) as lte_asuLevel_avg,
        min(cells_lte.dBm) as lte_dBm_min,
        max(cells_lte.dBm) as lte_dBm_max,
        avg(cells_lte.dBm) as lte_dBm_avg,
        min(cells_lte.level) as lte_level_min,
        max(cells_lte.level) as lte_level_max,
        avg(cells_lte.level) as lte_level_avg,

        max(if(cells_gsm.cell_type is not null, 1, 0)) as gsm_available,
        SUM(if(cells_gsm.cell_type is not null, 1, 0)) as gsm_count,
        min(cells_gsm.isRegistered) as gsm_isRegistered_min,
        max(cells_gsm.isRegistered) as gsm_isRegistered_max,
        min(cells_gsm.MCC) as gsm_MCC_min,
        max(cells_gsm.MCC) as gsm_MCC_max,
        min(cells_gsm.MNC) as gsm_MNC_min,
        max(cells_gsm.MNC) as gsm_MNC_max,
        min(cells_gsm.ci) as gsm_ci_cid_min,
        max(cells_gsm.ci) as gsm_ci_cid_max,
        min(cells_gsm.TAC) as gsm_TAC_LAC_min,
        max(cells_gsm.TAC) as gsm_TAC_LAC_max,
        min(cells_gsm.PCI) as gsm_PCI_PSC_min,
        max(cells_gsm.PCI) as gsm_PCI_PSC_max,
        min(cells_gsm.asuLevel) as gsm_asuLevel_min,
        max(cells_gsm.asuLevel) as gsm_asuLevel_max,
        avg(cells_gsm.asuLevel) as gsm_asuLevel_avg,
        min(cells_gsm.dBm) as gsm_dBm_min,
        max(cells_gsm.dBm) as gsm_dBm_max,
        avg(cells_gsm.dBm) as gsm_dBm_avg,
        min(cells_gsm.level) as gsm_level_min,
        max(cells_gsm.level) as gsm_level_max,
        avg(cells_gsm.level) as gsm_level_avg,

        max(if(cells_wcdma.cell_type is not null, 1, 0)) as wcdma_available,
        SUM(if(cells_wcdma.cell_type is not null, 1, 0)) as wcdma_count,
        min(cells_wcdma.isRegistered) as wcdma_isRegistered_min,
        max(cells_wcdma.isRegistered) as wcdma_isRegistered_max,
        min(cells_wcdma.MCC) as wcdma_MCC_min,
        max(cells_wcdma.MCC) as wcdma_MCC_max,
        min(cells_wcdma.MNC) as wcdma_MNC_min,
        max(cells_wcdma.MNC) as wcdma_MNC_max,
        min(cells_wcdma.ci) as wcdma_ci_cid_min,
        max(cells_wcdma.ci) as wcdma_ci_cid_max,
        min(cells_wcdma.TAC) as wcdma_TAC_LAC_min,
        max(cells_wcdma.TAC) as wcdma_TAC_LAC_max,
        min(cells_wcdma.PCI) as wcdma_PCI_PSC_min,
        max(cells_wcdma.PCI) as wcdma_PCI_PSC_max,
        min(cells_wcdma.asuLevel) as wcdma_asuLevel_min,
        max(cells_wcdma.asuLevel) as wcdma_asuLevel_max,
        avg(cells_wcdma.asuLevel) as wcdma_asuLevel_avg,
        min(cells_wcdma.dBm) as wcdma_dBm_min,
        max(cells_wcdma.dBm) as wcdma_dBm_max,
        avg(cells_wcdma.dBm) as wcdma_dBm_avg,
        min(cells_wcdma.level) as wcdma_level_min,
        max(cells_wcdma.level) as wcdma_level_max,
        avg(cells_wcdma.level) as wcdma_level_avg,

        avg(cells_lte.cell_ci_cid_median) as lte_ci_cid_median,
        avg(cells_lte.cell_MCC_median) as lte_MCC_median,
        avg(cells_lte.cell_MNC_median) as lte_MNC_median,
        avg(cells_lte.cell_PCI_PSC_median) as lte_PCI_PSC_median,
        avg(cells_lte.cell_TAC_LAC_median) as lte_TAC_LAC_median,
        avg(cells_lte.cell_isRegistered_median) as lte_isRegistered_median,
        avg(cells_lte.cell_dBm_median) as lte_dBm_median,
        avg(cells_lte.cell_asuLevel_median) as lte_asuLevel_median,
        avg(cells_lte.cell_level_median) as lte_level_median,

        avg(cells_gsm.cell_ci_cid_median) as gsm_ci_cid_median,
        avg(cells_gsm.cell_MCC_median) as gsm_MCC_median,
        avg(cells_gsm.cell_MNC_median) as gsm_MNC_median,
        avg(cells_gsm.cell_PCI_PSC_median) as gsm_PCI_PSC_median,
        avg(cells_gsm.cell_TAC_LAC_median) as gsm_TAC_LAC_median,
        avg(cells_gsm.cell_isRegistered_median) as gsm_isRegistered_median,
        avg(cells_gsm.cell_dBm_median) as gsm_dBm_median,
        avg(cells_gsm.cell_asuLevel_median) as gsm_asuLevel_median,
        avg(cells_gsm.cell_level_median) as gsm_level_median,

        avg(cells_wcdma.cell_ci_cid_median) as wcdma_ci_cid_median,
        avg(cells_wcdma.cell_MCC_median) as wcdma_MCC_median,
        avg(cells_wcdma.cell_MNC_median) as wcdma_MNC_median,
        avg(cells_wcdma.cell_PCI_PSC_median) as wcdma_PCI_PSC_median,
        avg(cells_wcdma.cell_TAC_LAC_median) as wcdma_TAC_LAC_median,
        avg(cells_wcdma.cell_isRegistered_median) as wcdma_isRegistered_median,
        avg(cells_wcdma.cell_dBm_median) as wcdma_dBm_median,
        avg(cells_wcdma.cell_asuLevel_median) as wcdma_asuLevel_median,
        avg(cells_wcdma.cell_level_median) as wcdma_level_median,
    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.label_train`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.label_test`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.label`
    ) label
    LEFT JOIN cells_lte ON cells_lte.epoch_time_id = label.epoch_time and cells_lte.data_type = label.data_type
    LEFT JOIN cells_gsm ON cells_gsm.epoch_time_id = label.epoch_time and cells_gsm.data_type = label.data_type
    LEFT JOIN cells_wcdma ON cells_wcdma.epoch_time_id = label.epoch_time and cells_wcdma.data_type = label.data_type
    GROUP BY data_type, epoch_time_id
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
