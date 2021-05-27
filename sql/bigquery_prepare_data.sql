-- Raw data
with cells as (
    SELECT
        cast(round(epoch_time, -3) as int64) as epoch_time_id,

        cells.*,

        if(cells.cell_type is not null, 1, 0) as cell_available,
        if(cells.cell_type = 'LTE', 1, 0) as lte_available,
        if(cells.cell_type = 'GSM', 1, 0) as gsm_available,
        if(cells.cell_type = 'WCDMA', 1, 0) as wcdma_available,

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
        select *, 'TRAIN' as data_type from `shl-2021.train.cells`
        union all
        select *, 'TEST' as data_type from `shl-2021.train.cells`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.validate.cells`
    ) cells
),
cell_agg as (
    SELECT
        cells.data_type,
        epoch_time_id,

        -- Cells aggregated
        SUM(cell_available) as cell_count,
        max(cell_available) as cell_available_max,
        max(gsm_available) as gsm_available_max,
        SUM(gsm_available) as gsm_count,
        max(lte_available) as lte_available_max,
        SUM(lte_available) as lte_count,
        max(wcdma_available) as wcdma_available_max,
        SUM(wcdma_available) as wcdma_count,

        min(isRegistered) as cell_isRegistered_min,
        max(isRegistered) as cell_isRegistered_max,
        min(MCC) as cell_MCC_min,
        max(MCC) as cell_MCC_max,
        min(MNC) as cell_MNC_min,
        max(MNC) as cell_MNC_max,
        min(ci) as cell_ci_cid_min,
        max(ci) as cell_ci_cid_max,
        min(TAC) as cell_TAC_LAC_min,
        max(TAC) as cell_TAC_LAC_max,
        min(PCI) as cell_PCI_PSC_min,
        max(PCI) as cell_PCI_PSC_max,
        min(asuLevel) as cell_asuLevel_min,
        max(asuLevel) as cell_asuLevel_max,
        avg(asuLevel) as cell_asuLevel_avg,
        min(dBm) as cell_dBm_min,
        max(dBm) as cell_dBm_max,
        avg(dBm) as cell_dBm_avg,
        min(level) as cell_level_min,
        max(level) as cell_level_max,
        avg(level) as cell_level_avg,

        avg(cell_ci_cid_median) as cell_ci_cid_median,
        avg(cell_MCC_median) as cell_MCC_median,
        avg(cell_MNC_median) as cell_MNC_median,
        avg(cell_PCI_PSC_median) as cell_PCI_PSC_median,
        avg(cell_TAC_LAC_median) as cell_TAC_LAC_median,

        avg(cell_isRegistered_median) as cell_isRegistered_median,
        avg(cell_dBm_median) as cell_dBm_median,
        avg(cell_asuLevel_median) as cell_asuLevel_median,
        avg(cell_level_median) as cell_level_median,
    FROM cells
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
        features_location.* EXCEPT (data_type, epoch_time),
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
