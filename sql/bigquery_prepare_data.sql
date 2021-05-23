-- Raw data
with cell_data as (
    select data_type,
           cell.cell_type,
           cast(round(cell.epoch_time, -3) as int64) as epoch_time,
           if(cell.cell_type is not null, 1, 0) as cell_available,
           ifnull(cell.isRegistered, 0) as cell_isRegistered,
           cell.MCC as cell_MCC,
           cell.MNC as cell_MNC,
           cell.ci as cell_ci_cid,
           cell.TAC as cell_TAC_LAC,
           cell.PCI as cell_PCI_PSC,
           ifnull(cell.asuLevel, 0) as cell_asuLevel,
           ifnull(cell.dBm, -1000) as cell_dBm,
           ifnull(cell.level, 0) as cell_level,

    from (
        select *, 'TRAIN' as data_type from `shl-2021.train.cells`
        union all
        select *, 'TEST' as data_type from `shl-2021.train.cells`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.validate.cells`
    ) cell
),
raw_data as (

    SELECT  label.data_type,
            label.epoch_time,
            label.label,

            -- Cells
            cells.* EXCEPT (data_type, epoch_time),
            PERCENTILE_DISC(cells.cell_available, 0.5) OVER(PARTITION BY label.epoch_time) as cell_available_median,
            PERCENTILE_DISC(cells.cell_isRegistered, 0.5) OVER(PARTITION BY label.epoch_time) as cell_isRegistered_median,
            PERCENTILE_DISC(cells.cell_MCC, 0.5) OVER(PARTITION BY label.epoch_time) as cell_MCC_median,
            PERCENTILE_DISC(cells.cell_MNC, 0.5) OVER(PARTITION BY label.epoch_time) as cell_MNC_median,
            PERCENTILE_DISC(cells.cell_ci_cid, 0.5) OVER(PARTITION BY label.epoch_time) as cell_ci_cid_median,
            PERCENTILE_DISC(cells.cell_TAC_LAC, 0.5) OVER(PARTITION BY label.epoch_time) as cell_TAC_LAC_median,
            PERCENTILE_DISC(cells.cell_PCI_PSC, 0.5) OVER(PARTITION BY label.epoch_time) as cell_PCI_PSC_median,
            PERCENTILE_DISC(cells.cell_dBm, 0.5) OVER(PARTITION BY label.epoch_time) as cell_dBm_median,
            PERCENTILE_DISC(cells.cell_asuLevel, 0.5) OVER(PARTITION BY label.epoch_time) as cell_asuLevel_median,
            PERCENTILE_DISC(cells.cell_level, 0.5) OVER(PARTITION BY label.epoch_time) as cell_level_median,

            -- Location
            location.Latitude as location_Latitude,
            location.Longitude as location_Longitude,
            location.Altitude as location_Altitude,
            location.accuracy as location_accuracy,
            PERCENTILE_CONT(location.Latitude, 0.5) OVER(PARTITION BY label.epoch_time) as location_Latitude_median,
            PERCENTILE_CONT(location.Longitude, 0.5) OVER(PARTITION BY label.epoch_time) as location_Longitude_median,
            PERCENTILE_CONT(location.Altitude, 0.5) OVER(PARTITION BY label.epoch_time) as location_Altitude_median,
            PERCENTILE_CONT(location.accuracy, 0.5) OVER(PARTITION BY label.epoch_time) as location_accuracy_median,

            -- GPS
            gps.ID as gps_ID,
            gps.Azimuth__degrees_ as gps_Azimuth,
            gps.Elevation__degrees_ as gps_Elevation,
            gps.SNR as gps_SNR,
            PERCENTILE_DISC(gps.ID, 0.5) OVER(PARTITION BY label.epoch_time) as gps_ID_median,
            PERCENTILE_DISC(gps.Azimuth__degrees_, 0.5) OVER(PARTITION BY label.epoch_time) as gps_Azimuth_median,
            PERCENTILE_DISC(gps.Elevation__degrees_, 0.5) OVER(PARTITION BY label.epoch_time) as gps_Elevation_median,
            PERCENTILE_DISC(gps.SNR, 0.5) OVER(PARTITION BY label.epoch_time) as gps_SNR_median,

            -- WiFi
            -- wifi.BSSID as wifi_BSSID,
            wifi.RSSI as wifi_RSSI,
            -- wifi.SSID as wifi_SSID,
            wifi.Frequency__MHz_ as wifi_Frequency,
            wifi.Capabilities as wifi_Capabilities,
            PERCENTILE_DISC(wifi.Frequency__MHz_, 0.5) OVER (PARTITION BY label.epoch_time) as wifi_Frequency_median,
            PERCENTILE_DISC(wifi.RSSI, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_RSSI_median,

            -- Location features
            features_location.accuracy_change,
            features_location.Latitude_change,
            features_location.Longitude_change,
            features_location.Altitude_change,
            features_location.distance,
            features_location.speed,
            features_location.vertical_speed,
            features_location.direction,
            features_location.vertical_direction,
            features_location.speed_change,
            features_location.vertical_speed_change,
            features_location.direction_change,
            features_location.vertical_direction_change,
            features_location.abs_speed_change,
            features_location.abs_vertical_speed_change,
            features_location.abs_direction_change,
            features_location.abs_vertical_direction_change,
            features_location.accuracy_3_s_window_avg,
            features_location.Latitude_3_s_window_avg,
            features_location.Longitude_3_s_window_avg,
            features_location.Altitude_3_s_window_avg,
            features_location.accuracy_change_3_s_window_avg,
            features_location.Latitude_change_3_s_window_avg,
            features_location.Longitude_change_3_s_window_avg,
            features_location.Altitude_change_3_s_window_avg,
            features_location.distance_3_s_window_avg,
            features_location.speed_3_s_window_avg,
            features_location.vertical_speed_3_s_window_avg,
            features_location.direction_3_s_window_avg,
            features_location.vertical_direction_3_s_window_avg,
            features_location.speed_change_3_s_window_avg,
            features_location.vertical_speed_change_3_s_window_avg,
            features_location.direction_change_3_s_window_avg,
            features_location.vertical_direction_change_3_s_window_avg,
            features_location.abs_speed_change_3_s_window_avg,
            features_location.abs_vertical_speed_change_3_s_window_avg,
            features_location.abs_direction_change_3_s_window_avg,
            features_location.abs_vertical_direction_change_3_s_window_avg

    FROM (
        select *, 'TRAIN' as data_type from `shl-2021.train.label_train`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.label_test`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.label`
    ) label

    left join cell_data as cells on cells.epoch_time = label.epoch_time and cells.data_type = label.data_type


    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.location`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.location`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.location`
    ) location on cast(round(location.epoch_time, -3) as int64) = label.epoch_time and location.data_type  = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.gps`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.gps`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.gps`
    ) gps on cast(round(gps.Epoch_time__ms_, -3) as int64) = label.epoch_time and gps.data_type = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.wifi`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.wifi`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.wifi`
    ) wifi on cast(round(wifi.Epoch_time__ms_, -3) as int64) = label.epoch_time and wifi.data_type = label.data_type

    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_denys`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_denys`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_denys`
    ) features_location on features_location.epoch_time = label.epoch_time and features_location.data_type = label.data_type



--where label.epoch_time = 1493282527000
),


aggragated_data as (

    -- Aggregated data
    select
        raw_data.data_type,
        raw_data.epoch_time,
        raw_data.label,

        -- Cells aggregated
        count(cell_available) as cell_count,
        min(cell_available) as cell_available_min,
        max(cell_available) as cell_available_max,

        min(cell_isRegistered) as cell_isRegistered_min,
        max(cell_isRegistered) as cell_isRegistered_max,
        min(cell_MCC) as cell_MCC_min,
        max(cell_MCC) as cell_MCC_max,
        min(cell_MNC) as cell_MNC_min,
        max(cell_MNC) as cell_MNC_max,
        min(cell_ci_cid) as cell_ci_cid_min,
        max(cell_ci_cid) as cell_ci_cid_max,
        min(cell_TAC_LAC) as cell_TAC_LAC_min,
        max(cell_TAC_LAC) as cell_TAC_LAC_max,
        min(cell_PCI_PSC) as cell_PCI_PSC_min,
        max(cell_PCI_PSC) as cell_PCI_PSC_max,
        min(cell_asuLevel) as cell_asuLevel_min,
        max(cell_asuLevel) as cell_asuLevel_max,
        avg(cell_asuLevel) as cell_asuLevel_avg,
        min(cell_dBm) as cell_dBm_min,
        max(cell_dBm) as cell_dBm_max,
        avg(cell_dBm) as cell_dBm_avg,
        min(cell_level) as cell_level_min,
        max(cell_level) as cell_level_max,
        avg(cell_level) as cell_level_avg,

        avg(cell_ci_cid_median) as cell_ci_cid_median,
        avg(cell_MCC_median) as cell_MCC_median,
        avg(cell_MNC_median) as cell_MNC_median,
        avg(cell_PCI_PSC_median) as cell_PCI_PSC_median,
        avg(cell_TAC_LAC_median) as cell_TAC_LAC_median,

        avg(cell_available_median) as cell_available_median,
        avg(cell_isRegistered_median) as cell_isRegistered_median,
        avg(cell_dBm_median) as cell_dBm_median,
        avg(cell_asuLevel_median) as cell_asuLevel_median,
        avg(cell_level_median) as cell_level_median,

        -- Location aggregated
        count(location_Latitude) as location_count,
        min(location_Latitude) as location_Latitude_min,
        max(location_Latitude) as location_Latitude_max,
        avg(location_Latitude) as location_Latitude_avg,
        min(location_Longitude) as location_Longitude_min,
        max(location_Longitude) as location_Longitude_max,
        avg(location_Longitude) as location_Longitude_avg,
        min(location_Altitude) as location_Altitude_min,
        max(location_Altitude) as location_Altitude_max,
        avg(location_Altitude) as location_Altitude_avg,
        min(location_accuracy) as location_accuracy_min,
        max(location_accuracy) as location_accuracy_max,
        avg(location_accuracy) as location_accuracy_avg,
        avg(location_Latitude_median) as location_Latitude_median,
        avg(location_Longitude_median) as location_Longitude_median,
        avg(location_Altitude_median) as location_Altitude_median,
        avg(location_accuracy_median) as location_accuracy_median,

        -- GPS aggregated
        count(gps_Azimuth) as gps_count,
        min(gps_Azimuth) as gps_Azimuth_min,
        max(gps_Azimuth) as gps_Azimuth_max,
        avg(gps_Azimuth) as gps_Azimuth_avg,
        min(gps_Elevation) as gps_Elevation_min,
        max(gps_Elevation) as gps_Elevation_max,
        avg(gps_Elevation) as gps_Elevation_avg,
        min(gps_SNR) as gps_SNR_min,
        max(gps_SNR) as gps_SNR_max,
        avg(gps_SNR) as gps_SNR_avg,
        avg(gps_ID_median) as gps_ID_median,
        avg(gps_Azimuth_median) as gps_Azimuth_median,
        avg(gps_Elevation_median) as gps_Elevation_median,
        avg(gps_SNR_median) as gps_SNR_median,

        -- WiFi aggregated
        count(wifi_RSSI) as wifi_count,
        -- any_value(wifi_BSSID) as wifi_BSSID_any,
        -- any_value(wifi_SSID) as wifi_SSID_any,
        any_value(wifi_Capabilities) as wifi_Capabilities_any,
        min(wifi_Frequency) as wifi_Frequency_min,
        max(wifi_Frequency) as wifi_Frequency_max,
        avg(wifi_Frequency_median) as wifi_Frequency_median,
        min(wifi_RSSI) as wifi_RSSI_min,
        max(wifi_RSSI) as wifi_RSSI_max,
        avg(wifi_RSSI_median) as wifi_RSSI_median,

        -- Location features aggregated
        avg(accuracy_change) as accuracy_change,
        avg(Latitude_change) as Latitude_change,
        avg(Longitude_change) as Longitude_change,
        avg(Altitude_change) as Altitude_change,
        avg(distance) as distance,
        avg(speed) as speed,
        avg(vertical_speed) as vertical_speed,
        avg(direction) as direction,
        avg(vertical_direction) as vertical_direction,
        avg(speed_change) as speed_change,
        avg(vertical_speed_change) as vertical_speed_change,
        avg(direction_change) as direction_change,
        avg(vertical_direction_change) as vertical_direction_change,
        avg(abs_speed_change) as abs_speed_change,
        avg(abs_vertical_speed_change) as abs_vertical_speed_change,
        avg(abs_direction_change) as abs_direction_change,
        avg(abs_vertical_direction_change) as abs_vertical_direction_change,
        avg(accuracy_3_s_window_avg) as accuracy_3_s_window_avg,
        avg(Latitude_3_s_window_avg) as Latitude_3_s_window_avg,
        avg(Longitude_3_s_window_avg) as Longitude_3_s_window_avg,
        avg(Altitude_3_s_window_avg) as Altitude_3_s_window_avg,
        avg(accuracy_change_3_s_window_avg) as accuracy_change_3_s_window_avg,
        avg(Latitude_change_3_s_window_avg) as Latitude_change_3_s_window_avg,
        avg(Longitude_change_3_s_window_avg) as Longitude_change_3_s_window_avg,
        avg(Altitude_change_3_s_window_avg) as Altitude_change_3_s_window_avg,
        avg(distance_3_s_window_avg) as distance_3_s_window_avg,
        avg(speed_3_s_window_avg) as speed_3_s_window_avg,
        avg(vertical_speed_3_s_window_avg) as vertical_speed_3_s_window_avg,
        avg(direction_3_s_window_avg) as direction_3_s_window_avg,
        avg(vertical_direction_3_s_window_avg) as vertical_direction_3_s_window_avg,
        avg(speed_change_3_s_window_avg) as speed_change_3_s_window_avg,
        avg(vertical_speed_change_3_s_window_avg) as vertical_speed_change_3_s_window_avg,
        avg(direction_change_3_s_window_avg) as direction_change_3_s_window_avg,
        avg(vertical_direction_change_3_s_window_avg) as vertical_direction_change_3_s_window_avg,
        avg(abs_speed_change_3_s_window_avg) as abs_speed_change_3_s_window_avg,
        avg(abs_vertical_speed_change_3_s_window_avg) as abs_vertical_speed_change_3_s_window_avg,
        avg(abs_direction_change_3_s_window_avg) as abs_direction_change_3_s_window_avg,
        avg(abs_vertical_direction_change_3_s_window_avg) as abs_vertical_direction_change_3_s_window_avg,

    from raw_data
    group by data_type,
             epoch_time,
             label
),
aggregated_with_features as (
    SELECT

        aggragated_data.*,

        -- WiFi names aggregated
        wifi_names.* EXCEPT (data_type, epoch_time_id)

    FROM aggragated_data
    left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_wifi_names`
    ) wifi_names on wifi_names.epoch_time_id = aggragated_data.epoch_time and wifi_names.data_type = aggragated_data.data_type
)

select * EXCEPT (epoch_time) FROM aggregated_with_features
