-- Raw data
with raw_data as (

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
        PERCENTILE_DISC(wifi.Frequency__MHz_, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_Frequency_median,
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
        features_location.abs_vertical_direction_change_3_s_window_avg,

        -- WiFi names
        wifi_names.wifi_0 as wifi_names_0,
        wifi_names.wifi_1 as wifi_names_1,
        wifi_names.wifi_2 as wifi_names_2,
        wifi_names.wifi_3 as wifi_names_3,
        wifi_names.wifi_4 as wifi_names_4,
        wifi_names.wifi_5 as wifi_names_5,
        wifi_names.wifi_6 as wifi_names_6,
        wifi_names.wifi_7 as wifi_names_7,
        wifi_names.wifi_8 as wifi_names_8,
        wifi_names.wifi_9 as wifi_names_9,
        wifi_names.wifi_10 as wifi_names_10,
        wifi_names.wifi_11 as wifi_names_11,
        wifi_names.wifi_12 as wifi_names_12,
        wifi_names.wifi_13 as wifi_names_13,
        wifi_names.wifi_14 as wifi_names_14,
        wifi_names.wifi_15 as wifi_names_15,
        wifi_names.wifi_16 as wifi_names_16,
        wifi_names.wifi_17 as wifi_names_17,
        wifi_names.wifi_18 as wifi_names_18,
        wifi_names.wifi_19 as wifi_names_19,
        wifi_names.wifi_20 as wifi_names_20,
        wifi_names.wifi_21 as wifi_names_21,
        wifi_names.wifi_22 as wifi_names_22,
        wifi_names.wifi_23 as wifi_names_23,
        wifi_names.wifi_24 as wifi_names_24,
        wifi_names.wifi_25 as wifi_names_25,
        wifi_names.wifi_26 as wifi_names_26,
        wifi_names.wifi_27 as wifi_names_27,
        wifi_names.wifi_28 as wifi_names_28,
        wifi_names.wifi_29 as wifi_names_29,
        PERCENTILE_DISC(wifi_names.wifi_0, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_0_median,
        PERCENTILE_DISC(wifi_names.wifi_1, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_1_median,
        PERCENTILE_DISC(wifi_names.wifi_2, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_2_median,
        PERCENTILE_DISC(wifi_names.wifi_3, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_3_median,
        PERCENTILE_DISC(wifi_names.wifi_4, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_4_median,
        PERCENTILE_DISC(wifi_names.wifi_5, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_5_median,
        PERCENTILE_DISC(wifi_names.wifi_6, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_6_median,
        PERCENTILE_DISC(wifi_names.wifi_7, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_7_median,
        PERCENTILE_DISC(wifi_names.wifi_8, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_8_median,
        PERCENTILE_DISC(wifi_names.wifi_9, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_9_median,
        PERCENTILE_DISC(wifi_names.wifi_10, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_10_median,
        PERCENTILE_DISC(wifi_names.wifi_11, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_11_median,
        PERCENTILE_DISC(wifi_names.wifi_12, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_12_median,
        PERCENTILE_DISC(wifi_names.wifi_13, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_13_median,
        PERCENTILE_DISC(wifi_names.wifi_14, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_14_median,
        PERCENTILE_DISC(wifi_names.wifi_15, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_15_median,
        PERCENTILE_DISC(wifi_names.wifi_16, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_16_median,
        PERCENTILE_DISC(wifi_names.wifi_17, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_17_median,
        PERCENTILE_DISC(wifi_names.wifi_18, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_18_median,
        PERCENTILE_DISC(wifi_names.wifi_19, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_19_median,
        PERCENTILE_DISC(wifi_names.wifi_20, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_20_median,
        PERCENTILE_DISC(wifi_names.wifi_21, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_21_median,
        PERCENTILE_DISC(wifi_names.wifi_22, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_22_median,
        PERCENTILE_DISC(wifi_names.wifi_23, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_23_median,
        PERCENTILE_DISC(wifi_names.wifi_24, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_24_median,
        PERCENTILE_DISC(wifi_names.wifi_25, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_25_median,
        PERCENTILE_DISC(wifi_names.wifi_26, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_26_median,
        PERCENTILE_DISC(wifi_names.wifi_27, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_27_median,
        PERCENTILE_DISC(wifi_names.wifi_28, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_28_median,
        PERCENTILE_DISC(wifi_names.wifi_29, 0.5) OVER(PARTITION BY label.epoch_time) as wifi_names_29_median,


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

left join (
        select *, 'TRAIN' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'VALIDATE' as data_type from `shl-2021.train.features_wifi_names`
        union all
        select *, 'TEST' as data_type from `shl-2021.validate.features_wifi_names`
        ) wifi_names on wifi_names.epoch_time_id = label.epoch_time and wifi_names.data_type = label.data_type



--where label.epoch_time = 1493282527000
),


aggragated_data as (

-- Aggregated data
select  data_type,
        epoch_time,
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
        avg(abs_vertical_direction_change_3_s_window_avg) as abs_vertical_direction_change_3_s_window_avg

        -- WiFi names aggregated


from raw_data
group by data_type,
         epoch_time,
         label
)

select * EXCEPT (epoch_time) FROM aggragated_data
