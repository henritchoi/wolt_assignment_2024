--

--Creating tables after cleaning up data (removing headers and delimiters)
--Assuming the delivery radius logs come from the Order Management System (OMS)
create table dl_oms.dl_oms_delivery_radius_log  (
    delivery_area_id varchar(24)
    , delivery_radius_meters BIGINT
    , event_started_timestamp timestamp
    );

--Creating hourly periods in a separate reference data-specific SCHEMA
--Changing name to hourly periods as hours would indicate a series of hours
create table dl_reference.dl_ref_hourly_periods  (
    period_start timestamp
    , period_end timestamp
    );

--Assuming the purchases come from the Order Management System
create table dl_oms.dl_oms_purchases  (
    purchase_id varchar(24)
    , time_received timestamp
    , time_delivered timestamp
    , end_amount_with_vat_eur FLOAT8
    , dropoff_distance_straight_line_metres int4
    , delivery_area_id varchar(24)
    );


--Copying into Redshift from S3
COPY  dl_oms.dl_oms_delivery_radius_log (delivery_area_id, delivery_radius_meters, event_started_timestamp)
FROM 's3://woltassignmentdata/delivery_radius_log.csv'
iam_role 'arn:aws:iam::851725459032:role/iam_deng_role'
CSV;

COPY  dl_reference.dl_ref_hours (period_start, period_end)
FROM 's3://woltassignmentdata/hours.csv'
iam_role 'arn:aws:iam::851725459032:role/iam_deng_role'
CSV;    

COPY  dl_oms.dl_oms_purchases (purchase_id, time_received, time_delivered, end_amount_with_vat_eur, dropoff_distance_straight_line_metres, delivery_area_id)
FROM 's3://woltassignmentdata/purchases.csv'
iam_role 'arn:aws:iam::851725459032:role/iam_deng_role'
CSV;   