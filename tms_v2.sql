select
	pck.id,
	pck.platform_package_id,
	(
		select
			psh.tracking_number
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.tracking_number is not null
		order by
			psh.updated_at desc limit 1
	) as "platform_tracking_id",
	total_price,
	delivery_type,
	payment_type,
	platform_shipper_id as "seller_id",
	pck.created_at::timestamp at time zone 'Asia/Ho_Chi_Minh' as "created_at",
	pck.updated_at::timestamp at time zone 'Asia/Ho_Chi_Minh' as "updated_at",
	(
		select
			re.status
		from
			public.pickup_requests re
		where
			re.package_id = pck."id"
			and re.request_type = 'domestic_first_mile'
		order by
			re.updated_at desc limit 1
	) "first_mile_api_status",
	(
		select
			re.status
		from
			public.pickup_requests re
		where
			re.package_id = pck."id"
			and re.request_type = 'last_mile'
		order by
			re.updated_at desc limit 1
	) "last_mile_api_status",
	pck.STATUS as "current_status",
	pck.type,
	pck.shipper_sender_name,
	pck.shipping_type,
	pck.platform_order_number,
	pck.journey,
	-- Add new 16/10/2017
 -- For TMS failed reasons
(
		select
			psh.reason_code
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.reason_code is not null
		order by
			psh.updated_at desc limit 1
	) as "tms_failed_reason_code",
	-- Add new 16/10/2017
 -- For 3PL failed reasons
(
		select
			psh.tpl_reason_code
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.tpl_reason_code is not null
		order by
			psh.updated_at desc limit 1
	) as "tpl_failed_reason_code",
	-- ready to be shipped timestamp 23/11/2017
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in('package_ready_to_be_shipped')
	) as "ready_to_be_shipped_timestamp",
	-- transit to shipped timestamp 23/11/2017
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_pickup/sign_in_success',
				'domestic_sc_sign_in_success',
				'domestic_ib_success_first_mile_hub',
				'domestic_ob_success_first_mile_hub'
			)
	) as "transit_to_shipped_timestamp",
	-- shipped timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_package_stationed_in',
				'domestic_package_stationed_out',
				'domestic_ob_success_in_sort_center'
			)
	) as "shipped_timestamp",
	-- min first attempt timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_on_hold',
				'domestic_out_for_delivery',
				'domestic_1st_attempt_failed',
				'domestic_delivered',
				'domestic_failed_delivery',
				'domestic_redelivery',
				'domestic_reattempts_failed'
			)
	) as "first_attempt_timestamp",
	-- min delivered_timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_delivered'
	) as "delivered_timestamp",
	-- min delivery_failed timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_delivery_failed'
	) as "failed_delivery_timestamp",
	-- min package_returning timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_return_with_last_mile_3PL',
				'domestic_return_at transit_hub',
				'domestic_back_to_shipper'
			)
	) as "package_returning_timestamp",
	-- min package_returned timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_package_returned'
	) as "package_returned_timestamp",
	-- min shipper_received_timestamp
(
		select
			coalesce(
				min( psh.processed_date ),
				min( psh.updated_at )
			)::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_shipper_received'
	) as "shipper_received_timestamp"
from
	public.packages as pck
where
	pck.updated_at > current_timestamp - interval '2 hour'
	and pck.platform_name in(
		'LAZADA_VN',
		'OMS_VN'
	)
	limit 1000;