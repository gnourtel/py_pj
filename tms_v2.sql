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
		-- shipped timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_package_stationed_in',
				'domestic_package_stationed_out',
				'domestic_pickup/sign_in_success',
				'domestic_sc_sign_in_success'
			)
	) as "first_attempt_timestamp",
	-- min first attempt timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh' at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS in(
				'domestic_on_hold',
				'domestic_out_for_delivery',
				'domestic_first_attempt_failed',
				'domestic_delivered',
				'domestic_failed_delivery',
				'domestic_redelivery',
				'domestic_reattempts_failed'
			)
	) as "first_attempt_timestamp",
	-- min delivered_timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_delivered'
	) as "delivered_timestamp",
	-- min delivery_failed timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_delivery_failed'
	) as "failed_delivery_timestamp",
	-- min package_returned timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_package_returned'
	) as "package_returned_timestamp",
	-- min shipper_received_timestamp
	(
		select
			min( psh.updated_at )::timestamp at time zone 'Asia/Ho_Chi_Minh'
		from
			public.package_status_history psh
		where
			psh.package_id = pck."id"
			and psh.STATUS = 'domestic_shipper_received'
	) as "shipper_received_timestamp"

from
	public.packages as pck
where
	pck.updated_at > current_timestamp - interval '1 hour'
	and pck.platform_name in(
		'LAZADA_VN',
		'OMS_VN'
	);