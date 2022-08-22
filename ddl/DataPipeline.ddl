CREATE TABLE `clickstream_data`(
`event_id` INT NOT NULL,
`event_timestamp` TIMESTAMP NOT NULL,
`user_device_type` VARCHAR(10),
`session_id` VARCHAR(10) NOT NULL,
`visitor_id` VARCHAR(10) NOT NULL,
`redirection_source` VARCHAR(15),
`is_add_to_cart` BOOLEAN,
`is_order_placed` BOOLEAN,
`item_id`  VARCHAR(10) NOT NULL,
`item_price` DOUBLE,
`product_type`  VARCHAR(15),
`department_name`  VARCHAR(20),
`vendor_id` INT NOT NULL,
`vendor_name`  VARCHAR(20)
);
