create table if not exists purchases (
client_id varchar, 
gender varchar, 
purchase_datetime date, 
purchase_time_as_seconds_from_midnight integer, 
product_id varchar, 
quantity integer, 
price_per_item integer, 
discount_per_item integer, 
total_price integer,
CONSTRAINT sales_pkey PRIMARY KEY (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price)
)