import config as c
import psycopg2 

conn = psycopg2.connect(database=c.db_name, 
                            user=c.user_name, 
                            password=c.password, 
                            host=c.db_host)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE if not exists staging.orders (
	row_id numeric(20, 4) NOT NULL, -- Ключ
	order_id varchar(14) NOT NULL,
	order_date date NOT NULL,
	ship_date date NOT NULL,
	ship_mode varchar(14) NOT NULL,
	customer_id varchar(8) NOT NULL,
	customer_name varchar(22) NOT NULL,
	segment varchar(11) NOT NULL,
	country varchar(13) NOT NULL,
	city varchar(17) NOT NULL,
	state varchar(20) NOT NULL,
	postal_code numeric(20, 4) NULL,
	region varchar(7) NOT NULL,
	product_id varchar(15) NOT NULL,
	category varchar(15) NOT NULL,
	subcategory varchar(11) NOT NULL,
	product_name varchar(127) NOT NULL,
	sales numeric(20, 4) NOT NULL,
	quantity numeric(20, 4) NOT NULL,
	discount numeric(20, 4) NOT NULL,
	profit numeric(20, 4) NOT NULL,
	persone text NULL,
	returned text NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (row_id)
);

-- Column comments

COMMENT ON COLUMN staging.orders.row_id IS 'Ключ';
""")


cursor.execute("""
            create table if not exists staging.people(
               person VARCHAR(17) NOT NULL PRIMARY KEY,
               region VARCHAR(7) NOT NULL
               );
               """)

cursor.execute("""
            create table if not exists staging.returns(
                returned VARCHAR(3) NOT NULL, 
            	order_id VARCHAR(14) NOT NULL PRIMARY KEY);
               """)

cursor.execute("comment on column staging.people.person is 'Ключ'")

cursor.execute("comment on column staging.orders.row_id is 'Ключ'")

cursor.execute("comment on column staging.returns.order_id is 'Ключ'")


conn.commit()
cursor.close()
conn.close()

