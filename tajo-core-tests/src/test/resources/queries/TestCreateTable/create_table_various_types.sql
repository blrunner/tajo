-- Some types were commented out due to Hive meta test.

create table various_types (
  -- col0 bit,
  -- col1 BIT(10),
  -- col2 bit varying,
  -- col3 bit VARYING(10),
  col4 tinyint,
  col5 smallInt,
  col6 integer,
  col7 biginT,
  col8 real,
  col9 float,
  col10 float(53),
  col11 double,
  col12 doublE precision,
  col13 numeric,
  col14 numeric(10),
  col15 numeric(10,2),
  col16 decimal,
  col17 decimal(10),
  col18 decimal(10,2),
  col19 char,
  col20 character,
  col21 chaR(10),
  col22 character(10),
  col23 varchar,
  col24 character varying,
  col25 varchar(255),
  col26 character varying (255),
  col27 nchar,
  col28 nchar(255),
  col29 national character,
  col30 national character(255),
  col31 nvarchar,
  col32 nvarchar(255),
  col33 natIonal character varying,
  col34 national character varying (255),
  col35 date,
  col36 time,
  -- col37 timetz,
  -- col38 time With time zone,
  -- col39 timesTamptz,
  -- col40 timestamp with time zone,
  -- col41 binary,
  -- col42 binary(10),
  -- col43 varbinary(10),
  -- col44 binary Varying(10),
  col45 blOb
);