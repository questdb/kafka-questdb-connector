create table if not exists stock (
    id serial primary key,
    symbol varchar(10) unique,
    price float8,
    last_update timestamp
);
insert into stock (symbol, price, last_update) values ('AAPL', 500.0, now());
insert into stock (symbol, price, last_update) values ('IBM', 50.0, now());
insert into stock (symbol, price, last_update) values ('MSFT', 100.0, now());
insert into stock (symbol, price, last_update) values ('GOOG', 1000.0, now());
insert into stock (symbol, price, last_update) values ('FB', 200.0, now());
insert into stock (symbol, price, last_update) values ('AMZN', 1000.0, now());
insert into stock (symbol, price, last_update) values ('TSLA', 500.0, now());
insert into stock (symbol, price, last_update) values ('NFLX', 500.0, now());
insert into stock (symbol, price, last_update) values ('TWTR', 50.0, now());
insert into stock (symbol, price, last_update) values ('SNAP', 10.0, now());