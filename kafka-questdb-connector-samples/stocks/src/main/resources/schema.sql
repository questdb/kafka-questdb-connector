create table if not exists stock (
    id serial primary key,
    symbol varchar(10) unique,
    price float8
);
insert into stock (symbol, price) values ('AAPL', 500.0);
insert into stock (symbol, price) values ('IBM', 50.0);
insert into stock (symbol, price) values ('MSFT', 100.0);
insert into stock (symbol, price) values ('GOOG', 1000.0);
insert into stock (symbol, price) values ('FB', 200.0);
insert into stock (symbol, price) values ('AMZN', 1000.0);
insert into stock (symbol, price) values ('TSLA', 500.0);
insert into stock (symbol, price) values ('NFLX', 500.0);
insert into stock (symbol, price) values ('TWTR', 50.0);
insert into stock (symbol, price) values ('SNAP', 10.0);