drop table if exists product;
create table product (
    id    binary(16)       not null,
    date  char(10)         not null, -- yyyy-MM-dd
    price bigint default 0 not null,
    constraint product_pk
        primary key (id)
);

create index product_date_idx
    on product(date);

drop table if exists product_monthly;
create table product_monthly (
    id    binary(16)       not null,
    month char(7)          not null, -- yyyy-MM
    price bigint default 0 not null,
    constraint product_monthly_pk
        primary key (id)
);




