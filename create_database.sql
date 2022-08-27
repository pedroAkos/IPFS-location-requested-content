CREATE DATABASE ipfs_content_location;
\c ipfs_content_location

Create TABLE requests (
                          req_id bytea primary key,
                          timestamp TIMESTAMP not null,
                          cid VARCHAR(100) not null,
                          continent char(2),
                          country char(2),
                          region varchar(5),
                          lat float,
                          long float,
                          asn int,
                          aso text,
                          request_time float,
                          upstream_time float,
                          body_bytes bigint,
                          user_agent text,
                          cache text
);


Create TABLE providers (
                           prov_id bytea primary key,
                           timestamp TIMESTAMP not null,
                           cid VARCHAR(100) not null,
                           continent char(2),
                           country char(2),
                           region varchar(5),
                           lat float,
                           long float,
                           asn int,
                           aso text,
                           request_time float,
                           peerID varchar(100),
                           request_at timestamp,
                           req_id bytea,
                           constraint req_id foreign key(req_id) references requests
);

create index requests_timestamp_idx  on requests(timestamp);
create index providers_timestamp_idx  on providers(timestamp);