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
                          cache text,
                          status int
);


Create TABLE providers (
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
                           found_at timestamp,
                           updated_at timestamp,
                           primary key (cid, peerID)
);

create index requests_timestamp_idx  on requests(timestamp);
