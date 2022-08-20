---CREATE DATABASE ipfs_content_location;

Create TABLE requests (
                          timestamp TIMESTAMP not null,
                          cid VARCHAR(100) not null,
                          continent char(2),
                          country char(2),
                          lat float,
                          long float,
                          request_time float,
                          upstream_time float,
                          body_bytes int,
                          user_agent text,
                          cache text
);


Create TABLE providers (
                           timestamp TIMESTAMP not null,
                           cid VARCHAR(100) not null,
                           continent char(2),
                           country char(2),
                           lat float,
                           long float,
                           request_time float,
                           peerID varchar(100),
                           request_at timestamp
)