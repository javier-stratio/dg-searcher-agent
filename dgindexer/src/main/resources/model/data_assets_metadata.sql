CREATE TABLE IF NOT EXISTS dg_metadata.data_asset_last_ingested (
    id SMALLINT NOT NULL UNIQUE,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_data_asset_last_ingested PRIMARY KEY (id),
);
