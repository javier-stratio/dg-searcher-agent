CREATE TABLE IF NOT EXISTS dg_metadata.data_asset(
    id SERIAL,
    name TEXT,
    description TEXT,
    metadata_path TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    tenant TEXT NOT NULL,
    properties jsonb NOT NULL,
    active BOOLEAN NOT NULL,
    discovered_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_data_asset PRIMARY KEY (id),
    CONSTRAINT u_data_asset_meta_data_path_tenant UNIQUE (metadata_path, tenant)
);