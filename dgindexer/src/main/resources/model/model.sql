SELECT EXISTS(SELECT 1 from pg_database WHERE datname='dg_database');
CREATE DATABASE dg_database;
\c dg_database;

SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dg_metadata');
CREATE SCHEMA dg_metadata;

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

-- status = 0 leyendo de data_asset, 1 = leyendo key_data_asset, 2 = key, 3 = business_assets_data_asset, 4 = business_assets
CREATE TABLE IF NOT EXISTS dg_metadata.partial_indexation_state (
    id SMALLINT NOT NULL UNIQUE,
    last_read_data_asset TIMESTAMP,
    last_read_key_data_asset TIMESTAMP,
    last_read_key TIMESTAMP,
		last_read_business_assets_data_asset TIMESTAMP,
		last_read_business_assets TIMESTAMP,
		CONSTRAINT pk_data_asset_last_ingested PRIMARY KEY (id)
);

-- status = 0 leyendo de data_asset, 1 = leyendo key_data_asset, 2 = key, 3 = business_assets_data_asset, 4 = business_assets
CREATE TABLE IF NOT EXISTS dg_metadata.total_indexation_state (
    id SMALLINT NOT NULL UNIQUE,
    last_read_data_asset TIMESTAMP,
    CONSTRAINT pk_data_asset_last_ingested PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dg_metadata.key (
    id SERIAL,
    key TEXT NOT NULL,
    description TEXT,
    status BOOLEAN NOT NULL,
    tenant TEXT NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_key PRIMARY KEY (id),
    CONSTRAINT u_key_key_tenant UNIQUE (key, tenant)
);

CREATE TABLE IF NOT EXISTS dg_metadata.key_data_asset (
    id SERIAL,
    value TEXT NOT NULL,
    key_id INTEGER NOT NULL,
    data_asset_id INTEGER NOT NULL,
    tenant TEXT NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_key_value PRIMARY KEY (id),
    CONSTRAINT fk_key_value_key FOREIGN KEY (key_id) REFERENCES dg_metadata.key(id) ON DELETE CASCADE,
    CONSTRAINT fk_key_value_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dg_metadata.community (
    id SERIAL,
    name TEXT NOT NULL,
    description TEXT,
    tenant TEXT NOT NULL,
    CONSTRAINT pk_community PRIMARY KEY(id),
    CONSTRAINT u_community_name_tenant UNIQUE (name, tenant)
);

    id SERIAL,
CREATE TABLE IF NOT EXISTS dg_metadata.domain (
    name TEXT NOT NULL,
    description TEXT,
    community_id INTEGER NOT NULL,
    tenant TEXT NOT NULL,
    CONSTRAINT pk_domain PRIMARY KEY(id),
    CONSTRAINT u_domain_name_community_id UNIQUE (name, community_id),
    CONSTRAINT fk_domain_community FOREIGN KEY (community_id) REFERENCES dg_metadata.community(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_type (
    id SERIAL,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    properties jsonb,
    CONSTRAINT pk_business_assets_type PRIMARY KEY(id)
);

INSERT INTO dg_metadata.business_assets_type (name, description, properties) VALUES ('TERM', 'Business term', '{"description":"string","examples":"string"}');
--INSERT INTO dg_metadata.business_assets_type (name, description, properties) VALUES ('QR', 'Quality rules', '{"volumetria":"integer","regex":"string"}');

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_status (
    id SERIAL,
    name TEXT NOT NULL,
    description TEXT,
    CONSTRAINT pk_business_assets_status PRIMARY KEY(id)
);

INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Approved', 'APR');
INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Pending', 'PEN');
INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Under review', 'UNR');

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets (
    id SERIAL,
    name TEXT NOT NULL,
    description TEXT,
    properties jsonb,
    tenant TEXT NOT NULL,
    business_assets_type_id INTEGER NOT NULL,
    domain_id INTEGER NOT NULL,
    business_assets_status_id INTEGER NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_business_assets PRIMARY KEY(id),
    CONSTRAINT fk_business_assets_business_assets_type FOREIGN KEY (business_assets_type_id) REFERENCES dg_metadata.business_assets_type(id),
    CONSTRAINT fk_business_assets_domain FOREIGN KEY (domain_id) REFERENCES dg_metadata.domain(id) ON DELETE CASCADE,
    CONSTRAINT fk_business_assets_business_assets_status FOREIGN KEY (business_assets_status_id) REFERENCES dg_metadata.business_assets_status(id),
    CONSTRAINT u_business_assets_name_domain_id UNIQUE (name, domain_id)
);

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_data_asset (
    id SERIAL,
    name TEXT NOT NULL,
    description TEXT,
    tenant TEXT NOT NULL,
    data_asset_id INTEGER NOT NULL,
    business_assets_id INTEGER NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_business_assets_data_asset PRIMARY KEY(id),
    CONSTRAINT fk_business_assets_business_assets_id_business_assets FOREIGN KEY (business_assets_id) REFERENCES dg_metadata.business_assets(id) ON DELETE CASCADE,
    CONSTRAINT fk_business_assets_data_asset_id_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE,
    CONSTRAINT u_business_assets_data_asset_data_asset_id_business_assets_id UNIQUE (data_asset_id, business_assets_id)
);




