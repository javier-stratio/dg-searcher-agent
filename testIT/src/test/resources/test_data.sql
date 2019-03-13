-- Test Data for the set of test contained in PostgresSourceDaoITTest

-- Data Asset
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (201,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (202,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (203,'R_COMMENT', 'Hdfs parquet column','hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:', 'HDFS', 'FIELD', 'NONE', '{"type": "org.apache.parquet.io.api.Binary", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));

-- Admin base information
insert into dg_metadata.community (id, name, description, tenant) values (1, 'Marketing', 'Description marketing', 'NONE');
insert into dg_metadata.domain (id, name, description, tenant, community_id) values (1, 'Marketing domain', 'Description marketing domain', 'NONE', 1);
insert into dg_metadata.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (1,'Production', 'desc bt1', '{}', 'NONE', 1, 1, 1,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (2,'Client', 'desc bt1', '{}', 'NONE', 1, 1, 1, ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (3,'Department', 'desc bt1', '{}', 'NONE', 1, 1, 1, ('2018-12-10 09:27:17.815'::timestamp));

-- business asset and keyvalues for dataasset 201, 202, 203
insert into dg_metadata.key (id, key, description, active, tenant, value_regexp, modified_at) values (1, 'OWNER','Owner',true,'NONE','{"type":"STRING","regexp":".*"}',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key (id, key, description, active, tenant, value_regexp, modified_at) values (2, 'QUALITY','Quality',true,'NONE','{"type":"STRING","regexp":".*"}',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('finance',1,'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('Low',2,'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('finance2',1,'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('finance3',1,'hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 1,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 3,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:', 3,('2018-12-10 09:27:17.815'::timestamp));
