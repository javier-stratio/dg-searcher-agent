-- Test Data for the set of test contained in PostgresSourceDatoITTest

-- Data Asset
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, discovered_at, modified_at) values (201,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, discovered_at, modified_at) values (202,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, discovered_at, modified_at) values (203,'R_COMMENT', 'Hdfs parquet column','hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:', 'HDFS', 'FIELD', 'NONE', '{"type": "org.apache.parquet.io.api.Binary", "default": "", "constraint": "", "schemaType": "parquet"}', true, ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));

-- Admin base information
insert into dg_metadata.community (id, name, description, tenant) values (1, 'Marketing', 'Description marketing', 'NONE');
insert into dg_metadata.domain (id, name, description, tenant, community_id) values (1, 'Marketing domain', 'Description marketing domain', 'NONE', 1);
insert into dg_metadata.business_assets (name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values ('Production', 'desc bt1', '{}', 'NONE', 1, 1, 1,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets (name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values ('Client', 'desc bt1', '{}', 'NONE', 1, 1, 1, ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets (name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values ('Department', 'desc bt1', '{}', 'NONE', 1, 1, 1, ('2018-12-10 09:27:17.815'::timestamp));

-- business asset and keyvalues for dataasset 201, 202, 203
insert into dg_metadata.key (key, description, active, tenant, modified_at) values ('OWNER','Owner',true,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key (key, description, active, tenant, modified_at) values ('QUALITY','Quality',true,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, data_asset_id, tenant, modified_at) values ('finance',1,201,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, data_asset_id, tenant, modified_at) values ('Low',2,201,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, data_asset_id, tenant, modified_at) values ('finance2',1,202,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.key_data_asset (value, key_id, data_asset_id, tenant, modified_at) values ('finance3',1,203,'NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, data_asset_id, business_assets_id, modified_at) values ('','','NONE', 201, 1,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, data_asset_id, business_assets_id, modified_at) values ('','','NONE', 201, 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, data_asset_id, business_assets_id, modified_at) values ('','','NONE', 202, 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, data_asset_id, business_assets_id, modified_at) values ('','','NONE', 202, 3,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata.business_assets_data_asset(name, description, tenant, data_asset_id, business_assets_id, modified_at) values ('','','NONE', 203, 3,('2018-12-10 09:27:17.815'::timestamp));
