-- Test Data for the set of test contained in PostgresSourceDaoITTest

-- Data Asset
insert into dg_metadata_test.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (201,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (202,'R_REGIONKEY','Hdfs parquet column','hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 'HDFS', 'FIELD', 'NONE', '{"type": "long", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.data_asset (id, name, description, metadata_path, type, subtype, tenant, properties, active, vendor, discovered_at, modified_at) values (203,'R_COMMENT', 'Hdfs parquet column','hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:', 'HDFS', 'FIELD', 'NONE', '{"type": "org.apache.parquet.io.api.Binary", "default": "", "constraint": "", "schemaType": "parquet"}', true, 'PostgreSQL' , ('2018-12-10 09:27:17.815'::timestamp), ('2018-12-10 09:27:17.815'::timestamp));

-- Admin base information
insert into dg_metadata_test.community (id, name, description, tenant, modified_at) values (1, 'Marketing', 'Description marketing', 'NONE', current_timestamp::timestamp);
insert into dg_metadata_test.domain (id, name, description, tenant, community_id, modified_at) values (1, 'Marketing domain', 'Description marketing domain', 'NONE', 1, current_timestamp::timestamp);
INSERT INTO dg_metadata_test.bpm_flow (id, "name",description,entity_type_id,tenant,created_at,modified_at,user_id) VALUES
(1, 'Defalut BT Flow','Default Businsess Terms state flow',1,'NONE','2019-07-30 06:49:55.078','2019-07-30 06:49:55.078','admin')
;
INSERT INTO dg_metadata_test.bpm_status (id, "name",description,"action",flow_id,"type",active,colour,tenant,created_at,modified_at,user_id) VALUES
(1, 'Draft','First status when creating a Business asset. In this step you work on the asset definition.',NULL,1,'INITIAL',false,'#128bdd','NONE','2019-07-29 11:33:23.150','2019-07-29 11:33:23.150','admin')
,(2, 'Under Review','Status where the term is reviewed exhaustively and then you can ask for modifications or then go to Publish the asset.',NULL,1,'TRANSITIONABLE',false,'#ec445c','NONE','2019-07-29 11:33:23.150','2019-07-29 11:33:23.150','admin')
,(3, 'Need Modifications','The Data Steward tells to make modifications to the asset to be valid and be published.',NULL,1,'TRANSITIONABLE',false,'#fa9330','NONE','2019-07-29 11:33:23.150','2019-07-29 11:33:23.150','admin')
,(4, 'Published','The asset is published and visible for everybody.',NULL,1,'FINAL',true,'#2cce93','NONE','2019-07-29 11:33:23.150','2019-07-29 11:33:23.150','admin')
,(5, 'Rejected','The term does not meet the needs of the Business Glossary and therefore is Rejected.',NULL,1,'FINAL',false,'#0f1b27','NONE','2019-07-29 11:33:23.150','2019-07-29 11:33:23.150','admin')
;
insert into dg_metadata_test.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (1,'Production', 'desc bt1', '{}', 'NONE', 1, 1, 4,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (2,'Client', 'desc bt1', '{}', 'NONE', 1, 1, 4, ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (3,'Department', 'desc bt1', '{}', 'NONE', 1, 1, 4, ('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets (id, name, description, properties, tenant, business_assets_type_id, domain_id, business_assets_status_id, modified_at) values (4,'Test', 'desc bt1', '{}', 'NONE', 1, 1, 1, ('2018-12-10 09:27:17.815'::timestamp));

-- business asset and keyvalues for dataasset 201, 202, 203
insert into dg_metadata_test.key (id, key, description, active, tenant, value_design, modified_at) values (1, 'OWNER','Owner',true,'NONE','{"type":"STRING","regexp":".*"}',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.key (id, key, description, active, tenant, value_design, modified_at) values (2, 'QUALITY','Quality',true,'NONE','{"type":"STRING","regexp":".*"}',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('{"name":"","value":"finance"}'::jsonb,1,'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('{"name":"","value":"Low"}'::jsonb,2,'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('{"name":"","value":"finance2"}'::jsonb,1,'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.key_data_asset (value, key_id, metadata_path, tenant, modified_at) values ('{"name":"","value":"finance3"}'::jsonb,1,'hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:','NONE',('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 1,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:', 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 2,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:', 3,('2018-12-10 09:27:17.815'::timestamp));
insert into dg_metadata_test.business_assets_data_asset(name, description, tenant, metadata_path, business_assets_id, modified_at) values ('','','NONE', 'hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:', 3,('2018-12-10 09:27:17.815'::timestamp));

-- Quality Rules
insert into dg_metadata_test.quality(id,metadata_path,name, description, type, catalog_attribute_type, query, parameters, link, active, result_unit, result_operation_type, result_operation, result_action, result_execute, tenant, created_at, modified_at)
values (1,'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:','quality1', 'description','SPARK', 'catalog', 'select *', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, true, '{"name":"","value":"1.0"}'::jsonb, 'int', 'blabla', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, 'NONE', '2018-12-10 09:27:17.815'::timestamp, '2018-12-10 09:27:17.815'::timestamp);

insert into dg_metadata_test.quality(id,metadata_path,name, description, type, catalog_attribute_type, query, parameters, link, active, result_unit, result_operation_type, result_operation, result_action, result_execute, tenant, created_at, modified_at)
values (2,'hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:','quality2', 'description','SPARK', 'catalog', 'select *', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, true, '{"name":"","value":"1.0"}'::jsonb, 'int', 'blabla', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, 'NONE', '2018-12-10 09:27:17.815'::timestamp, '2018-12-10 09:27:17.815'::timestamp);

insert into dg_metadata_test.quality(id,metadata_path,name, description, type, catalog_attribute_type, query, parameters, link, active, result_unit, result_operation_type, result_operation, result_action, result_execute, tenant, created_at, modified_at)
values (3,'hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:','comment', 'description','SPARK', 'catalog', 'select *', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, true, '{"name":"","value":"1.0"}'::jsonb, 'int', 'blabla', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, 'NONE', '2018-12-10 09:27:17.815'::timestamp, '2018-12-10 09:27:17.815'::timestamp);

insert into dg_metadata_test.quality(id,metadata_path,name, description, type, catalog_attribute_type, query, parameters, link, active, result_unit, result_operation_type, result_operation, result_action, result_execute, tenant, created_at, modified_at)
values (4,'hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:','2018', 'description','SPARK', 'catalog', 'select *', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, true, '{"name":"","value":"1.0"}'::jsonb, 'int', 'blabla', '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, '{"name": "John","phone": ["333-3333", "555-5555"]}'::jsonb, 'NONE', '2018-12-10 09:27:17.815'::timestamp, '2018-12-10 09:27:17.815'::timestamp);

-- Business Rule
insert into dg_metadata_test.business_assets (id,"name",description,properties,tenant,business_assets_type_id,domain_id,business_assets_status_id,modified_at,user_id)
values (5,'LegalAge','<p>Different types of legal age definitions</p>','{"examples": "<p>Come of Age in USA = 21</p><p>Come of Age in Spain = 18</p><p><br></p>"}','NONE',2,1,4,('2018-12-10 09:27:17.815'::timestamp),'anonymous');

-- keyValues for Business Asset
insert into dg_metadata_test.key_business_assets (id,value,key_id,business_assets_id,tenant,created_at,modified_at,user_id) values (1,'{"name":"","value":"YourSelf"}',1,5,'NONE','2019-07-31 06:52:00.238','2019-07-31 06:52:00.238','anonymous');
insert into dg_metadata_test.key_business_assets (id,value,key_id,business_assets_id,tenant,created_at,modified_at,user_id) values (5,'{"name":"","value":"HimSelf"}',1,1,'NONE','2019-07-31 06:52:00.238','2019-07-31 06:52:00.238','anonymous');

-- keyValues for Quality Rule
insert into dg_metadata_test.key_quality (id,value,key_id,quality_id,tenant,created_at,modified_at,user_id) values (1,'{"name":"","value":"YourSelf"}',1,1,'NONE','2019-07-31 06:52:00.238','2019-07-31 06:52:00.238','anonymous');

-- Business Rule for QualityRule
INSERT INTO dg_metadata_test.business_assets_quality (id,quality_id,business_assets_id,tenant,created_at,modified_at,user_id) VALUES (1,1,5,'NONE','2019-07-31 06:52:00.238','2019-07-31 06:52:00.238','anonymous');
