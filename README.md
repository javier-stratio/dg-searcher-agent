# dg-searcher-agent

To format sources execute: mvn -P format validate






- Levantar elasticsearch en local

search-engine-core/local-env/configure.sh   Solo hace falta lanzarlo una vez

search-engine-core/local-env/docker-compose.yml    Configuración del compose

docker-compose up

Consola “cerebro” de elastic en http://localhost:9001


- Módulos que componen la aplicación 

dg-searcher-agent: Se compone de dos módulos. partial-indexer y total-indexer. Para indexaciones parciales y totales. También tiene un módulo commons con clases del modelo de bbdd, clases del modelo de elastic y algunas utilidades.

dg-search-engine-core: Se compone de los siguientes módulos:

	se-common
	se-manager: tareas de gestión de los domain (alta, modificación, baja…). Para dar de alta un dominio, se el endpoint http://${servidor_elastic}:${puerto}/manager/admin/${nombre_dominio}
	se-indexer: componente que indexa los documentos en elastic. Para probar un flujo completo de indexaciones parciales o totales, este módulo deberá estar levantado. Es un módulo de Spring Boot y se levanta ejecutando su clase principal. En él se encuentran los endpoints para las indexaciones totales y parciales.
	se-searcher: módulo con la lógica del buscador


- Creación de dominio: **Anexo 1


- Indexaciones totales

En las indexaciones totales se recuperan todos los datos de la bbdd de Governance (dg_metadata), se construyen los documentos (json) y se envían a elastic en bulks de 1000.
El proceso de indexación consta de un inicial cambio de estado a RUNNING mediante la llamada al endpoint http://${indexer_base_path}:${port}/domain/total del indexer. Esta llamada nos devolverá el token necesario para el siguiente paso.
A continuación comienza la indexación de los documentos mediante el endpoint http://${indexer_base_path}:${port}/indexer/test_domain/total/${token}/index
Se recomienda realizar las indexaciones en bulks de 1000 documentos.
Una vez terminada la indexación de los documentos, se llama al endpoint http://${indexer_base_path}:${port}/indexer/test_domain/total/${token}/end
y se da por finalizada la indexación.
En este último paso se realiza el switch entre el índice sobre el que se ha hecho la indexación total, y el índice sobre el que se pudieron seguir haciendo indexaciones parciales y/o búsquedas. En este momento, el índice sobre el que se han hecho las totales pasa a ser el índice activo y el otro se cierra.

Si por cualquier motivo, una vez iniciada la indexación, queremos dejarlo en el estado inicial, el indexer dispone de un endpoint para ello: http://${indexer_base_path}:${port}/indexer/test_domain/total/$token/cancel

- Indexaciones parciales

En las indexaciones parciales, se reciben los eventos provocados por los insert, update o delete de nuestro esquema de bbdd dg_metadata.
Para ello se han creado triggers sobre las tablas a monitorizar. Como por ejemplo:

CREATE TRIGGER datastore_engine_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dg_metadata.datastore_engine
    FOR EACH ROW EXECUTE PROCEDURE dg_metadata.notify_event();

Como se puede ver, el trigger, por cada fila afectada, ejecuta una llamada a una función de postgresql.


CREATE OR REPLACE FUNCTION dg_metadata.notify_event()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$    DECLARE 
        data json;
        notification json;
    
    BEGIN
    
        IF (TG_OP = 'DELETE') THEN
            data = row_to_json(OLD);
        ELSE
            data = row_to_json(NEW);
        END IF;
        
        -- Contruct the notification as a JSON string.
        notification = json_build_object(
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'data', data);
        
                        
        -- Execute pg_notify(channel, notification)
        PERFORM pg_notify('events',notification::text);
        
        -- Result is ignored since this is an AFTER trigger
        RETURN NULL; 
    END;
    
$function$


Esta función envía la info de la tabla afectada, la operación realizada y el registro completo a un bus de eventos de postgresql.

La funcionalidad de escucha de ese bus la realiza el módulo  partial-indexer del componente dg-searcher-agent

Tanto la función como los triggers están subidos a la carpeta de resources del módulo common del componente dg-searcher-agent (para la productivización de este componente, deberían ir al bootstrap y lanzarse de igual manera que se lanzan las sql evolutions para crear la bbdd)



Nota: En las indexaciones (totales o parciales), cada documento deberá llevar un id único. En caso de indexar un documento con un id ya existente, lo que hace elastic es machacar el antiguo con el nuevo. El id que se está utilizando es el metadata path.



ANEXO 1

- Creación de dominio:

Se realiza sobre el componente manager:

PUT a http://localhost:8080/manager/admin/${nombre_dominio}

Body de la petición. En él se declara el modelo a utilizar: 

{
  "id": "governance_search",
  "name": "Governance Search",
  "model": {
    "id": "generated_id",
    "category": "generated.categories",
    "language": "spanish",
    "fields": [
      {
        "field": "accessTime",
        "name": "Access Time",
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "agentVersion",
        "name": "Agent Version",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "baseType",
        "name": "Base Type",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "createdAt",
        "name": "Created At",
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "credentials",
        "name": "Credentials",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "databaseSchemaId",
        "name": "Database Schema Id",
        "type": "number",
        "subtype": "long",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "datastoreEngineId",
        "name": "Datastore Engine Id",
        "type": "number",
        "subtype": "long",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "description",
        "name": "Description",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "displayName",
        "name": "Display Name",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "id",
        "name": "Id",
        "type": "number",
        "subtype": "long",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "isDir",
        "name": "Is Dir",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "isFk",
        "name": "Is Fk",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "isPartK",
        "name": "Is PartK",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "isPk",
        "name": "Is Pk",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "isView",
        "name": "Is View",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "metadataPath",
        "name": "Metadata Path",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "modificationTime",
        "name": "Modification Time",
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "name",
        "name": "Name",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "operationCommandType",
        "name": "Operation Command Type",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "schema",
        "name": "Schema",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "schemaName",
        "name": "Schema Name",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "serverVersion",
        "name": "Server Version",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "tableId",
        "name": "Table Id",
        "type": "number",
        "subtype": "long",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "type",
        "name": "Type",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "updatedAt",
        "name": "Updated At",
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "uri",
        "name": "URI",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.entity",
        "name": "(G) Entity",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.id",
        "name": "(G) Identifier",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.parent_id",
        "name": "(G) Parent Id",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.type",
        "name": "(G) Type",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.categories",
        "name": "(G) Category",
        "type": "category",
        "aggregable": true,
        "key": {
          "field": "id",
          "name": "(G) Category Id",
          "type": "text",
          "searchable": true,
          "sortable": true,
          "aggregable": true
        },
        "value": {
          "field": "name",
          "name": "(G) Category Name",
          "type": "text",
          "searchable": true,
          "sortable": true,
          "aggregable": true
        }
      },
      {
        "field": "generated.keyValuePairs.owner",
        "name": "(G) KeyValue Owner",
        "type": "text",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.keyValuePairs.gdp",
        "name": "(G) KeyValue GDP",
        "type": "boolean",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      },
      {
        "field": "generated.keyValuePairs.quality",
        "name": "(G) KeyValue Quality",
        "type": "number",
        "subtype": "float",
        "searchable": true,
        "sortable": true,
        "aggregable": true
      }
    ]
  },
  "search_fields": {
    "baseType": 1,
    "description": 1,
    "displayName": 1,
    "name": 1,
    "schema": 1,
    "uri": 1,
    "generated.keyValuePairs.owner": 1,
    "generated.keyValuePairs.gdp": 1,
    "generated.keyValuePairs.quality": 1
  }
}

Queda pendiente incluir un campo type en el modelo y declararlo como buscable.
