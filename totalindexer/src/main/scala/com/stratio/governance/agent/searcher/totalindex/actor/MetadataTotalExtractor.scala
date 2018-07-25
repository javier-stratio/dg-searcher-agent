package com.stratio.governance.agent.searcher.totalindex.actor

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.totalindex.actor.MetadataTotalExtractor.Chunks
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.{ConnectionPool, DB}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{MILLISECONDS, _}
import scala.util.{Failure, Success, Try}

object MetadataTotalExtractor {
  case class Chunks(l: List[Seq[DatastoreEngine]])
}

class MetadataTotalExtractor(indexer: ActorRef, override val circuitBreakerConfig: Config)
  extends Actor with CircuitBreakerConfig {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  // execution context for the notifications
  context.dispatcher

  val connection: Connection = ConnectionPool.borrow()
  val db: DB = DB(connection)
  val metadataList: Cancellable = context.system.scheduler.scheduleOnce(1000 millis, self, "metadataList")


  circuitBreaker.onOpen(setEOnErrorState())
    .onClose(backToClosedState)
    .onHalfOpen(backToNormalState())

  def setEOnErrorState(): Unit = {
    LOG.error("Circuit breaker open")
    context.become(error)
  }

  def backToNormalState(): Unit = {
    LOG.debug("Circuit breaker half Open")
    context.unbecome()
  }

  def backToClosedState(): Unit = {
    LOG.debug("Circuit breaker Closed")
    context.unbecome()
  }

  override def receive = {
    case "metadataList" =>
      getAllMetadataList(connection)


    case Chunks(list) =>

      if(list.nonEmpty){
        (indexer ? TotalIndexer.IndexerEvent(list.head)).onComplete{
          case Success(_) => self ! Chunks(list.tail)
          case Failure(e) => {
            //TODO manage errors
            println(s"Indexation failed")
            e.printStackTrace()
          }

        }

      } else {
        self ! "postgresNotification"
      }



    /*
          db.readOnly { implicit session =>
            val notifications: Array[PGNotification] = Option(pgConnection.getNotifications).getOrElse(Array[PGNotification]())

            if (notifications.nonEmpty) {
              self ! Chunks(notifications.grouped(1000).toList)
            }else{
              self ! "postgresNotification"
            }
          }
    */

  }


  def getAllMetadataList(connection: Connection)
  //: Try[List[String]]
  = {
    def getDatastoresList(rs: ResultSet): Try[List[String]] = Try {
      val dbList = scala.collection.mutable.ArrayBuffer.empty[String]
      while (rs.next) {
        LOG.debug(s"listDownAllDatabases: found database ${rs.getString(1)}")
        dbList.append(rs.getString(1))
      }
      dbList.toList
    }

    def closeResultSet(rs: ResultSet) = rs.isClosed match {
      case false => rs.close
      case _ => Unit
    }

    def closeStatement(ps: PreparedStatement) = ps.isClosed match {
      case false => ps.close
      case _ => Unit
    }
    //val kkkkk = Try {
      //connection match {
        //case (conn) =>
          //val ps = conn.prepareStatement("SELECT row_to_json(r) (SELECT * FROM pg_datastores);")

          val stmt1: Statement = connection.createStatement(ResultSet.CONCUR_READ_ONLY,
            //ResultSet.FETCH_FORWARD,
            ResultSet.TYPE_FORWARD_ONLY)

    connection.setAutoCommit(false)
          stmt1.setFetchSize(2)

          val rs1: ResultSet = stmt1.executeQuery("SELECT * FROM dg_metadata.datastore_engine;")
          val datastores: Seq[DatastoreEngine] = DatastoreEngine.getResult(rs1)
          if (datastores.nonEmpty) {
            self ! Chunks(datastores.grouped(1000).toList)
          }else{
            //TODO to be implemented
            println("sdfsfsdf")
          }

        /*
                    val list = datastores.grouped(1000).toList
                    if(list.nonEmpty){
                      (indexer ? TotalIndexer.IndexerEvent(list.head)).onComplete{
                        case Success(_) => self ! Chunks(list.tail)
                        case Failure(e) => {
                          //TODO manage errors
                          println(s"Indexation failed")
                          e.printStackTrace()
                        }

                      }

                    } else {
                      self ! "postgresNotification"
                    }
        */

        //  }


/*
          val rss: ResultSet = stmt.executeQuery("SELECT * FROM dg_metadata.datastore_engine;")
          //PostgresUtils.processCursor(stmt.executeQuery("SELECT * FROM dg_metadata.datastore_engine;").setFetchSize(2)) {
          PostgresUtils.processCursor(rss) {
            resultSet => {
              resultSet.beforeFirst()
              val kk = PostgresUtils.sqlResultSetToJson(resultSet)
              println(kk)
              kk
            }
            // process current row of the ResultSet
          }
*/


          //val rs = ps.executeQuery
          //val jsValueResult: JsValue = PostgresUtils.sqlResultSetToJson(rs)

/*
        case _ =>
          LOG.error("There was an error when discovering all databases")
          ""
*/
      //}
    //}

    //print(kkkkk)

    List.empty
  }

  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }


/*
  def getResultSet() = {
    Ok.stream(Enumerator.unfold(resultSet) { (rs: ResultSet) =>
      if (rs.next()) {
        val chunk = // Read the result from the ResultSet and format it in the way you want it formatted
          Some((rs, chunk))
      } else None
    }.onDoneEnumerating {
      resultSet.close()
      stmt.close()
      conn.close()
    })
  }
*/

}
