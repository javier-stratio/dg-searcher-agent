package com.stratio.governance.agent.searcher.totalindex.actor

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.totalindex.actor.MetadataTotalExtractor.Chunks
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.{ConnectionPool, DB}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{MILLISECONDS, _}
import scala.util.{Failure, Success, Try}
import scalikejdbc._
import scalikejdbc.streams._

object MetadataTotalExtractor {
  case class Chunks(l: List[Seq[DatastoreEngine]])
}

class MetadataTotalExtractor(indexer: ActorRef) extends Actor {

  private lazy val LOG: Logger  = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  // execution context for the notifications
  context.dispatcher

  val connection: Connection    = ConnectionPool.borrow()
  val db: DB                    = DB(connection)
  val metadataList: Cancellable = context.system.scheduler.scheduleOnce(10000 millis, self, "metadataList")

  implicit val mat = ActorMaterializer()

  override def receive = {
    case "metadataList" =>
      val publisher: DatabasePublisher[DatastoreEngine] = DB.readOnlyStream {
        sql"SELECT * FROM dg_metadata.datastore_engine"
          .map(r => DatastoreEngine.getOneResult(r.underlying))
          .iterator
          .withDBSessionForceAdjuster(session => {
            session.connection.setAutoCommit(false)
            session.fetchSize(10)
          })
      }

      Source
        .fromPublisher(publisher)
        .grouped(1000)
        //statefulMapConcat para mantener el estado y ver los batches que se han formado
        .statefulMapConcat { () =>
          var i = 1
          x =>
            println(i)
            i = i + 1
            x
        }
        .to(Sink.ignore)
        .run
//        .runForeach(_ => print(1))

// TODO descomentar
//      getAllMetadataList(connection)

    case Chunks(list) =>
      if (list.nonEmpty) {
        (indexer ? TotalIndexer.IndexerEvent(list.head)).onComplete {
          case Success(_) => self ! Chunks(list.tail)
          case Failure(e) =>
            //TODO manage errors
            println(s"Indexation failed")
            e.printStackTrace()
        }
      } else {
        self ! "postgresNotification"
      }

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
      case _     => Unit
    }

    def closeStatement(ps: PreparedStatement) = ps.isClosed match {
      case false => ps.close
      case _     => Unit
    }

    val stmt1: Statement = connection.createStatement(ResultSet.CONCUR_READ_ONLY,
                                                      //ResultSet.FETCH_FORWARD,
                                                      ResultSet.TYPE_FORWARD_ONLY)

    connection.setAutoCommit(false)
    stmt1.setFetchSize(2)

    val rs1: ResultSet                   = stmt1.executeQuery("SELECT * FROM dg_metadata.datastore_engine;")
    val datastores: Seq[DatastoreEngine] = DatastoreEngine.getResult(rs1)
    if (datastores.nonEmpty) {
      self ! Chunks(datastores.grouped(1000).toList)
    } else {
      //TODO to be implemented
      println("sdfsfsdf")
    }
    List.empty
  }

  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }

}
