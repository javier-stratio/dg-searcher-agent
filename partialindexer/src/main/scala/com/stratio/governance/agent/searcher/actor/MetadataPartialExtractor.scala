package com.stratio.governance.agent.searcher.actor

import java.sql.{Connection, ResultSet, Statement}

import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actor.MetadataPartialExtractor.Chunks
import com.typesafe.config.Config
import org.apache.commons.dbcp.DelegatingConnection
import org.json4s.DefaultFormats
import org.postgresql.{PGConnection, PGNotification}
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object MetadataPartialExtractor {
  case class Chunks(l: List[Array[PGNotification]])
}


class MetadataPartialExtractor(indexer: ActorRef, override val circuitBreakerConfig: Config)
  extends Actor with CircuitBreakerConfig{

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
  implicit val formats: DefaultFormats.type = DefaultFormats

  // execution context for the notifications
  context.dispatcher

  val connection: Connection = ConnectionPool.borrow()
  val db: DB = DB(connection)
  val postgresNotification: Cancellable = context.system.scheduler.scheduleOnce(1000 millis, self, "postgresNotification")
  val pgConnection: PGConnection = connection.asInstanceOf[DelegatingConnection].getInnermostDelegate.asInstanceOf[PGConnection]

  //implicit val formats = DefaultFormats

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


  override def preStart() = {
    // make sure connection isn't closed when executing queries
    // we setup the
    db.autoClose(false)
    db.localTx { implicit session =>
      session.connection.setAutoCommit(false)
      val stmt: Statement = session.connection.createStatement(ResultSet.CONCUR_READ_ONLY,
        ResultSet.FETCH_FORWARD,
        ResultSet.TYPE_FORWARD_ONLY)
      stmt.setFetchSize(1000)
      stmt.setMaxRows(1000)

      stmt.execute("LISTEN events")

    }
  }

  override def postStop() = {
    postgresNotification.cancel()
    db.close()
  }


  def receive = {
    case "postgresNotification" =>
      db.readOnly { implicit session =>
        val notifications: Array[PGNotification] = Option(pgConnection.getNotifications).getOrElse(Array[PGNotification]())

        if (notifications.nonEmpty) {
          self ! Chunks(notifications.grouped(1000).toList)
        }else{
          self ! "postgresNotification"
        }
      }


    case Chunks(list) =>
      if(list.nonEmpty){
        (indexer ? PartialIndexer.IndexerEvent(list.head)).onComplete{
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
  }


  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }


}
