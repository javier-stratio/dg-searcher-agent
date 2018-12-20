package com.stratio.governance.agent.searcher.actors.manager

import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{PartialIndexationMessageInit, TotalIndexationMessageInit}
import com.stratio.governance.agent.searcher.actors.manager.dao.SearcherDao
import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler
import com.stratio.governance.agent.searcher.actors.manager.scheduler.defimpl.DGScheduler
import com.stratio.governance.agent.searcher.actors.manager.utils.ManagerUtils
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DGSchedulerTest(system: ActorSystem, interval: Int, cronExpression: String, testNumber: Int) extends DGScheduler(system, true, interval, true, cronExpression) {

  override def schedulePartialIndexation(actor: ActorRef, reference: String): Unit = {
    testNumber match {
      case 3 =>
        system.scheduler.schedule(100 millis, interval millis, actor, reference)
      case _ => // Do not program
    }
  }

  override def scheduleTotalIndexation(actor: ActorRef, reference: String): Unit = {
    testNumber match {
      case 4 =>
        scheduler.schedule(TOTAL_INDEXER_REF, actor, reference)
      case _ => // Do not program
    }
  }

}

class ManagerUtilsTest(scheduler: DGScheduler, s: Semaphore, testNumber: Int) extends ManagerUtils {

  var modelGenerated: Boolean = false

  def isModelGenerated(): Boolean = {
    modelGenerated
  }

  override def getGeneratedModel(): String = {
    modelGenerated = true
    "{}"
  }

  override def getScheduler(): Scheduler = {
    scheduler
  }

}

class DGSearcherDaoMock(s: Semaphore, testNumber: Int) extends SearcherDao {

  var modelsJson: Option[String] = None
  var checked: Boolean = false

  var totalIndexationSteps: Int = 0

  override def getModels(): List[String] = {
    testNumber match {
      case 2 => {
        return List("Model1", "Model2")
      }
      case _ => {
        return List(DGManager.MODEL_NAME, "Other")
      }
    }

  }

  def isChecked(): Boolean = {
    return checked
  }

  def getModelsJson(): Option[String] = {
    return modelsJson
  }

  def getTotalIndexationSteps(): Int = {
    totalIndexationSteps
  }

  override def checkTotalIndexation(model: String): (Boolean, Option[String]) = {
    totalIndexationSteps += 1
    checked = true
    testNumber match {
      case _ => {
        return (false, None)
      }
    }
  }

  override def insertModel(model: String, jsonModel: String): Unit = {
    totalIndexationSteps += 1
    modelsJson = Some(jsonModel)
  }

  override def initTotalIndexationProcess(model: String): String = {
    totalIndexationSteps += 1
    return "1234567890"
  }

  override def finishTotalIndexationProcess(model: String, token: String): Unit = {
    totalIndexationSteps += 1
    s.release()
  }

  override def cancelTotalIndexationProcess(model: String, token: String): Unit = {}


}

class ExtratorActorMock(s: Semaphore, name: String) extends Actor {

  override def receive: Receive = {
    case TotalIndexationMessageInit(token) => {
      // Total indexation  execution simulated
      implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
      for (res <- context.actorSelection("/user/" + name).resolveOne()) {
        val managerActor = res
        println(res)
        managerActor ! DGManager.ManagerTotalIndexationEvent(token, IndexationStatus.SUCCESS)
      }
    }
    case PartialIndexationMessageInit() => {
      // Total indexation  execution simulated
      implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
      for (res <- context.actorSelection("/user/" + name).resolveOne()) {
        val managerActor = res
        println(res)
        managerActor ! DGManager.ManagerPartialIndexationEvent(IndexationStatus.SUCCESS)
        s.release()
      }
    }
  }

}

class DGManagerTest extends FlatSpec {

  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
  lazy val system = ActorSystem("DGManagerTest")

  "Second Boot Process " should " not execute Total Initialization" in {

    val testNumber: Int = 1
    val scheduler: DGScheduler = new DGSchedulerTest(system, 1000, "", testNumber)
    val managerUtils: ManagerUtilsTest = new ManagerUtilsTest(scheduler, null, testNumber)
    val searcherDao: DGSearcherDaoMock = new DGSearcherDaoMock(null, testNumber)
    val extractor: ActorRef = system.actorOf(Props(classOf[ExtratorActorMock], new Semaphore(1), "DGManagerTest_actor" + testNumber), "DGManagerTest_extractor" + testNumber)
    val manager: ActorRef = system.actorOf(Props(classOf[DGManager],extractor,managerUtils,searcherDao), "DGManagerTest_actor" + testNumber)

    val result = searcherDao.isChecked()

    assert(!result, "model must not be checked, because it is not checked")

  }

  "First Boot Process " should " be processed commanding a total indexation" in {

    val s: Semaphore = new Semaphore(1)
    s.acquire(1)

    val reference: String = "{}"

    val testNumber: Int = 2
    val scheduler: DGScheduler = new DGSchedulerTest(system, 1000, "", testNumber)
    val managerUtils: ManagerUtilsTest = new ManagerUtilsTest(scheduler, s, testNumber)
    val searcherDao: DGSearcherDaoMock = new DGSearcherDaoMock(s, testNumber)
    val extractor = system.actorOf(Props(classOf[ExtratorActorMock], s, "DGManagerTest_actor" + testNumber), "DGManagerTest_extractor" + testNumber)
    val manager = system.actorOf(Props(classOf[DGManager],extractor,managerUtils, searcherDao), "DGManagerTest_actor" + testNumber)

    s.acquire(1)
    s.release()

    val result = searcherDao.getModelsJson()

    assertResult(Some(reference))(result)

  }

  "Simple Partial Indexation Process " should " be responsed by Extractor actor" in {

    val s: Semaphore = new Semaphore(1)
    s.acquire(1)

    val testNumber: Int = 3
    val scheduler: DGScheduler = new DGSchedulerTest(system, 1000, "", testNumber)
    val managerUtils: ManagerUtilsTest = new ManagerUtilsTest(scheduler, s, testNumber)
    val searcherDao: DGSearcherDaoMock = new DGSearcherDaoMock(s, testNumber)
    val extractor = system.actorOf(Props(classOf[ExtratorActorMock],s, "DGManagerTest_actor" + testNumber), "DGManagerTest_extractor" + testNumber)
    val manager = system.actorOf(Props(classOf[DGManager],extractor,managerUtils, searcherDao), "DGManagerTest_actor" + testNumber)

    s.acquire(1)
    s.release()

    // Getting here is working. Otherwise will be blocked
    assert(true, "")

  }

  "Total Indexation Process " should " be responsed by Extractor actor and REST SE interfaces" in {

    val s: Semaphore = new Semaphore(1)
    s.acquire(1)

    val testNumber: Int = 4
    val scheduler: DGScheduler = new DGSchedulerTest(system, 1000, "*/1 * * ? * *", testNumber)
    scheduler.createTotalIndexerScheduling()
    val managerUtils: ManagerUtilsTest = new ManagerUtilsTest(scheduler, s, testNumber)
    val searcherDao: DGSearcherDaoMock = new DGSearcherDaoMock(s, testNumber)
    val extractor = system.actorOf(Props(classOf[ExtratorActorMock],s, "DGManagerTest_actor" + testNumber), "DGManagerTest_extractor" + testNumber)
    val manager = system.actorOf(Props(classOf[DGManager],extractor,managerUtils, searcherDao), "DGManagerTest_actor" + testNumber)

    s.acquire(1)
    s.release()

    // Getting here is working. Otherwise will be blocked
    assert(managerUtils.isModelGenerated(), "Model has not been generated!")
    assertResult(5)(searcherDao.getTotalIndexationSteps())

  }

}