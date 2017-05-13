/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package singleround

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.io.File
import java.io.PrintWriter
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import u._

case object Start

sealed trait Message
case object Marker extends Message
case class Ball(num: Int) extends Message 

case object Begin
sealed trait Snapshot
case class ByInitiator(balls: Seq[Message]) extends Snapshot
case class ByReceiver(balls: Seq[Message]) extends Snapshot
//case class ChannelOnly(balls: Seq[Message]) extends Snapsnot

object s {

  def main(args: Array[String]) {
    val system = ActorSystem("SingleRound")
    val a = system.actorOf(Node.props(1), name = "1")
    val b = system.actorOf(Node.props(2), name = "2")
    val c = system.actorOf(Node.props(3), name = "3")
 
    // neighbors
    a ! b
    b ! c
    c ! a
    c ! b

    // balls 

    a ! Ball(1)
    a ! Ball(2)
    a ! Ball(3)

    b ! Ball(4)
    b ! Ball(5)

    c ! Ball(6)
    c ! Ball(7)
    c ! Ball(8)
 
    // start snapshot procedure

    a ! Start
    b ! Start
    c ! Start
  }

}

object Node {

  def props(id: Int): Props = Props(classOf[Node], id)

}
class Node(id: Int) extends Actor with ActorLogging {

  var neighbors = Seq.empty[ActorRef]
  var messages = Seq.empty[Message]
  var observer: Option[ActorRef] = None 
  val isInitiator = if (1 == id) true else false
  var snapshotted = false

  override def preStart() {
    observer = Option(context.system.actorOf(Observer.props(id)))
  }

  override def receive = initialize

  def initialize(): Actor.Receive = {
    case neighbor: ActorRef => neighbors ++= Seq(neighbor)  
    case message: Message => messages ++= Seq(message) 
    case Start => {
      log.info(s"${self.path.name} has neighbors ${neighbors.map{_.path.name}}, containing messages ${messages}")
      if(isInitiator) self ! Begin
      context.become(process) 
    }
  }

  def pickedMessage(): Message = {
    val picked = messages(pick(messages.size))
    messages = messages diff Seq(picked)
    picked
  }

  def process: Actor.Receive = {
    case Begin => {
      observer.map { o => 
        messages = Marker +: messages
        o ! ByInitiator(messages) 
        neighbors.foreach { neighbor =>
          neighbor ! Marker
          neighbor ! pickedMessage 
        }
      }
      snapshotted = true
      log.info(s"$id is snapshotted!")
    }
    case Marker => {
      // **dummy** simulate send data before marker is received.
      neighbors.foreach { neighbor => neighbor ! pickedMessage }   

      if(!snapshotted) observer.map { o => 
        messages = Marker +: messages 
        o ! ByReceiver(messages)
        neighbors.map { neighbor => neighbor ! Marker }
        snapshotted = true
      } else {
        // TODO: snapshot received message before the latest received Marker  
        observer.map { o => 
          // TODO: find balls before marker in messages
          //o ! ChannelOnly(ballsBeforeMarker) 
        }  
      }
      neighbors.foreach(_ ! Marker)
    }  
    case ball: Ball => // TODO: prepend to the head in order of balls being received
  }
}

object Observer {

  def props(id: Int): Props = Props(classOf[Observer], id)

}
class Observer(id: Int) extends Actor with ActorLogging {

  var count = 0

  override def preStart() {
    val dir = new File(s"/tmp/$id")
    if(!dir.exists) {
      log.info(s"Create directory $dir!")
      dir.mkdirs
    }
  }   

  def write(balls: Seq[Message])(path: String) {
    val file = new File(path)
    val writer = new PrintWriter(file)
    writer.write(balls.mkString("[", ",", "]"))
    writer.close
  }
 
  override def receive = {
    case ByInitiator(balls) => write(balls)(s"/tmp/$id/first")
    case ByReceiver(balls) => {
      count += 1
      write(balls)(s"/tmp/$id/snapshot.$count")
    }
    case msg@_ => log.warning(s"Observe message: $msg")
  }
}
