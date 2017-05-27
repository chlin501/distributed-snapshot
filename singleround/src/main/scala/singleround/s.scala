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

case object Start

sealed trait Message
case object Marker extends Message
case class Ball(num: Int) extends Message 

case object Begin
sealed trait Snapshot
case class ByInitiator(balls: Seq[Message]) extends Snapshot
case class ByReceiver(balls: Seq[Message]) extends Snapshot
case class ChannelOnly(balls: Seq[Message]) extends Snapshot

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
    val picked = messages.filterNot(_.equals(Marker)).last //.take(1).head
    messages = messages diff Seq(picked)
    picked
  }

  def ballsBeforeMarker(): Seq[Message] = {
    messages.slice(0, messages.indexOf(Marker))
  }

  def process: Actor.Receive = {
    case Begin => {
      observer.map { o => 
        messages = Marker +: messages
        o ! ByInitiator(messages) 
        log.info(s"[snapshot] [$id] $messages!")
        neighbors.foreach { neighbor =>
          neighbor ! Marker
          neighbor ! pickedMessage 
        }
      }
      snapshotted = true
    }
    case Marker => {
      // **dummy** simulate send data before marker is received.
      neighbors.foreach { neighbor => neighbor ! pickedMessage }   
      messages = Marker +: messages 
      if(!snapshotted) observer.map { o => 
        o ! ByReceiver(messages)
        log.info(s"[snapshot] [$id] $messages")
        neighbors.map { neighbor => neighbor ! Marker }
        snapshotted = true
      } else observer.map { o => 
        val channelBalls = ballsBeforeMarker
        o ! ChannelOnly(channelBalls) 
        log.info(s"[snapshot] [$id] $messages")
      }
    }  
    case ball: Ball => messages = ball +: messages
  }
}

object Observer {

  def props(id: Int): Props = Props(classOf[Observer], id)

}
class Observer(id: Int) extends Actor with ActorLogging {

  var count = 0
  val prefix = "/tmp/snapshot"

  override def preStart() {
    val dir = new File(s"$prefix/$id")
    if(dir.exists) rm(dir) 
    dir.mkdirs
  }   

  def rm(path: File): Boolean = {
    if(path.isDirectory) {
      path.listFiles.foreach { e => rm(e) }
    } 
    path.delete
  } 

  def write(balls: Seq[Message])(path: String) {
    val file = new File(path)
    val writer = new PrintWriter(file)
    writer.write(balls.mkString("[", ",", "]"))
    writer.close
  }
 
  override def receive = {
    case ByInitiator(balls) => write(balls)(s"$prefix/$id/first")
    case ByReceiver(balls) => {
      count += 1
      write(balls)(s"$prefix/$id/snapshot.$count")
    }
    case ChannelOnly(balls) => {
      count += 1
      write(balls)(s"$prefix/$id/snapshot.$count")
    }
    case msg@_ => log.warning(s"Observe message: $msg")
  }
}
