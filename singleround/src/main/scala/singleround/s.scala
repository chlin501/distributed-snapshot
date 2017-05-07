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

import scala.collection.immutable.Queue

case object Start

sealed trait Message
case object Marker extends Message
case class Ball(num: Int) extends Message 

object s {

  def main(args: Array[String]) {
    val system = ActorSystem("SingleRound")
    val a = system.actorOf(Node.props(1), name = "1")
    val b = system.actorOf(Node.props(2), name = "2")
    val c = system.actorOf(Node.props(3), name = "3")
  }

}

object Node {

  def props(id: Int): Props = Props(classOf[Node], id)

}
case class Node(id: Int) extends Actor with ActorLogging {

  var neighbors = Queue.empty[ActorRef]
  var messages = Queue.empty[Message]

  override def preStart() {
    // TODO: spwan observer 
  }

  override def receive = initialize

  def initialize(): Actor.Receive = {
    case neighbor: ActorRef => neighbors = neighbors.enqueue(neighbor)  
    case message: Message => messages = messages.enqueue(message) 
    case Start => context.become(process) 
  }

  def process: Actor.Receive = {
    case Marker =>  
    case Ball(num) =>
  }
}
