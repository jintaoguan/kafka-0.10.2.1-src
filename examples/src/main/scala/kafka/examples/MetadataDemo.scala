package kafka.examples

import kafka.api.TopicMetadataRequest._
import kafka.api.{TopicMetadataRequest, TopicMetadataResponse}
import kafka.consumer.SimpleConsumer

object MetadataDemo {
  def main(args: Array[String]): Unit = {
    val consumer = new SimpleConsumer("localhost", 9092, 50, 1024 * 4, DefaultClientId)

    val req: TopicMetadataRequest = new TopicMetadataRequest(CurrentVersion, 0, DefaultClientId, List("test"))
    val resp: TopicMetadataResponse = consumer.send(req)

    println("Broker Infos:")
    println(resp.brokers.mkString("\n\t"))
    val metadata = resp.topicsMetadata
    metadata.foreach { topicMetadata =>
      val partitionsMetadata = topicMetadata.partitionsMetadata
      partitionsMetadata.foreach { partitionMetadata =>
        println(s"partitionId = ${partitionMetadata.partitionId}\n" +
          s"\tleader = ${partitionMetadata.leader}\n" +
          s"\tISR = ${partitionMetadata.isr}\n" +
          s"\treplicas = ${partitionMetadata.replicas}")
      }
    }
  }
}
