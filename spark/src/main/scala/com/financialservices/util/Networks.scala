package com.financialservices.util

import java.net.{Inet6Address, InetAddress, NetworkInterface}

import scala.collection.JavaConverters._

case class NetworkInterfaceNotFound(message: String) extends Exception(message)

class Networks(interfaceName: String, networkProvider: NetworkInterfaceProvider) {

  def this(interfaceName: String) =
    this(interfaceName, new NetworkInterfaceProvider)

  def this() = this("")

  def hostname(): String = ipv4Address.getHostAddress

  def ipv4Address: InetAddress =
    mappings
      .sortBy(_._1)
      .find(pair => isIpv4(pair._2))
      .getOrElse((0, InetAddress.getLocalHost))
      ._2

  // Check if the given InetAddress is not a loopback address and is a ipv4 address
  private def isIpv4(addr: InetAddress): Boolean =
  // Don't use ipv6 addresses yet, since it seems to not be working with the current akka version
    !addr.isLoopbackAddress && !addr.isInstanceOf[Inet6Address]

  //Get a flattened seq of Index -> InetAddresses pairs
  private def mappings: Seq[(Int, InetAddress)] =
    for {
      (index, inetAddresses) <- interfaces
      inetAddress            <- inetAddresses
    } yield (index, inetAddress)

  private def interfaces: Seq[(Int, List[InetAddress])] =
    if (interfaceName.isEmpty)
      networkProvider.allInterfaces
    else
      networkProvider.getInterface(interfaceName)

}

class NetworkInterfaceProvider {

  def allInterfaces: Seq[(Int, List[InetAddress])] =
    NetworkInterface.getNetworkInterfaces.asScala.toList
      .map(iface => (iface.getIndex, iface.getInetAddresses.asScala.toList))

  def getInterface(interfaceName: String): Seq[(Int, List[InetAddress])] =
    Option(NetworkInterface.getByName(interfaceName)) match {
      case Some(nic) =>
        List((nic.getIndex, nic.getInetAddresses.asScala.toList))
      case None =>
        val networkInterfaceNotFound = NetworkInterfaceNotFound(s"Network interface=$interfaceName not found")
        throw networkInterfaceNotFound
    }
}
