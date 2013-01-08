package org.dpratt.spray

import org.jboss.netty.handler.codec.http.{HttpResponseStatus, HttpRequest => NettyHttpRequest, HttpResponse => NettyHttpResponse}

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Status, Response}
import com.twitter.util.{Duration, ScheduledThreadPoolTimer, Future, Promise}

import com.typesafe.config.Config

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.NonFatal
import akka.actor.UnhandledMessage

import spray.http._
import spray.http.StatusCodes._
import spray.http.parser.HttpParser
import spray.http.HttpHeaders.RawHeader
import spray.util.{IOClosed, ClosedEventReason, ConnectionCloseReasons}
import akka.spray.UnregisteredActorRef

/**
* A spray connector for Twitter's Finagle HTTP server. This component routes incoming HttpRequests into
* the Spray RootService actor, along with a responder lambda suitable for completing the Finagle Future.
*
* @author David Pratt (dpratt@vast.com)
*/
class FinagleSprayConnector(system: ActorSystem, serviceActor: ActorRef, config: Config) extends Service[NettyHttpRequest, NettyHttpResponse] {

  implicit var settings = new FinagleConnectorSettings(config)
  implicit def log = system.log

  require(system != null, "No ActorSystem configured")
  require(serviceActor != null, "No ServiceActor configured")
  require(settings != null, "No ConnectorSettings configured")

  var timeoutHandler = if (settings.TimeoutHandler.isEmpty) serviceActor else system.actorFor(settings.TimeoutHandler)

  //twitter Futures need an implicit timer to handle timeouts
  implicit private val timer = new ScheduledThreadPoolTimer(2, "finagle-timer", makeDaemons = true)

  override def release() {
    super.release()
    timer.stop()
  }

  //This is the main entry point to Finagle - it will be called with each HTTP request
  //This method transforms the Netty request to a generic Spray request, creates a lambda to do the final
  //completion of the request, and then dispatches into the async spray framework
  def apply(req: NettyHttpRequest) = {
    Future {
      val out = new Promise[NettyHttpResponse]
      try {
        val request = FinagleConverter.toHttpRequest(req)
        val responder = new Responder(req, out, request)
        serviceActor.tell(request, responder)
        out.within(Duration(settings.RequestTimeout, TimeUnit.MILLISECONDS)).rescue {
          case _: java.util.concurrent.TimeoutException => handleTimeout(request)
        }
      } catch {
        case IllegalRequestException(status, summary, detail) =>
          log.warning("Illegal request {}\n\t{}: {}\n\tCompleting with '{}' response", req, summary, detail, status)
          val msg = if (settings.VerboseErrorMessages) summary + ": " + detail else summary
          val errorResponse = Response()
          writeResponse(HttpResponse(status, msg), errorResponse, req) { out.setValue(errorResponse) }
          out
        case RequestProcessingException(status, msg) =>
          log.warning("Request {} could not be handled normally\n\t{}\n\tCompleting with '{}' response", req, msg, status)
          val errorResponse = Response()
          writeResponse(HttpResponse(status, msg), errorResponse, req) { out.setValue(errorResponse) }
          out
        case NonFatal(e) =>
          log.error(e, "Error during processing of request {}", req)
          val errorResponse = Response()
          writeResponse(HttpResponse(500, entity = "The request could not be handled"), errorResponse, req) { out.setValue(errorResponse) }
          out
      }
    }.flatten
  }

  def handleTimeout(req: HttpRequest): Promise[NettyHttpResponse] = {
    val responseFuture = new Promise[NettyHttpResponse]
    log.warning("Timeout of {}", req)
    val latch = new CountDownLatch(1)
    val nettyResponse = Response()
    val responder = new UnregisteredActorRef(system) {
      def handle(message: Any)(implicit sender: ActorRef) {
        message match {
          case x: HttpResponse => {
            writeResponse(x, nettyResponse, req) { responseFuture.setValue(nettyResponse); latch.countDown() }
          }
          case x => system.eventStream.publish(UnhandledMessage(x, sender, this))
        }
      }
    }
    timeoutHandler.tell(Timeout(req), responder)
    //give the timeout handler some time to complete
    timer.schedule(Duration(settings.TimeoutTimeout, TimeUnit.MILLISECONDS).fromNow) {
      //if the latch hasn't been tripped by now, the timeout handler is not responding - do a manual
      //timeout response
      if (latch.getCount != 0) writeResponse(timeoutResponse(req), nettyResponse, req) { responseFuture.setValue(nettyResponse) }
    }
    responseFuture
  }

  class Responder(nettyRequest: NettyHttpRequest, responsePromise: Promise[NettyHttpResponse], req: HttpRequest)
    extends UnregisteredActorRef(system) {

    val OPEN = 0
    val COMPLETED = 2

    val state = new AtomicInteger(OPEN)

    def postProcess(error: Option[Throwable], sentAck: Option[Any], close: Boolean)(implicit sender: ActorRef) {
      error match {
        case None =>
          sentAck.foreach(sender.tell(_, this))
          if (close) sender.tell(Closed(ConnectionCloseReasons.CleanClose), this)
        case Some(e) =>
          sender.tell(Closed(ConnectionCloseReasons.IOError(e)), this)
      }
    }

    val nettyResponse = Response()

    def handle(message: Any)(implicit sender: ActorRef) {
      message match {
        case wrapper: HttpMessagePartWrapper if wrapper.messagePart.isInstanceOf[HttpResponsePart] =>
          wrapper.messagePart.asInstanceOf[HttpResponsePart] match {
            case response: HttpResponse =>
              if (state.compareAndSet(OPEN, COMPLETED)) {
                val error = writeResponse(response, nettyResponse, req) {
                  //complete the future
                  responsePromise.setValue(nettyResponse)
                }
                postProcess(error, wrapper.sentAck, close = true)
              } else state.get match {
                case COMPLETED =>
                  log.warning("Received a second response for a request that was already completed, dropping ...\nRequest: {}\nResponse: {}", req, response)
              }

            case _ => {
              log.error("Received something besides an HttpResponse. This connector does not support chunked responses.")
              //complete the request with an error
              val message = "Internal Server Error"
              nettyResponse.setStatus(Status.InternalServerError)
              nettyResponse.setHeader("Cache-Control", "no-cache")
              nettyResponse.write(message)
              responsePromise.setValue(nettyResponse)
            }
          }

        case x => system.eventStream.publish(UnhandledMessage(x, sender, this))
      }
    }
  }


  def writeResponse(response: HttpMessageStart with HttpResponsePart,
                    nettyResponse: Response, req: AnyRef)(complete: => Unit): Option[Throwable] = {
    try {
      val resp: HttpResponse = response.message.asInstanceOf[HttpResponse]
      val nettyStatus = HttpResponseStatus.valueOf(resp.status.value)
      nettyResponse.setStatus(nettyStatus)
      resp.headers.foreach { header: HttpHeader =>
        header.lowercaseName match {
          case "content-type"   => // we never render these headers here, because their production is the
          case "content-length" => // responsibility of this layer, not the user
          case _ => nettyResponse.addHeader(header.name, header.value)
        }
      }
      resp.entity.foreach { (contentType: ContentType, buffer: Array[Byte]) =>
        nettyResponse.addHeader("Content-Type", contentType.value)
        if (response.isInstanceOf[HttpResponse]) {
          nettyResponse.addHeader("Content-Length", buffer.length.toString)
        }
        nettyResponse.write(buffer)
        //nettyResponse.setContent(copiedBuffer(buffer))
      }
      complete
      None
    } catch {
      case e: IOException =>
        log.error("Could not write response body, probably the request has either timed out or the client has " +
          "disconnected\nRequest: {}\nResponse: {}\nError: {}", req, response, e)
        Some(e)
      case NonFatal(e) =>
        log.error("Could not complete request\nRequest: {}\nResponse: {}\nError: {}", req, response, e)
        Some(e)
    }
  }

  def timeoutResponse(request: HttpRequest): HttpResponse = HttpResponse(
    status = 500,
    entity = "Ooops! The server was not able to produce a timely response to your request.\n" +
      "Please try again in a short while!"
  )

  case class Closed(reason: ClosedEventReason) extends IOClosed
}

class FinagleConnectorSettings(config: Config) {

  protected val c: Config = config.getConfig("spray.finagle")

  val RequestTimeout       = c getMilliseconds "request-timeout"
  val TimeoutTimeout       = c getMilliseconds "timeout-timeout"
  val TimeoutHandler       = c getString       "timeout-handler"
  val RemoteAddressHeader  = c getBoolean      "remote-address-header"
  val VerboseErrorMessages = c getBoolean      "verbose-error-messages"
  val MaxContentLength     = c getBytes        "max-content-length"

  require(RequestTimeout >= 0, "request-timeout must be >= 0")
  require(TimeoutTimeout >= 0, "timeout-timeout must be >= 0")
  require(MaxContentLength > 0, "max-content-length must be > 0")

}

private[spray] object FinagleConverter {

  def toHttpRequest(nettyRequest: NettyHttpRequest)
                   (implicit settings: FinagleConnectorSettings, log: LoggingAdapter): HttpRequest = {

    import collection.JavaConverters._
    var contentType: ContentType = null
    var contentLength: Int = 0
    val rawHeaders = nettyRequest.getHeaderNames.asScala.toList.map { name =>
      val value = nettyRequest.getHeaders(name).asScala.mkString(", ")
      val lcName = name.toLowerCase
      lcName match {
        case "content-type" =>
          contentType = HttpParser.parseContentType(value) match {
            case Right(x) => x
            case Left(errorInfo) => throw new IllegalRequestException(BadRequest, errorInfo)
          }
        case "content-length" =>
          contentLength =
            try value.toInt
            catch {
              case e: NumberFormatException =>
                throw new IllegalRequestException(BadRequest, RequestErrorInfo("Illegal Content-Length", e.getMessage))
            }
        case _ =>
      }
      RawHeader(name, value)
    }
    HttpRequest(
      method = toHttpMethod(nettyRequest.getMethod.getName),
      uri = nettyRequest.getUri,
      headers = rawHeaders,
      entity = toHttpEntity(nettyRequest, contentType, contentLength),
      protocol = toHttpProtocol(nettyRequest.getProtocolVersion.toString)
    )

  }

  def toHttpMethod(name: String) =
    HttpMethods.getForKey(name)
      .getOrElse(throw new IllegalRequestException(MethodNotAllowed, RequestErrorInfo("Illegal HTTP method", name)))

  def toHttpProtocol(name: String) =
    HttpProtocols.getForKey(name)
      .getOrElse(throw new IllegalRequestException(BadRequest, "Illegal HTTP protocol", name))


  def toHttpEntity(nettyRequest: NettyHttpRequest, contentType: ContentType, contentLength: Int)
                  (implicit settings: FinagleConnectorSettings, log: LoggingAdapter): HttpEntity = {
    val input = nettyRequest.getContent
    def body: Array[Byte] = {
      if (contentLength > 0) {
        if (contentLength <= settings.MaxContentLength) {
          try {
            if (input.capacity() != contentLength) {
              throw new RequestProcessingException(InternalServerError, "Illegal request entity, " +
                "expected length " + contentLength + " but only has length " + input.capacity())
            }
            val buf = new Array[Byte](contentLength)
            input.getBytes(0, buf)
            buf
          } catch {
            case e: IOException =>
              log.error(e, "Could not read request entity")
              throw new RequestProcessingException(InternalServerError, "Could not read request entity")
          }
        } else throw new IllegalRequestException(RequestEntityTooLarge, "HTTP message Content-Length " +
          contentLength + " exceeds the configured limit of " + settings.MaxContentLength)
      } else Array[Byte]()
    }
    if (contentType == null) HttpEntity(body) else HttpBody(contentType, body)
  }



}

