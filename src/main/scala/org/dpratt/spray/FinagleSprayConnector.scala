package org.dpratt.spray

import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpResponseStatus, HttpRequest => NettyHttpRequest, HttpResponse => NettyHttpResponse}

import cc.spray.http._
import cc.spray.http.HttpHeaders._
import cc.spray.http.MediaTypes._
import cc.spray.{RequestResponder, RequestContext, SprayServerSettings}
import cc.spray.utils.ActorHelpers._

import java.io.IOException

import scala.collection.JavaConversions._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.Service
import cc.spray.utils.Logging
import com.twitter.util.{Future, Promise}

/**
 * A spray connector for Twitter's Finagle HTTP server. This component routes incoming HttpRequests into
 * the Spray RootService actor, along with a responder lambda suitable for completing the Finagle Future.
 *
 * @author David Pratt (david.pratt@gmail.com)
 */
class FinagleSprayConnector extends Service[NettyHttpRequest, NettyHttpResponse] with Logging {

  //TODO: This connector does not deal with timeouts at all - the assumption is that the Finagle server
  //that uses this service will do it's own timeout handling
  //It would not be difficult to add either another Finagle filter that handled sending messages to the timeout actor
  //or to just put that functionality here

  lazy val rootService = actor(SprayServerSettings.RootActorId)

  //This is the main entry point to Finagle - it will be called with each HTTP request
  //This method transforms the Netty request to a generic Spray request, creates a closure to do the final
  //completion of the request, and then dispatches into the async spray framework
  def apply(req: NettyHttpRequest) = {
    Future {
      val out = new Promise[NettyHttpResponse]
      try {
        //Create the responder
        //TODO - this responder does not support chunked responses - we could extend this to properly
        //chunk a response to Finagle
        val responder = RequestResponder(
          complete = {
            response =>
              try {
                //complete Finagle's future with the actual response
                out.setValue(nettyResponse(req, response))
              } catch {
                case e: IllegalStateException => log.error("Could not complete " + requestString(req) + ", it probably timed out and has therefore" +
                  "already been completed", e)
                case e: Exception => log.error("Could not complete " + requestString(req) + " due to {}", e)
              }
          },
          reject = _ => throw new IllegalStateException
        )
        val requestContext = sprayRequestContext(req, responder)
        //send off the request to the spray root actor
        requestContext.foreach(rootService ! _)
      } catch {
        case HttpException(failure, reason) => {
          out.setValue(blankNettyResponse(HttpResponseStatus.valueOf(failure.value), reason.getBytes))
        }
        case e: Exception => {
          out.setValue(blankNettyResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, ("Internal Server Error:\n" + e.toString).getBytes))
        }
      }
      out
    }.flatten
  }

  //convert a Netty http request into a spray http request
  private def sprayHttpRequest(req: NettyHttpRequest): HttpRequest = {
    val (contentTypeHeader, contentLengthHeader, regularHeaders) = HttpHeaders.parseFromRaw {
      req.getHeaderNames.toList.map {
        name =>
          name -> req.getHeaders(name).toList.mkString(", ")
      }
    }
    HttpRequest(
      method = HttpMethods.getForKey(req.getMethod.toString).get,
      uri = req.getUri,
      headers = regularHeaders,
      content = httpContent(req.getContent, contentTypeHeader, contentLengthHeader),
      protocol = HttpProtocols.getForKey(req.getProtocolVersion.toString).get
    )
  }

  //create a spray HttpContent from the specified parameters
  private def httpContent(input: ChannelBuffer, contentTypeHeader: Option[`Content-Type`],
                  contentLengthHeader: Option[`Content-Length`]): Option[HttpContent] = {
    contentLengthHeader.flatMap {
      case `Content-Length`(0) => None
      case `Content-Length`(contentLength) => {
        val body = if (contentLength == 0) {
          cc.spray.utils.EmptyByteArray
        } else {
          if (input.capacity() != contentLength) {
            throw new HttpException(StatusCodes.BadRequest, "Illegal request entity, expected length " +
              contentLength + " but body has length " + input.capacity())
          }
          val buf = new Array[Byte](contentLength)
          input.getBytes(0, buf)
          buf
        }
        val contentType = contentTypeHeader.map(_.contentType).getOrElse(ContentType(`application/octet-stream`))
        Some(HttpContent(contentType, body))
      }
    }
  }

  //create a top-level Spray RequestContext object from a netty HTTP request and a specified responder function
  private def sprayRequestContext(req: NettyHttpRequest, responder: RequestResponder): Option[RequestContext] = {
    Some {
      RequestContext(
        request = sprayHttpRequest(req),
        remoteHost = "0.0.0.0", //TODO: Fix this!
        responder = responder
      )
    }
  }

  //create a netty HTTP response from a generic spray HTTP response
  private def nettyResponse(nettyRequest: NettyHttpRequest, sprayResponse: HttpResponse): NettyHttpResponse = {
    val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.valueOf(sprayResponse.status.value))

    try {
      sprayResponse.headers.foreach(header => response.addHeader(header.name, header.value))
      sprayResponse.content.foreach {
        content =>
          response.addHeader("Content-Type", content.contentType.value)
          response.addHeader("Content-Length", content.buffer.length.toString)
          response.setContent(copiedBuffer(content.buffer))
      }
      response
    } catch {
      case e: IOException => errorResponse("Could not write response body of " + requestString(nettyRequest) + ", probably the request has either timed out" +
        "or the client has disconnected", e)
      case e: Exception => errorResponse("Could not complete " + requestString(nettyRequest), e)
    }
  }

  private def blankNettyResponse(status: HttpResponseStatus, content: Array[Byte]): NettyHttpResponse = {
    val response = new DefaultHttpResponse(HTTP_1_1, status)
    response.setHeader("Cache-Control", "no-cache")
    response.setContent(copiedBuffer(content))
    response
  }

  private def errorResponse(message: String, exception: Throwable): NettyHttpResponse = {
    log.error(message, exception)
    blankNettyResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, message.getBytes)
  }

  def requestString(req: NettyHttpRequest) = req.getMethod.toString + " request to '" + req.getUri + "'"


}
