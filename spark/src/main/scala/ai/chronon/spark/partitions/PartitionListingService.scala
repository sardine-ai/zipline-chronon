package ai.chronon.spark.partitions

import ai.chronon.api.{PartitionRange, PartitionSpec, Window}
import ai.chronon.api.Extensions.{WindowUtils, WindowOps}
import ai.chronon.spark.catalog.TableUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

case class PartitionListResponse(
    tableName: String,
    partitions: List[String],
    success: Boolean,
    message: Option[String] = None
)

class PartitionListingService(sparkSession: SparkSession, port: Int = 8080) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT, true)
  
  private val tableUtils = new TableUtils(sparkSession)

  private class PartitionHandler extends SimpleChannelInboundHandler[HttpObject] {
    
    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      ctx.flush()
    }

    private def parseQueryParams(uri: String): Map[String, String] = {
      uri.split("\\?", 2) match {
        case Array(_, queryString) =>
          queryString.split("&")
            .filter(_.nonEmpty)
            .map(_.split("=", 2))
            .collect { case Array(key, value) => key -> java.net.URLDecoder.decode(value, "UTF-8") }
            .toMap
        case _ => Map.empty
      }
    }

    private def createPartitionSpec(queryParams: Map[String, String]): PartitionSpec = {
      val column = queryParams.getOrElse("partitionColumn", tableUtils.partitionColumn)
      val format = queryParams.getOrElse("partitionFormat", "yyyy-MM-dd")
      val interval = queryParams.get("partitionInterval") match {
        case Some("hourly") => WindowUtils.Hour.millis
        case Some("daily") => WindowUtils.Day.millis
        case Some(millis) if millis.forall(_.isDigit) => millis.toLong
        case _ => WindowUtils.Day.millis // default to daily
      }
      PartitionSpec(column, format, interval)
    }

    private def listPartitions(tableName: String, queryParams: Map[String, String]): PartitionListResponse = {
      Try {
        val subPartitionsFilter = queryParams.collect {
          case (key, value) if key.startsWith("filter.") => key.substring(7) -> value
        }
        
        val partitionSpec = createPartitionSpec(queryParams)
        val partitionColumnName = partitionSpec.column
        
        val partitionRange = for {
          startDate <- queryParams.get("startDate")
          endDate <- queryParams.get("endDate")
        } yield {
          implicit val implicitPartitionSpec: PartitionSpec = partitionSpec
          PartitionRange(startDate, endDate)
        }

        val partitions = tableUtils.partitions(
          tableName = tableName,
          subPartitionsFilter = subPartitionsFilter,
          partitionRange = partitionRange,
          partitionColumnName = partitionColumnName
        )

        PartitionListResponse(tableName, partitions, success = true)
      }.recover {
        case exception =>
          logger.error(s"Failed to list partitions for table $tableName", exception)
          PartitionListResponse(tableName, List.empty, success = false, Some(s"Error: ${exception.getMessage}"))
      }.get
    }

    override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject): Unit = {
      msg match {
        case request: HttpRequest =>
          val uri = request.uri()
          val start = System.currentTimeMillis()
          
          val (status, content) = uri match {
            case u if u.startsWith("/health") =>
              (HttpResponseStatus.OK, """{"status": "healthy", "service": "partition-listing"}""")
              
            case u if u.startsWith("/api/partitions/") =>
              u.split("/") match {
                case parts if parts.length >= 4 =>
                  val tableName = parts(3).split("\\?")(0)
                  val queryParams = parseQueryParams(u)
                  val response = listPartitions(tableName, queryParams)
                  (HttpResponseStatus.OK, mapper.writeValueAsString(response))
                case _ =>
                  (HttpResponseStatus.BAD_REQUEST, """{"error": "Invalid table name in path"}""")
              }
              
            case "/api/partitions" =>
              (HttpResponseStatus.BAD_REQUEST, """{"error": "Table name is required. Use /api/partitions/{tableName}"}""")
              
            case "/api/info" =>
              val info = Map(
                "service" -> "chronon-partition-listing",
                "version" -> "1.0.0",
                "sparkVersion" -> sparkSession.version,
                "endpoints" -> List(
                  "/health - Health check",
                  "/api/partitions/{tableName} - List partitions for a table",
                  "/api/info - Service information"
                )
              )
              (HttpResponseStatus.OK, mapper.writeValueAsString(info))
              
            case _ =>
              (HttpResponseStatus.NOT_FOUND, """{"error": "Endpoint not found"}""")
          }
          
          val end = System.currentTimeMillis()
          logger.info(s"Request $uri took ${end - start}ms, status: $status")

          val response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            Unpooled.copiedBuffer(content, CharsetUtil.UTF_8)
          )

          response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
            .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
            .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
            .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type")

          if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
          }

          ctx.write(response)
          
        case _ =>
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      logger.error("Exception in partition listing handler", cause)
      ctx.close()
    }
  }

  def start(): Unit = {
    try {
      val bootstrap = new ServerBootstrap()
      bootstrap.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            val pipeline = ch.pipeline()
            pipeline.addLast(new HttpServerCodec())
            pipeline.addLast(new HttpObjectAggregator(65536))
            pipeline.addLast(new PartitionHandler())
          }
        })
        .option[Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

      val future = bootstrap.bind(port).sync()
      logger.info(s"Partition Listing Service started at http://localhost:$port")
      logger.info(s"Available endpoints:")
      logger.info(s"  GET /health - Health check")
      logger.info(s"  GET /api/partitions/{{tableName}} - List partitions")
      logger.info(s"  GET /api/info - Service information")
      
      future.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }
}

object PartitionListingService {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Chronon Partition Listing Service")
      .master("local[*]")
      .getOrCreate()
      
    val port = if (args.length > 0) args(0).toInt else 8080
    
    val service = new PartitionListingService(spark, port)
    service.start()
  }
}
