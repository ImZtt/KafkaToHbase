package com.dinglicom.streaming
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Table, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
/**
 * 每分钟启动把hfile加载进hbase里
 */
object HfileLoad {
  def main(args: Array[String]): Unit = {
  //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val hconf = HBaseConfiguration.create()
    val load = new LoadIncrementalHFiles(hconf)
    //hbase的表名
    val tableName = "lte_xdr"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(hconf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val time = new SimpleDateFormat("yyyyMMddhhmm").format(new Date())
    try {
      //获取hbase表的region分布
      val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(hconf)
      //设置job名称
      job.setJobName("DumpHFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      //开始导入
      load.doBulkLoad(new Path("/user/cloudil/hbase/"+time+"/"), table.asInstanceOf[HTable])
    } finally {
      table.close()
      conn.close()
    }
  }
}