package com.dinglicom.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext

object LteUserSubject {

  def main(args: Array[String]) {

    val jobName = "lte_user_counter"

    val conf = new SparkConf().setAppName(jobName)
    val context = new StreamingContext(conf, Seconds(300))

    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr", "metadata.broker.list" -> "172.16.30.101:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "smallest")

    val topics = Set("lteu1_http")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics);

    var cc = kafkaStream.map(x => {
      var tmpValues = x._2.split("\\|");

      var xdrInterface = tmpValues(1).toInt
      var processtype = tmpValues(23).toInt

      var citycode = tmpValues(91)

      var sgw = tmpValues(11).toLong
      var enodeb = tmpValues(15).toLong
      var lac = tmpValues(20).toLong
      var cell = tmpValues(21).toLong
      var apptype_whole = tmpValues(27).toLong
      var apptype = tmpValues(28).toLong
      var tac8 = tmpValues(90).toLong

      var appstore_dl_500kbps_cnt = 0L;
      var appstore_dl_500kbps_flow = 0L;
      var appstore_dl_duration = 0L;
      var appstore_dl_flow = 0L;
      var appstore_req_cnt = 0L;
      var appstore_succ_cnt = 0L;
      var bigpage_dn_duration = 0L;
      var bigpage_dn_flow = 0L;
      var bigpage_show_delay = 0L;
      var bigpage_show_delay_cnt = 0L;
      var bigweb_resp_delay = 0L;
      var bigweb_resp_succ_cnt = 0L;
      var dns_delay = 0L;
      var dns_delay_cnt = 0L;
      var dns_dl_flow = 0L;
      var dns_dl_ip_fragpacket_num = 0L;
      var dns_dl_ip_packet_num = 0L;
      var dns_dl_online_duration = 0L;
      var dns_dl_tcp_disordpacket_num = 0L;
      var dns_dl_tcp_packet_num = 0L;
      var dns_dl_tcp_retranspacket_num = 0L;
      var dns_fail_cnt = 0L;
      var dns_query_cnt = 0L;
      var dns_succ_cnt = 0L;
      var dns_tcp_dl_flow = 0L;
      var dns_tcp_linkack_cnt = 0L;
      var dns_tcp_linkack_delay = 0L;
      var dns_tcp_linkresp_cnt = 0L;
      var dns_tcp_linkresp_delay = 0L;
      var dns_tcp_link_req_cnt = 0L;
      var dns_tcp_ul_flow = 0L;
      var dns_timeout_cnt = 0L;
      var dns_udp_dl_flow = 0L;
      var dns_udp_ul_flow = 0L;
      var dns_ul_flow = 0L;
      var dns_ul_ip_fragpacket_num = 0L;
      var dns_ul_ip_packet_num = 0L;
      var dns_ul_online_duration = 0L;
      var dns_ul_tcp_disordpacket_num = 0L;
      var dns_ul_tcp_packet_num = 0L;
      var dns_ul_tcp_retranspacket_num = 0L;
      var email_dl_flow = 0L;
      var email_dl_ip_fragpacket_num = 0L;
      var email_dl_ip_packet_num = 0L;
      var email_dl_online_duration = 0L;
      var email_dl_tcp_disordpacket_num = 0L;
      var email_dl_tcp_packet_num = 0L;
      var email_dl_tcp_retranspacket_num = 0L;
      var email_tcp_dl_flow = 0L;
      var email_tcp_linkack_cnt = 0L;
      var email_tcp_linkack_delay = 0L;
      var email_tcp_linkresp_cnt = 0L;
      var email_tcp_linkresp_delay = 0L;
      var email_tcp_link_req_cnt = 0L;
      var email_tcp_ul_flow = 0L;
      var email_udp_dl_flow = 0L;
      var email_udp_ul_flow = 0L;
      var email_ul_flow = 0L;
      var email_ul_ip_fragpacket_num = 0L;
      var email_ul_ip_packet_num = 0L;
      var email_ul_online_duration = 0L;
      var email_ul_tcp_disordpacket_num = 0L;
      var email_ul_tcp_packet_num = 0L;
      var email_ul_tcp_retranspacket_num = 0L;
      var emial_imap_dl_flow = 0L;
      var emial_imap_ul_flow = 0L;
      var emial_pop3_dl_flow = 0L;
      var emial_pop3_ul_flow = 0L;
      var emial_smtp_dl_flow = 0L;
      var emial_smtp_ul_flow = 0L;
      var firstfin_ack_delay = 0L;
      var firstfin_ack_num = 0L;
      var ftp_conn_cnt = 0L;
      var ftp_dl_flow = 0L;
      var ftp_dl_ip_fragpacket_num = 0L;
      var ftp_dl_ip_packet_num = 0L;
      var ftp_dl_online_duration = 0L;
      var ftp_dl_tcp_disordpacket_num = 0L;
      var ftp_dl_tcp_packet_num = 0L;
      var ftp_dl_tcp_retranspacket_num = 0L;
      var ftp_fail_cnt = 0L;
      var ftp_resp_delay = 0L;
      var ftp_resp_delay_cnt = 0L;
      var ftp_succ_cnt = 0L;
      var ftp_tcp_dl_flow = 0L;
      var ftp_tcp_linkack_cnt = 0L;
      var ftp_tcp_linkack_delay = 0L;
      var ftp_tcp_linkresp_cnt = 0L;
      var ftp_tcp_linkresp_delay = 0L;
      var ftp_tcp_link_req_cnt = 0L;
      var ftp_tcp_ul_flow = 0L;
      var ftp_transfer_duration = 0L;
      var ftp_transfer_file = 0L;
      var ftp_udp_dl_flow = 0L;
      var ftp_udp_ul_flow = 0L;
      var ftp_ul_flow = 0L;
      var ftp_ul_ip_fragpacket_num = 0L;
      var ftp_ul_ip_packet_num = 0L;
      var ftp_ul_online_duration = 0L;
      var ftp_ul_tcp_disordpacket_num = 0L;
      var ftp_ul_tcp_packet_num = 0L;
      var ftp_ul_tcp_retranspacket_num = 0L;
      var game_businessvisit_req_cnt = 0L;
      var game_businessvisit_succ_cnt = 0L;
      var himcom_dn_online_duration = 0L;
      var http_dl_100k_duration = 0L;
      var http_dl_100k_flow = 0L;
      var http_dl_30k_duration = 0L;
      var http_dl_30k_flow = 0L;
      var http_dl_500kbps_duration = 0L;
      var http_dl_500kbps_flow = 0L;
      var http_dl_50k_duration = 0L;
      var http_dl_50k_flow = 0L;
      var http_dl_disord_flow = 0L;
      var http_dl_flow = 0L;
      var http_dl_ip_fragpacket_num = 0L;
      var http_dl_ip_packet_num = 0L;
      var http_dl_online_duration = 0L;
      var http_dl_retrans_flow = 0L;
      var http_dl_tcp_disordpacket_num = 0L;
      var http_dl_tcp_packet_num = 0L;
      var http_dl_tcp_retranspacket_num = 0L;
      var http_fail_cnt = 0L;
      var http_firstresp_delay = 0L;
      var http_firstresp_delay_cnt = 0L;
      var http_interpt_cnt = 0L;
      var http_req_cnt = 0L;
      var http_resp_200_300_cnt = 0L;
      var http_resp_300_400_cnt = 0L;
      var http_resp_400_500_cnt = 0L;
      var http_resp_big500_cnt = 0L;
      var http_resp_delay = 0L;
      var http_resp_equ200_cnt = 0L;
      var http_resp_less200_cnt = 0L;
      var http_resp_no_cnt = 0L;
      var http_resp_succ_cnt = 0L;
      var http_succ_cnt = 0L;
      var http_tcp_dl_flow = 0L;
      var http_tcp_linkack_cnt = 0L;
      var http_tcp_linkack_delay = 0L;
      var http_tcp_linkresp_cnt = 0L;
      var http_tcp_linkresp_delay = 0L;
      var http_tcp_link_req_cnt = 0L;
      var http_tcp_ul_flow = 0L;
      var http_udp_dl_flow = 0L;
      var http_udp_ul_flow = 0L;
      var http_ul_flow = 0L;
      var http_ul_ip_fragpacket_num = 0L;
      var http_ul_ip_packet_num = 0L;
      var http_ul_online_duration = 0L;
      var http_ul_tcp_disordpacket_num = 0L;
      var http_ul_tcp_packet_num = 0L;
      var http_ul_tcp_retranspacket_num = 0L;
      var imap_dl_flow = 0L;
      var imap_ul_flow = 0L;
      var imcom_businessvisit_req_cnt = 0L;
      var imcom_businessvisit_succ_cnt = 0L;
      var imcom_conn_delay = 0L;
      var imcom_dn_flow = 0L;
      var imcom_dn_speed_200kbps_cnt = 0L;
      var imcom_interpt_cnt = 0L;
      var imcom_receiveimg_duration = 0L;
      var imcom_receiveimg_flow = 0L;
      var imcom_receiveimg_req_cnt = 0L;
      var imcom_receiveimg_succ_cnt = 0L;
      var imcom_receivemusic_duration = 0L;
      var imcom_receivemusic_flow = 0L;
      var imcom_receivemusic_req_cnt = 0L;
      var imcom_receivemusic_succ_cnt = 0L;
      var imcom_receivetext_req_cnt = 0L;
      var imcom_receivetext_succ_cnt = 0L;
      var imcom_receivevideo_duration = 0L;
      var imcom_receivevideo_flow = 0L;
      var imcom_receivevideo_req_cnt = 0L;
      var imcom_receivevideo_succ_cnt = 0L;
      var imcom_req_cnt = 0L;
      var imcom_sendimg_duration = 0L;
      var imcom_sendimg_flow = 0L;
      var imcom_sendimg_req_cnt = 0L;
      var imcom_sendimg_succ_cnt = 0L;
      var imcom_sendmusic_duration = 0L;
      var imcom_sendmusic_flow = 0L;
      var imcom_sendmusic_req_cnt = 0L;
      var imcom_sendmusic_succ_cnt = 0L;
      var imcom_sendtext_req_cnt = 0L;
      var imcom_sendtext_succ_cnt = 0L;
      var imcom_sendvideo_duration = 0L;
      var imcom_sendvideo_flow = 0L;
      var imcom_sendvideo_req_cnt = 0L;
      var imcom_sendvideo_succ_cnt = 0L;
      var imcom_succ_cnt = 0L;
      var imcom_up_flow = 0L;
      var imcom_up_online_duration = 0L;
      var imcom_up_speed_50kbps_cnt = 0L;
      var im_dl_flow = 0L;
      var im_dl_ip_fragpacket_num = 0L;
      var im_dl_ip_packet_num = 0L;
      var im_dl_online_duration = 0L;
      var im_dl_tcp_disordpacket_num = 0L;
      var im_dl_tcp_packet_num = 0L;
      var im_dl_tcp_retranspacket_num = 0L;
      var im_tcp_dl_flow = 0L;
      var im_tcp_linkack_cnt = 0L;
      var im_tcp_linkack_delay = 0L;
      var im_tcp_linkresp_cnt = 0L;
      var im_tcp_linkresp_delay = 0L;
      var im_tcp_link_req_cnt = 0L;
      var im_tcp_ul_flow = 0L;
      var im_udp_dl_flow = 0L;
      var im_udp_ul_flow = 0L;
      var im_ul_flow = 0L;
      var im_ul_ip_fragpacket_num = 0L;
      var im_ul_ip_packet_num = 0L;
      var im_ul_online_duration = 0L;
      var im_ul_tcp_disordpacket_num = 0L;
      var im_ul_tcp_packet_num = 0L;
      var im_ul_tcp_retranspacket_num = 0L;
      var mms_dl_ip_fragpacket_num = 0L;
      var mms_dl_ip_packet_num = 0L;
      var mms_dl_online_duration = 0L;
      var mms_dl_tcp_disordpacket_num = 0L;
      var mms_dl_tcp_packet_num = 0L;
      var mms_dl_tcp_retranspacket_num = 0L;
      var mms_mo_delay = 0L;
      var mms_mo_delay_cnt = 0L;
      var mms_mo_dl_flow = 0L;
      var mms_mo_duration = 0L;
      var mms_mo_fail_cnt = 0L;
      var mms_mo_req_cnt = 0L;
      var mms_mo_succ_cnt = 0L;
      var mms_mo_ul_flow = 0L;
      var mms_mt_delay = 0L;
      var mms_mt_delay_cnt = 0L;
      var mms_mt_dl_flow = 0L;
      var mms_mt_duration = 0L;
      var mms_mt_fail_cnt = 0L;
      var mms_mt_req_cnt = 0L;
      var mms_mt_succ_cnt = 0L;
      var mms_mt_ul_flow = 0L;
      var mms_tcp_dl_flow = 0L;
      var mms_tcp_linkack_cnt = 0L;
      var mms_tcp_linkack_delay = 0L;
      var mms_tcp_linkresp_cnt = 0L;
      var mms_tcp_linkresp_delay = 0L;
      var mms_tcp_link_req_cnt = 0L;
      var mms_tcp_ul_flow = 0L;
      var mms_udp_dl_flow = 0L;
      var mms_udp_ul_flow = 0L;
      var mms_ul_ip_fragpacket_num = 0L;
      var mms_ul_ip_packet_num = 0L;
      var mms_ul_online_duration = 0L;
      var mms_ul_tcp_disordpacket_num = 0L;
      var mms_ul_tcp_packet_num = 0L;
      var mms_ul_tcp_retranspacket_num = 0L;
      var p2p_dl_flow = 0L;
      var p2p_dl_ip_fragpacket_num = 0L;
      var p2p_dl_ip_packet_num = 0L;
      var p2p_dl_online_duration = 0L;
      var p2p_dl_tcp_disordpacket_num = 0L;
      var p2p_dl_tcp_packet_num = 0L;
      var p2p_dl_tcp_retranspacket_num = 0L;
      var p2p_tcp_dl_flow = 0L;
      var p2p_tcp_linkack_cnt = 0L;
      var p2p_tcp_linkack_delay = 0L;
      var p2p_tcp_linkresp_cnt = 0L;
      var p2p_tcp_linkresp_delay = 0L;
      var p2p_tcp_link_req_cnt = 0L;
      var p2p_tcp_ul_flow = 0L;
      var p2p_udp_dl_flow = 0L;
      var p2p_udp_ul_flow = 0L;
      var p2p_ul_flow = 0L;
      var p2p_ul_ip_fragpacket_num = 0L;
      var p2p_ul_ip_packet_num = 0L;
      var p2p_ul_online_duration = 0L;
      var p2p_ul_tcp_disordpacket_num = 0L;
      var p2p_ul_tcp_packet_num = 0L;
      var p2p_ul_tcp_retranspacket_num = 0L;
      var page_dn_duration = 0L;
      var page_dn_flow = 0L;
      var page_req_cnt = 0L;
      var page_resp_delay = 0L;
      var page_resp_succ_cnt = 0L;
      var page_show_delay = 0L;
      var page_show_succ_cnt = 0L;
      var pop3_dl_flow = 0L;
      var pop3_ul_flow = 0L;
      var rtsp_audio_flow = 0L;
      var rtsp_dl_flow = 0L;
      var rtsp_dl_ip_fragpacket_num = 0L;
      var rtsp_dl_ip_packet_num = 0L;
      var rtsp_dl_online_duration = 0L;
      var rtsp_dl_tcp_disordpacket_num = 0L;
      var rtsp_dl_tcp_packet_num = 0L;
      var rtsp_dl_tcp_retranspacket_num = 0L;
      var rtsp_resp_delay = 0L;
      var rtsp_resp_delay_cnt = 0L;
      var rtsp_tcp_dl_flow = 0L;
      var rtsp_tcp_linkack_cnt = 0L;
      var rtsp_tcp_linkack_delay = 0L;
      var rtsp_tcp_linkresp_cnt = 0L;
      var rtsp_tcp_linkresp_delay = 0L;
      var rtsp_tcp_link_req_cnt = 0L;
      var rtsp_tcp_ul_flow = 0L;
      var rtsp_udp_dl_flow = 0L;
      var rtsp_udp_ul_flow = 0L;
      var rtsp_ul_flow = 0L;
      var rtsp_ul_ip_fragpacket_num = 0L;
      var rtsp_ul_ip_packet_num = 0L;
      var rtsp_ul_online_duration = 0L;
      var rtsp_ul_tcp_disordpacket_num = 0L;
      var rtsp_ul_tcp_packet_num = 0L;
      var rtsp_ul_tcp_retranspacket_num = 0L;
      var rtsp_video_flow = 0L;
      var s1u_dl_flow = 0L;
      var s1u_dl_ip_fragpacket_num = 0L;
      var s1u_dl_ip_packet_num = 0L;
      var s1u_dl_online_duration = 0L;
      var s1u_dl_tcp_disordpacket_num = 0L;
      var s1u_dl_tcp_packet_num = 0L;
      var s1u_dl_tcp_retranspacket_num = 0L;
      var s1u_tcp_dl_flow = 0L;
      var s1u_tcp_linkack_cnt = 0L;
      var s1u_tcp_linkack_delay = 0L;
      var s1u_tcp_linkresp_cnt = 0L;
      var s1u_tcp_linkresp_delay = 0L;
      var s1u_tcp_link_req_cnt = 0L;
      var s1u_tcp_ul_flow = 0L;
      var s1u_udp_dl_flow = 0L;
      var s1u_udp_ul_flow = 0L;
      var s1u_ul_flow = 0L;
      var s1u_ul_ip_fragpacket_num = 0L;
      var s1u_ul_ip_packet_num = 0L;
      var s1u_ul_online_duration = 0L;
      var s1u_ul_tcp_disordpacket_num = 0L;
      var s1u_ul_tcp_packet_num = 0L;
      var s1u_ul_tcp_retranspacket_num = 0L;
      var secondfin_ack_delay = 0L;
      var secondfin_ack_num = 0L;
      var session_resp_succ_cnt = 0L;
      var session_timeout_cnt = 0L;
      var sess_fail_badprocess_cnt = 0L;
      var sess_fail_noresp_cnt = 0L;
      var sess_fail_resp_fail_cnt = 0L;
      var smtp_dl_flow = 0L;
      var smtp_ul_flow = 0L;
      var tac_cnt = 0L;
      var tcp_fail_cnt = 0L;
      var tcp_req_cnt = 0L;
      var tcp_resp_cnt = 0L;
      var total_http_duration = 0L;
      var total_wap_duration = 0L;
      var user_cnt = 0L;
      var video_500ms_cnt = 0L;
      var video_ave_pause_cnt = 0L;
      var video_dl_duration = 0L;
      var video_dl_flow = 0L;
      var video_dn_delay_500kbps_cnt = 0L;
      var video_dn_speed_500kbps_cnt = 0L;
      var video_interpt_cnt = 0L;
      var video_req_cnt = 0L;
      var video_succ_cnt = 0L;
      var video_wait_delay = 0L;
      var voip_dl_flow = 0L;
      var voip_dl_ip_fragpacket_num = 0L;
      var voip_dl_ip_packet_num = 0L;
      var voip_dl_online_duration = 0L;
      var voip_dl_tcp_disordpacket_num = 0L;
      var voip_dl_tcp_packet_num = 0L;
      var voip_dl_tcp_retranspacket_num = 0L;
      var voip_tcp_dl_flow = 0L;
      var voip_tcp_linkack_cnt = 0L;
      var voip_tcp_linkack_delay = 0L;
      var voip_tcp_linkresp_cnt = 0L;
      var voip_tcp_linkresp_delay = 0L;
      var voip_tcp_link_req_cnt = 0L;
      var voip_tcp_ul_flow = 0L;
      var voip_udp_dl_flow = 0L;
      var voip_udp_ul_flow = 0L;
      var voip_ul_flow = 0L;
      var voip_ul_ip_fragpacket_num = 0L;
      var voip_ul_ip_packet_num = 0L;
      var voip_ul_online_duration = 0L;
      var voip_ul_tcp_disordpacket_num = 0L;
      var voip_ul_tcp_packet_num = 0L;
      var voip_ul_tcp_retranspacket_num = 0L;
      var wap_ack_delay = 0L;
      var wap_ack_delay_cnt = 0L;
      var wap_dl_flow = 0L;
      var wap_fail_cnt = 0L;
      var wap_firstack_delay = 0L;
      var wap_firstack_delay_cnt = 0L;
      var wap_req_cnt = 0L;
      var wap_succ_cnt = 0L;
      var wap_ul_flow = 0L;
      var web_dn_duration = 0L;
      var web_dn_flow = 0L;
      var web_dn_speed_200kbps_cnt = 0L;
      var web_estab_fail_cnt = 0L;
      var web_interpt_cnt = 0L;
      var web_open_toolong_cnt = 0L;
      var web_req_cnt = 0L;
      var web_resp_delay = 0L;
      var web_resp_delay_300ms_cnt = 0L;
      var web_resp_succ_cnt = 0L;
      var web_show_delay = 0L;
      var web_show_delay3s_cnt = 0L;
      var web_show_succ_cnt = 0L;
      var web_speed_tooslow_cnt = 0L;
      var weixin_biz_flow = 0L;
      var weixin_dl_music_flow = 0L;
      var weixin_dl_text_flow = 0L;
      var weixin_heart_cnt = 0L;
      var weixin_ul_music_flow = 0L;
      var weixin_ul_text_flow = 0L;

      if (xdrInterface == 11 && processtype == 103) // lteu1_http_zc
      {
        var u8httpversion = tmpValues(63).toInt //u8HttpVersion
        var dl_flow = tmpValues(39).toLong //u32DlTraffic"
        var ul_flow = tmpValues(38).toLong //u32UlTraffic"
        var u16httpwapstat = tmpValues(65).toInt //u16HttpWapStat"
        var u32endtime = tmpValues(93).toLong //u32EndTime"
        var u32begintime = tmpValues(92).toLong //u32BeginTime"

        if (u8httpversion <= 4) {
          http_req_cnt = 1;
          http_dl_flow = dl_flow
          http_ul_flow = ul_flow
        }

        if (u8httpversion <= 4 && u16httpwapstat >= 200 && u16httpwapstat <= 399) {
          http_succ_cnt = 1;
          total_http_duration = u32endtime - u32begintime;
        }
        if (u8httpversion >= 400) {
          http_fail_cnt = 1;
        }

        var u32contentlength = tmpValues(76).toLong //u32ContentLength"
        if (u8httpversion <= 400 && dl_flow < u32contentlength) {
          http_interpt_cnt = 1;
        }
        var u32httplastpackettime = tmpValues(67).toLong //u32HttpLastPacketTime"
        if (ul_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          http_ul_online_duration = u32httplastpackettime;
        }
        if (dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          http_dl_online_duration = u32httplastpackettime;
        }
        http_dl_ip_fragpacket_num = tmpValues(49).toLong //u32DlIpFragPackets"
        http_dl_ip_packet_num = tmpValues(41).toLong //u32DlIpPacketNum"

        var u32dltcpretranspacketnum = tmpValues(45).toLong //u32DlTCPReTransPacketNum"
        if (u32dltcpretranspacketnum > 0) {
          http_dl_retrans_flow = dl_flow;
        }
        var u32dltcpdisorderpacketnum = tmpValues(43).toLong //u32DlTCPDisorderPacketNum"
        if (u32dltcpdisorderpacketnum > 0) {
          http_dl_disord_flow = dl_flow;
        }
        var u8l4protocol = tmpValues(34).toInt //u8L4Protocol"
        var u8tcpretrytimes = tmpValues(54).toInt //u8TCPRetryTimes"
        if (u8l4protocol == 0) {
          http_dl_tcp_packet_num = http_dl_ip_packet_num;
        }
        http_dl_tcp_disordpacket_num = u32dltcpdisorderpacketnum;
        http_dl_tcp_retranspacket_num = u32dltcpretranspacketnum;

        var u32ulipfragpackets = tmpValues(48).toLong //u32UlIpFragPackets"
        var u32ulippacketnum = tmpValues(40).toLong //u32UlIpPacketNum"
        http_ul_ip_fragpacket_num = u32ulipfragpackets;
        http_ul_ip_packet_num = u32ulippacketnum;
        if (u8l4protocol == 0) {
          http_ul_tcp_packet_num = u32ulippacketnum;
          http_tcp_dl_flow = dl_flow;
          http_tcp_ul_flow = ul_flow;
          http_tcp_link_req_cnt = u8tcpretrytimes;
        }

        http_ul_tcp_disordpacket_num = tmpValues(42).toLong //u32UlTCPDisorderPacketNum"
        http_ul_tcp_retranspacket_num = tmpValues(44).toLong //u32UlTCPReTransPacketNum"
        if (u8l4protocol == 1) {
          http_udp_dl_flow = dl_flow;
          http_udp_ul_flow = ul_flow;
        }
        var u8tcpstat = tmpValues(55).toInt //u8TCPStat"
        var u32tcpconstructlinkresptime = tmpValues(46).toLong //u32TCPConstructLinkRespTime"
        var u32tcpconstructlinkacktime = tmpValues(47).toLong //u32TCPConstructLinkAckTime"
        if (u8l4protocol == 0 && u8tcpstat == 0 && u32tcpconstructlinkresptime > 0
          && u32tcpconstructlinkresptime < 65535 && u32tcpconstructlinkacktime > 0
          && u32tcpconstructlinkacktime < 65535) {
          http_tcp_linkack_cnt = 1;
          http_tcp_linkack_delay = u32tcpconstructlinkacktime;
        }

        if (u8l4protocol == 0 && u8tcpstat == 0 && u32tcpconstructlinkresptime > 0
          && u32tcpconstructlinkresptime < 65535) {
          http_tcp_linkresp_cnt = 1;
          http_tcp_linkresp_delay = u32tcpconstructlinkresptime;
        }
        if (u16httpwapstat == 200) {
          http_resp_equ200_cnt = 1;
        }
        if (u16httpwapstat < 200) {
          http_resp_less200_cnt = 1;
        }
        if (u16httpwapstat > 200 && u16httpwapstat <= 300) {
          http_resp_200_300_cnt = 1;
        }
        if (u16httpwapstat > 300 && u16httpwapstat <= 400) {
          http_resp_300_400_cnt = 1;
        }
        if (u16httpwapstat > 400 && u16httpwapstat <= 500) {
          http_resp_400_500_cnt = 1;
        }
        if (u16httpwapstat > 500) {
          http_resp_big500_cnt = 1;
        }
        if (u16httpwapstat == 0) {
          http_resp_no_cnt = 1;
        }

        //long u32httplastpackettime = //u32Httplastpackettime"
        if (u8l4protocol == 0) {
          tcp_req_cnt = 1;
        }
        if (u8l4protocol == 0 && u8tcpstat == 0) {
          tcp_resp_cnt = 1;
        }

        if (u8l4protocol == 0 && u8tcpstat == 1) {
          tcp_fail_cnt = 1;
        }

        if (dl_flow > 30720 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_30k_duration = u32httplastpackettime;
        }
        if (dl_flow > 30720 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_30k_flow = dl_flow;
        }
        if (dl_flow > 51200 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_50k_duration = dl_flow;
        }
        if (dl_flow > 51200 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_50k_flow = dl_flow;
        }
        if (dl_flow > 102400 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_100k_duration = u32httplastpackettime;
        }
        if (dl_flow > 102400 && u32httplastpackettime > 0 && u32httplastpackettime < 120000) {
          http_dl_100k_flow = dl_flow;
        }
        if (dl_flow > u32httplastpackettime * 64) {
          http_dl_500kbps_duration = u32httplastpackettime;
        }
        if (dl_flow > u32httplastpackettime * 64) {
          http_dl_500kbps_flow = dl_flow;
        }

        var u16apptypewhole = tmpValues(27).toInt //u16AppTypeWhole"
        var u16apptype = tmpValues(28).toInt //u16AppType"
        if (u16apptypewhole == 15 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          page_dn_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 15 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          page_dn_flow = dl_flow;
        }
        var u32httpfirstrespondtime = tmpValues(66).toLong //u32HttpFirstRespondTime"

        if (u16apptypewhole == 15 && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 50000) {
          page_req_cnt = 1;
        }
        if (u16apptypewhole == 15 && u16httpwapstat == 200 && u32httpfirstrespondtime > 0
          && u32httpfirstrespondtime < 50000) {
          page_resp_succ_cnt = 1;
        }
        if (u16apptypewhole == 15 && u16httpwapstat == 200 && u32httpfirstrespondtime > 0
          && u32httpfirstrespondtime < 50000) {
          page_resp_delay = u32httpfirstrespondtime;
        }
        if (u16apptypewhole == 15 && u16httpwapstat == 200 && u32httpfirstrespondtime > 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          page_show_succ_cnt = 1;
        }
        //long u32httplastpackettime = //u32Httplastpackettime"
        if (u16apptypewhole == 15 && u16httpwapstat == 200 && u32httpfirstrespondtime > 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          page_show_delay = u32httplastpackettime;
        }
        if (u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 120000 && u16apptypewhole == 15) {
          web_req_cnt = 1;
        }
        if (dl_flow >= 51200 && u32httplastpackettime > 0 && (u16httpwapstat >= 200 && u16httpwapstat <= 399)
          && dl_flow < 32768 * u32httplastpackettime / 1000 && u16apptypewhole == 15) {
          web_speed_tooslow_cnt = 1;
        }
        var u32httplastacktime = tmpValues(68).toLong //u32HttpLastAckTime"
        if (dl_flow >= 5120 && dl_flow < 51200 && u32httplastacktime > 12000 && u16httpwapstat >= 200
          && u16httpwapstat <= 399 && dl_flow >= u32contentlength && u16apptypewhole == 15) {
          web_open_toolong_cnt = 1;
        }
        if (u16httpwapstat <= 400 && dl_flow < u32contentlength && u16apptypewhole == 15) {
          web_interpt_cnt = 1;
        }
        if (u16httpwapstat >= 400 && u16apptypewhole == 15) {
          web_estab_fail_cnt = 1;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 0
          && u32httpfirstrespondtime < 120000 && u16apptypewhole == 15) {
          web_resp_succ_cnt = 1;
        }

        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 0
          && u32httpfirstrespondtime < 120000 && u16apptypewhole == 15) {
          web_resp_delay = u32httpfirstrespondtime;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 300
          && u32httpfirstrespondtime < 120000 && u16apptypewhole == 15) {
          web_resp_delay_300ms_cnt = 1;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 120000 && u16apptypewhole == 15) {
          web_show_succ_cnt = 1;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 120000 && u16apptypewhole == 15) {
          web_show_delay = u32httplastpackettime;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 3000
          && u32httpfirstrespondtime < 120000 && u16apptypewhole == 15) {
          web_show_delay3s_cnt = 1;
        }
        if (u32httplastpackettime > 0 && u32httplastpackettime < 120000 && u16apptypewhole == 15) {
          web_dn_duration = u32httplastpackettime;
        }
        if (u32httplastpackettime > 0 && u32httplastpackettime < 120000 && u16apptypewhole == 15) {
          web_dn_flow = dl_flow;
        }
        if (u16apptypewhole == 15 && dl_flow < u32httplastpackettime * 256 / 10 && u32httplastpackettime < 50000) {
          web_dn_speed_200kbps_cnt = 1;
        }
        if (dl_flow > 51200 && u16apptypewhole == 15 && (u16httpwapstat >= 200 && u16httpwapstat <= 399)
          && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 50000) {
          bigweb_resp_succ_cnt = 1;
        }
        if (dl_flow > 51200 && u16apptypewhole == 15 && (u16httpwapstat >= 200 && u16httpwapstat <= 399)
          && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 50000) {
          bigweb_resp_delay = 1;
        }
        var u8completeflag = tmpValues(83).toInt //u8CompleteFlag"
        if (dl_flow > 51200 && u8completeflag == 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000
          && u16apptypewhole == 15) {
          bigpage_show_delay = u32httplastpackettime;
        }
        if (dl_flow > 51200 && u8completeflag == 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000
          && u16apptypewhole == 15) {
          bigpage_show_delay_cnt = 1;
        }
        if (dl_flow > 51200 && u32httplastpackettime > 0 && u32httplastpackettime < 50000 && u16apptypewhole == 15) {
          bigpage_dn_duration = 1;
        }
        if (dl_flow > 51200 && u32httplastpackettime > 0 && u32httplastpackettime < 50000 && u16apptypewhole == 15) {
          bigpage_dn_flow = dl_flow;
        }
        if (u16apptypewhole == 7) {
          appstore_req_cnt = 1;
        }
        if (u16apptypewhole == 7 && u16httpwapstat >= 200 && u16httpwapstat <= 399) {
          appstore_succ_cnt = 1;
        }
        if (u16apptypewhole == 7 && u32httplastpackettime > 0 && u32httplastpackettime < 50000
          && dl_flow > u32httplastpackettime * 64) {
          appstore_dl_500kbps_flow = dl_flow;
        }
        if (u16apptypewhole == 7 && dl_flow < 64 * u32httplastpackettime) {
          appstore_dl_500kbps_cnt = 1;
        }
        if (u16apptypewhole == 7 && dl_flow > 0 && u32endtime > u32begintime && u32begintime >= 0) {
          appstore_dl_duration = u32endtime - u32begintime;
        }
        if (u16apptypewhole == 7) {
          appstore_dl_flow = dl_flow;
        }
        if (u16apptypewhole == 5) {
          video_req_cnt = 1;
        }
        if (u16apptypewhole == 5 && u16httpwapstat >= 200 && u16httpwapstat <= 399) {
          video_succ_cnt = 1;
        }
        if (u16apptypewhole == 5) {
          video_ave_pause_cnt = dl_flow / 1024 / 1024;
        }
        if (u16httpwapstat < 400 && dl_flow < u32contentlength && u16apptypewhole == 5) {
          video_interpt_cnt = 1;
        }
        if (u16apptypewhole == 5 && dl_flow > 0 && u32endtime > u32begintime && u32begintime >= 0) {
          video_dl_duration = u32endtime - u32begintime;
        }
        if (u16apptypewhole == 5 && dl_flow > 0) {
          video_dl_flow = dl_flow;
        }
        if (u16apptypewhole == 5 && dl_flow > 0 && u32endtime > u32begintime && u32begintime >= 0) {
          video_wait_delay = (384000 * (u32endtime - u32begintime) / dl_flow);
        }
        if (384000 * u32httplastpackettime > 500 * dl_flow && u16apptypewhole == 5) {
          video_dn_delay_500kbps_cnt = 1;
        }
        if (u16apptypewhole == 5 && u16httpwapstat >= 1 && u16httpwapstat <= 399 && u32httpfirstrespondtime > 500
          && u32httpfirstrespondtime < 120000) {
          video_500ms_cnt = 1;
        }
        if (u16apptypewhole == 5 && dl_flow < 64 * u32httplastpackettime && u32httplastpackettime < 50000) {
          video_dn_speed_500kbps_cnt = 1;
        }
        if (u16apptypewhole == 1) {
          imcom_businessvisit_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8completeflag == 0) {
          imcom_businessvisit_succ_cnt = 1;
        }
        var u8behaviorflag = tmpValues(82).toInt //u8BehaviorFlag"
        if (u16apptypewhole == 1 && u8behaviorflag == 0) {
          imcom_req_cnt = 1;
        }
        var u32servicedelaytime = tmpValues(84).toLong //u32ServiceDelayTime"

        if (u16apptypewhole == 1 && u8behaviorflag == 0 && u8completeflag == 0 && u32servicedelaytime > 0
          && u32servicedelaytime < 50000) {
          imcom_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8behaviorflag == 0 && u8completeflag == 0 && u32servicedelaytime > 0
          && u32servicedelaytime < 50000) {
          imcom_conn_delay = u32servicedelaytime;
        }
        if (u16apptypewhole == 1 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_up_flow = dl_flow;
        }

        if (u16apptypewhole == 1 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_up_online_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && dl_flow < 64 * u32httplastpackettime / 10 && u32httplastpackettime > 0
          && u32httplastpackettime < 50000) {
          imcom_up_speed_50kbps_cnt = 1;
        }
        if (u16apptypewhole == 1 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_dn_flow = dl_flow;
        }

        if (u16apptypewhole == 1 && dl_flow > 0 && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          himcom_dn_online_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && dl_flow < u32httplastpackettime * 256 / 10 && u32httplastpackettime > 0
          && u32httplastpackettime < 50000) {
          imcom_dn_speed_200kbps_cnt = 1;
        }
        if ((u16apptypewhole == 1 && u32servicedelaytime > 5000)
          || (u16apptypewhole == 1 && (u8behaviorflag == 1 || u8behaviorflag == 2) && u32servicedelaytime == 0)) {
          imcom_interpt_cnt = 1;
        }
        var u8appcontent = tmpValues(29).toInt //u8AppContent"
        var u8appstatus = tmpValues(30).toInt //u8AppStatus"
        if (u16apptypewhole == 1 && u8appcontent == 2 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receiveimg_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receiveimg_flow = dl_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && dl_flow > 0) {
          imcom_receiveimg_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && dl_flow > 0 && u8appstatus == 0) {
          imcom_receiveimg_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receivemusic_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receivemusic_flow = dl_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && dl_flow > 0) {
          imcom_receivemusic_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && dl_flow > 0 && u8appstatus == 0) {
          imcom_receivemusic_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 1 && dl_flow > 0) {
          imcom_receivetext_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 1 && dl_flow > 0 && u8appstatus == 0) {
          imcom_receivetext_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receivevideo_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && dl_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_receivevideo_flow = dl_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && dl_flow > 0) {
          imcom_receivevideo_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && dl_flow > 0 && u8appstatus == 0) {
          imcom_receivevideo_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendimg_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendimg_flow = ul_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && ul_flow > 0) {
          imcom_sendimg_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 2 && ul_flow > 0 && u8appstatus == 0) {
          imcom_sendimg_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendmusic_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendmusic_flow = ul_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && ul_flow > 0) {
          imcom_sendmusic_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 3 && ul_flow > 0 && u8appstatus == 0) {
          imcom_sendmusic_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 1 && ul_flow > 0) {
          imcom_sendtext_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 1 && ul_flow > 0 && u8appstatus == 0) {
          imcom_sendtext_succ_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendvideo_duration = u32httplastpackettime;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && ul_flow > 0 && u8appstatus == 0
          && u32httplastpackettime > 0 && u32httplastpackettime < 50000) {
          imcom_sendvideo_flow = ul_flow;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && ul_flow > 0) {
          imcom_sendvideo_req_cnt = 1;
        }
        if (u16apptypewhole == 1 && u8appcontent == 4 && ul_flow > 0 && u8appstatus == 0) {
          imcom_sendvideo_succ_cnt = 1;
        }
        /*
            if(u16apptypewhole==1 && u16apptype==9 ) {
                weixin_active_user_cnt=//u64imsi"
            }*/
        if (u16apptypewhole == 1 && u16apptype == 9) {
          weixin_biz_flow = (dl_flow + ul_flow);
        }
        if (u16apptypewhole == 1 && u16apptype == 9 && u8appcontent == 2) {
          weixin_dl_music_flow = dl_flow;
          weixin_ul_music_flow = ul_flow;
        }
        if (u16apptypewhole == 1 && u16apptype == 9 && u8appcontent == 1) {
          weixin_dl_text_flow = dl_flow;
          weixin_ul_text_flow = ul_flow;
        }
        if (u16apptypewhole == 1 && u16apptype == 9 && u8appcontent == 0) {
          weixin_heart_cnt = 1;
        }

        if (u16apptypewhole == 8) {
          game_businessvisit_req_cnt = 1;
        }
        if (u16apptypewhole == 8 && u8completeflag == 0) {
          game_businessvisit_succ_cnt = 1;
        }
        if (u16httpwapstat >= 200 && u16httpwapstat < 400) {
          session_resp_succ_cnt = 1;
        }
        if (u16httpwapstat == 408) {
          session_timeout_cnt = 1;
        }
        if (u32httpfirstrespondtime > 0) {
          http_firstresp_delay = u32httpfirstrespondtime;
          http_firstresp_delay_cnt = 1;
        }

        var u32_ul_tcp_retranspacket_num = tmpValues(44).toLong //u32UlTCPReTransPacketNum"
        var u32_dl_tcp_retranspacket_num = tmpValues(45).toLong //u32DlTCPReTransPacketNum"
        var u8tcpresetnum = tmpValues(62).toInt //u8TcpResetNum"
        if (u16httpwapstat == 200 && (u32_ul_tcp_retranspacket_num + u32_dl_tcp_retranspacket_num) >= 10
          && u8tcpresetnum >= 2) {
          sess_fail_badprocess_cnt = 1;
        }
        if (u16httpwapstat == 0) {
          sess_fail_noresp_cnt = 1;
        }
        if (u16httpwapstat != 0 && u16httpwapstat != 200) {
          sess_fail_resp_fail_cnt = 1;
        }
        if (u8httpversion >= 5 && u16httpwapstat >= 400) {
          wap_fail_cnt = 1;
        }
        if (u8httpversion >= 5) {
          wap_req_cnt = 1;
          wap_ul_flow = ul_flow;
          wap_dl_flow = dl_flow;
        }
        if (u8httpversion >= 5 && u16httpwapstat >= 200 && u16httpwapstat <= 399) {
          wap_succ_cnt = 1;
          total_wap_duration = u32endtime - u32begintime;
        }
        if (u8httpversion >= 5 && u32httpfirstrespondtime > 0 && u32httplastpackettime > 0
          && u32httplastpackettime < 120000) {
          wap_ack_delay = u32httplastpackettime;
          wap_ack_delay_cnt = 1;
        }
        if (u8httpversion >= 5 && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 120000) {
          wap_firstack_delay = u32httpfirstrespondtime;
          wap_firstack_delay_cnt = 1;
        }
      }

      new Tuple2(citycode + "," + lac + "," + cell, Array[Long](appstore_dl_500kbps_cnt, appstore_dl_500kbps_flow, appstore_dl_duration, appstore_dl_flow,
        appstore_req_cnt, appstore_succ_cnt, bigpage_dn_duration, bigpage_dn_flow, bigpage_show_delay,
        bigpage_show_delay_cnt, bigweb_resp_delay, bigweb_resp_succ_cnt, dns_delay, dns_delay_cnt, dns_dl_flow,
        dns_dl_ip_fragpacket_num, dns_dl_ip_packet_num, dns_dl_online_duration, dns_dl_tcp_disordpacket_num,
        dns_dl_tcp_packet_num, dns_dl_tcp_retranspacket_num, dns_fail_cnt, dns_query_cnt, dns_succ_cnt,
        dns_tcp_dl_flow, dns_tcp_linkack_cnt, dns_tcp_linkack_delay, dns_tcp_linkresp_cnt,
        dns_tcp_linkresp_delay, dns_tcp_link_req_cnt, dns_tcp_ul_flow, dns_timeout_cnt, dns_udp_dl_flow,
        dns_udp_ul_flow, dns_ul_flow, dns_ul_ip_fragpacket_num, dns_ul_ip_packet_num, dns_ul_online_duration,
        dns_ul_tcp_disordpacket_num, dns_ul_tcp_packet_num, dns_ul_tcp_retranspacket_num, email_dl_flow,
        email_dl_ip_fragpacket_num, email_dl_ip_packet_num, email_dl_online_duration,
        email_dl_tcp_disordpacket_num, email_dl_tcp_packet_num, email_dl_tcp_retranspacket_num,
        email_tcp_dl_flow, email_tcp_linkack_cnt, email_tcp_linkack_delay, email_tcp_linkresp_cnt,
        email_tcp_linkresp_delay, email_tcp_link_req_cnt, email_tcp_ul_flow, email_udp_dl_flow,
        email_udp_ul_flow, email_ul_flow, email_ul_ip_fragpacket_num, email_ul_ip_packet_num,
        email_ul_online_duration, email_ul_tcp_disordpacket_num, email_ul_tcp_packet_num,
        email_ul_tcp_retranspacket_num, emial_imap_dl_flow, emial_imap_ul_flow, emial_pop3_dl_flow,
        emial_pop3_ul_flow, emial_smtp_dl_flow, emial_smtp_ul_flow, firstfin_ack_delay, firstfin_ack_num,
        ftp_conn_cnt, ftp_dl_flow, ftp_dl_ip_fragpacket_num, ftp_dl_ip_packet_num, ftp_dl_online_duration,
        ftp_dl_tcp_disordpacket_num, ftp_dl_tcp_packet_num, ftp_dl_tcp_retranspacket_num, ftp_fail_cnt,
        ftp_resp_delay, ftp_resp_delay_cnt, ftp_succ_cnt, ftp_tcp_dl_flow, ftp_tcp_linkack_cnt,
        ftp_tcp_linkack_delay, ftp_tcp_linkresp_cnt, ftp_tcp_linkresp_delay, ftp_tcp_link_req_cnt,
        ftp_tcp_ul_flow, ftp_transfer_duration, ftp_transfer_file, ftp_udp_dl_flow, ftp_udp_ul_flow,
        ftp_ul_flow, ftp_ul_ip_fragpacket_num, ftp_ul_ip_packet_num, ftp_ul_online_duration,
        ftp_ul_tcp_disordpacket_num, ftp_ul_tcp_packet_num, ftp_ul_tcp_retranspacket_num,
        game_businessvisit_req_cnt, game_businessvisit_succ_cnt, himcom_dn_online_duration,
        http_dl_100k_duration, http_dl_100k_flow, http_dl_30k_duration, http_dl_30k_flow,
        http_dl_500kbps_duration, http_dl_500kbps_flow, http_dl_50k_duration, http_dl_50k_flow,
        http_dl_disord_flow, http_dl_flow, http_dl_ip_fragpacket_num, http_dl_ip_packet_num,
        http_dl_online_duration, http_dl_retrans_flow, http_dl_tcp_disordpacket_num, http_dl_tcp_packet_num,
        http_dl_tcp_retranspacket_num, http_fail_cnt, http_firstresp_delay, http_firstresp_delay_cnt,
        http_interpt_cnt, http_req_cnt, http_resp_200_300_cnt, http_resp_300_400_cnt, http_resp_400_500_cnt,
        http_resp_big500_cnt, http_resp_delay, http_resp_equ200_cnt, http_resp_less200_cnt, http_resp_no_cnt,
        http_resp_succ_cnt, http_succ_cnt, http_tcp_dl_flow, http_tcp_linkack_cnt, http_tcp_linkack_delay,
        http_tcp_linkresp_cnt, http_tcp_linkresp_delay, http_tcp_link_req_cnt, http_tcp_ul_flow,
        http_udp_dl_flow, http_udp_ul_flow, http_ul_flow, http_ul_ip_fragpacket_num, http_ul_ip_packet_num,
        http_ul_online_duration, http_ul_tcp_disordpacket_num, http_ul_tcp_packet_num,
        http_ul_tcp_retranspacket_num, imap_dl_flow, imap_ul_flow, imcom_businessvisit_req_cnt,
        imcom_businessvisit_succ_cnt, imcom_conn_delay, imcom_dn_flow, imcom_dn_speed_200kbps_cnt,
        imcom_interpt_cnt, imcom_receiveimg_duration, imcom_receiveimg_flow, imcom_receiveimg_req_cnt,
        imcom_receiveimg_succ_cnt, imcom_receivemusic_duration, imcom_receivemusic_flow,
        imcom_receivemusic_req_cnt, imcom_receivemusic_succ_cnt, imcom_receivetext_req_cnt,
        imcom_receivetext_succ_cnt, imcom_receivevideo_duration, imcom_receivevideo_flow,
        imcom_receivevideo_req_cnt, imcom_receivevideo_succ_cnt, imcom_req_cnt, imcom_sendimg_duration,
        imcom_sendimg_flow, imcom_sendimg_req_cnt, imcom_sendimg_succ_cnt, imcom_sendmusic_duration,
        imcom_sendmusic_flow, imcom_sendmusic_req_cnt, imcom_sendmusic_succ_cnt, imcom_sendtext_req_cnt,
        imcom_sendtext_succ_cnt, imcom_sendvideo_duration, imcom_sendvideo_flow, imcom_sendvideo_req_cnt,
        imcom_sendvideo_succ_cnt, imcom_succ_cnt, imcom_up_flow, imcom_up_online_duration,
        imcom_up_speed_50kbps_cnt, im_dl_flow, im_dl_ip_fragpacket_num, im_dl_ip_packet_num,
        im_dl_online_duration, im_dl_tcp_disordpacket_num, im_dl_tcp_packet_num, im_dl_tcp_retranspacket_num,
        im_tcp_dl_flow, im_tcp_linkack_cnt, im_tcp_linkack_delay, im_tcp_linkresp_cnt, im_tcp_linkresp_delay,
        im_tcp_link_req_cnt, im_tcp_ul_flow, im_udp_dl_flow, im_udp_ul_flow, im_ul_flow,
        im_ul_ip_fragpacket_num, im_ul_ip_packet_num, im_ul_online_duration, im_ul_tcp_disordpacket_num,
        im_ul_tcp_packet_num, im_ul_tcp_retranspacket_num, mms_dl_ip_fragpacket_num, mms_dl_ip_packet_num,
        mms_dl_online_duration, mms_dl_tcp_disordpacket_num, mms_dl_tcp_packet_num,
        mms_dl_tcp_retranspacket_num, mms_mo_delay, mms_mo_delay_cnt, mms_mo_dl_flow, mms_mo_duration,
        mms_mo_fail_cnt, mms_mo_req_cnt, mms_mo_succ_cnt, mms_mo_ul_flow, mms_mt_delay, mms_mt_delay_cnt,
        mms_mt_dl_flow, mms_mt_duration, mms_mt_fail_cnt, mms_mt_req_cnt, mms_mt_succ_cnt, mms_mt_ul_flow,
        mms_tcp_dl_flow, mms_tcp_linkack_cnt, mms_tcp_linkack_delay, mms_tcp_linkresp_cnt,
        mms_tcp_linkresp_delay, mms_tcp_link_req_cnt, mms_tcp_ul_flow, mms_udp_dl_flow, mms_udp_ul_flow,
        mms_ul_ip_fragpacket_num, mms_ul_ip_packet_num, mms_ul_online_duration, mms_ul_tcp_disordpacket_num,
        mms_ul_tcp_packet_num, mms_ul_tcp_retranspacket_num, p2p_dl_flow, p2p_dl_ip_fragpacket_num,
        p2p_dl_ip_packet_num, p2p_dl_online_duration, p2p_dl_tcp_disordpacket_num, p2p_dl_tcp_packet_num,
        p2p_dl_tcp_retranspacket_num, p2p_tcp_dl_flow, p2p_tcp_linkack_cnt, p2p_tcp_linkack_delay,
        p2p_tcp_linkresp_cnt, p2p_tcp_linkresp_delay, p2p_tcp_link_req_cnt, p2p_tcp_ul_flow, p2p_udp_dl_flow,
        p2p_udp_ul_flow, p2p_ul_flow, p2p_ul_ip_fragpacket_num, p2p_ul_ip_packet_num, p2p_ul_online_duration,
        p2p_ul_tcp_disordpacket_num, p2p_ul_tcp_packet_num, p2p_ul_tcp_retranspacket_num, page_dn_duration,
        page_dn_flow, page_req_cnt, page_resp_delay, page_resp_succ_cnt, page_show_delay, page_show_succ_cnt,
        pop3_dl_flow, pop3_ul_flow, rtsp_audio_flow, rtsp_dl_flow, rtsp_dl_ip_fragpacket_num,
        rtsp_dl_ip_packet_num, rtsp_dl_online_duration, rtsp_dl_tcp_disordpacket_num, rtsp_dl_tcp_packet_num,
        rtsp_dl_tcp_retranspacket_num, rtsp_resp_delay, rtsp_resp_delay_cnt, rtsp_tcp_dl_flow,
        rtsp_tcp_linkack_cnt, rtsp_tcp_linkack_delay, rtsp_tcp_linkresp_cnt, rtsp_tcp_linkresp_delay,
        rtsp_tcp_link_req_cnt, rtsp_tcp_ul_flow, rtsp_udp_dl_flow, rtsp_udp_ul_flow, rtsp_ul_flow,
        rtsp_ul_ip_fragpacket_num, rtsp_ul_ip_packet_num, rtsp_ul_online_duration,
        rtsp_ul_tcp_disordpacket_num, rtsp_ul_tcp_packet_num, rtsp_ul_tcp_retranspacket_num, rtsp_video_flow,
        s1u_dl_flow, s1u_dl_ip_fragpacket_num, s1u_dl_ip_packet_num, s1u_dl_online_duration,
        s1u_dl_tcp_disordpacket_num, s1u_dl_tcp_packet_num, s1u_dl_tcp_retranspacket_num, s1u_tcp_dl_flow,
        s1u_tcp_linkack_cnt, s1u_tcp_linkack_delay, s1u_tcp_linkresp_cnt, s1u_tcp_linkresp_delay,
        s1u_tcp_link_req_cnt, s1u_tcp_ul_flow, s1u_udp_dl_flow, s1u_udp_ul_flow, s1u_ul_flow,
        s1u_ul_ip_fragpacket_num, s1u_ul_ip_packet_num, s1u_ul_online_duration, s1u_ul_tcp_disordpacket_num,
        s1u_ul_tcp_packet_num, s1u_ul_tcp_retranspacket_num, secondfin_ack_delay, secondfin_ack_num,
        session_resp_succ_cnt, session_timeout_cnt, sess_fail_badprocess_cnt, sess_fail_noresp_cnt,
        sess_fail_resp_fail_cnt, smtp_dl_flow, smtp_ul_flow, tac_cnt, tcp_fail_cnt, tcp_req_cnt, tcp_resp_cnt,
        total_http_duration, total_wap_duration, user_cnt, video_500ms_cnt, video_ave_pause_cnt,
        video_dl_duration, video_dl_flow, video_dn_delay_500kbps_cnt, video_dn_speed_500kbps_cnt,
        video_interpt_cnt, video_req_cnt, video_succ_cnt, video_wait_delay, voip_dl_flow,
        voip_dl_ip_fragpacket_num, voip_dl_ip_packet_num, voip_dl_online_duration,
        voip_dl_tcp_disordpacket_num, voip_dl_tcp_packet_num, voip_dl_tcp_retranspacket_num, voip_tcp_dl_flow,
        voip_tcp_linkack_cnt, voip_tcp_linkack_delay, voip_tcp_linkresp_cnt, voip_tcp_linkresp_delay,
        voip_tcp_link_req_cnt, voip_tcp_ul_flow, voip_udp_dl_flow, voip_udp_ul_flow, voip_ul_flow,
        voip_ul_ip_fragpacket_num, voip_ul_ip_packet_num, voip_ul_online_duration,
        voip_ul_tcp_disordpacket_num, voip_ul_tcp_packet_num, voip_ul_tcp_retranspacket_num, wap_ack_delay,
        wap_ack_delay_cnt, wap_dl_flow, wap_fail_cnt, wap_firstack_delay, wap_firstack_delay_cnt, wap_req_cnt,
        wap_succ_cnt, wap_ul_flow, web_dn_duration, web_dn_flow, web_dn_speed_200kbps_cnt, web_estab_fail_cnt,
        web_interpt_cnt, web_open_toolong_cnt, web_req_cnt, web_resp_delay, web_resp_delay_300ms_cnt,
        web_resp_succ_cnt, web_show_delay, web_show_delay3s_cnt, web_show_succ_cnt, web_speed_tooslow_cnt,
        weixin_biz_flow, weixin_dl_music_flow, weixin_dl_text_flow, weixin_heart_cnt, weixin_ul_music_flow,
        weixin_ul_text_flow))
    }).reduceByKey((a, b) => {

      var tmpArr: Array[Long] = new Array[Long](a.length)

      for (i <- 0 until a.length) //
      {
        tmpArr(i) = a(i) + b(i)
      }

      tmpArr

    }).map(e => {

      var builder = new StringBuilder()
      builder.append(e._1).append(",")
      for (i <- 0 until e._2.length) {
        builder.append(e._2(i)).append(",");
      }

      builder.substring(0, builder.length - 1)

    }).repartition(1).saveAsTextFiles("/user/cloudil/subject/lte_user_counter");

    context.start();
    context.awaitTermination();

  }

}