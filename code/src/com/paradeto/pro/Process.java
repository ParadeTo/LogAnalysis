package com.paradeto.pro;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



/**
 * 对日志每行进行处理
 * @author dataguru论坛张丹老师
 *
 */
public class Process {
	private String remote_addr = "";// 记录客户端的ip地址
	private String remote_user = "";// 记录客户端用户名称,忽略属性"-"
	private String time_local = "";// 记录访问时间与时区
	private String request = "";// 记录请求的url与http协议
	private String status = "";// 记录请求状态；成功是200
	private String body_bytes_sent = "";// 记录发送给客户端文件主体内容大小
	private String http_referer = "";// 用来记录从那个页面链接访问过来的
	private String http_user_agent = "";// 记录客户浏览器的相关信息
	private String spider = "";// 记录爬虫
	private String browser = "";//记录浏览器
	
	private boolean valid = true;// 判断数据是否合法
	private boolean bimage = false;// 判断是否是图片加载
	private boolean bspider = false;//判断是否是网页爬虫访问

	//初始化
	public static Process parser(String line) {
		Process Process = new Process();
		String[] arr = line.split(" ");
		if (arr.length > 11) {
			Process.setRemote_addr(arr[0]);
			Process.setRemote_user(arr[1]);
			Process.setTime_local(arr[3].substring(1));
			//请求的页面，只取网页地址？前面的部分
			int sep;
			if((sep = arr[6].indexOf("?"))>0){
				Process.setRequest(arr[6].substring(0, sep));
			}else{
				Process.setRequest(arr[6]);
			}
			
			Process.setStatus(arr[8]);
			Process.setBody_bytes_sent(arr[9]);
			Process.setHttp_referer(arr[10]);
			Process.setHttp_user_agent(arr[11]);
			//判断爬虫
			for(int i=11;i<arr.length;i++){
				if(arr[i].toLowerCase().contains("spider")||arr[i].toLowerCase().contains("googlebot")){
					Process.setSpider(arr[i]);
				}
			}

			
			if ((Process.getStatus().length())!=3) {
				Process.setValid(false);
			}else if (Integer.parseInt(Process.getStatus()) >= 400){//大于400，HTTP错误
				Process.setValid(false);
			}

		} else {
			Process.setValid(false);
		}
		return Process;
	}
	//判断是否是图片加载
	public Process findImage() {
		if (this.request.contains(".jpg") || this.request.contains(".png")
				|| this.request.contains(".gif")
				|| this.request.contains(".jpeg")
				|| this.request.contains(".bmp")){
			this.setValid(true);
			this.setBimage(true);
		}
		return this;
	}
	//过滤图片加载
	public Process filterImage() {
		if (this.request.contains(".jpg") || this.request.contains(".png")
				|| this.request.contains(".gif")
				|| this.request.contains(".jpeg")
				|| this.request.contains(".bmp"))
			this.setValid(false);
		return this;
	}
	//判断是否是爬虫
	public boolean isSpider() {
		String str = this.spider;
		if (str!=""){//this.setValid(true);
			this.setBspider(true);
		}else{
			this.setBspider(false);
		}
		return getBspider();
	}
	//
	public String getBrowser() {
		return this.browser;
	}

	//
	public void setBrowser(String[] arr) {
		Set<String> browsers = new HashSet<String>();
		browsers.add("Chrome");
		browsers.add("Firefox");
		browsers.add("MSIE");
		browsers.add("Opera");
		Iterator<String> ite = browsers.iterator();
		if (arr[arr.length - 1].contains("Safari")) {
			this.browser = "Safari";
		} else {
			while (ite.hasNext()) {
				String str = ite.next();

				for (int i = 11; i < arr.length - 1; i++) {
					if (arr[i].contains(str)) {
						this.browser = str;
						break;
					}
				}
			}
		}
		if (this.browser == "")
			this.setValid(false);
	}
	/**
	 * 按page的pv分类,这是原作者网站特有的，在我的工程中没有用到
	 */
	public static Process filterPVs(String line) {
		Process Process = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");

		if (!pages.contains(Process.getRequest())) {
			Process.setValid(false);
		}
		return Process;
	}

	/**
	 * 按page的独立ip分类
	 */
	public static Process filterIPs(String line) {
		Process Process = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");

		if (!pages.contains(Process.getRequest())) {
			Process.setValid(false);
		}

		return Process;
	}

	/**
	 * PV按浏览器分类
	 */
	public static Process filterBroswer(String line) {
		return parser(line);
	}

	/**
	 * PV按小时分类
	 */
	public static Process filterTime(String line) {
		return parser(line);
	}

	/**
	 * PV按访问域名分类
	 */
	public static Process filterDomain(String line) {
		return parser(line);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nremote_addr:" + this.remote_addr);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ntime_local:" + this.time_local);
		sb.append("\nrequest:" + this.request);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
		sb.append("\nhttp_referer:" + this.http_referer);
		sb.append("\nhttp_user_agent:" + this.http_user_agent);
		sb.append("\nspider:" + this.spider);
		return sb.toString();
	}
	public boolean getBspider(){
		return bspider;
	}
	public void setBspider(boolean bspider){
		this.bspider = bspider;
	}
	public boolean getBimage(){
		return bimage;
	}
	public void setBimage(boolean bimage){
		this.bimage = bimage;
	}
	
	public String getSpider() {
		return spider;
	}

	public void setSpider(String spider) {
		this.spider = spider;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getTime_local() {
		return time_local;
	}

	public Date getTime_local_Date() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
				Locale.US);
		return df.parse(this.time_local);
	}

	public String getTime_local_Date_hour() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
		return df.format(this.getTime_local_Date());
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public String getHttp_referer_domain() {
		if (http_referer.length() < 8) {
			return http_referer;
		}

		String str = this.http_referer.replace("\"", "").replace("http://", "")
				.replace("https://", "");
		return str.indexOf("/") > 0 ? str.substring(0, str.indexOf("/")) : str;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	public boolean isValid() {
		return valid;
	}


	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public static void main(String args[]) {
		String line[] = {
				"222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"",
				"123.125.71.101 - - [04/Jan/2012:00:00:03 +0800] \"GET /thread-686167-1-1.html HTTP/1.1\" 200 17512 \"-\" \"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"",
				"199.255.44.5 - - [04/Jan/2012:00:21:52 +0800] \"GET /popwin_js.php HTTP/1.1\" 200 32 \"http://www.itpub.net/ctp080113.php?action=getmedal\" \"Mozilla/5.0 (Windows NT 6.0; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7\"" };

		for (int i = 0; i < 3; i++) {
			System.out.println(line[i]);
			Process process = Process.parser(line[i]);
			System.out.println(process);
			try {
				SimpleDateFormat df = new SimpleDateFormat(
						"yyyy.MM.dd:HH:mm:ss", Locale.US);
				System.out.println(df.format(process.getTime_local_Date()));
				System.out.println(process.getTime_local_Date_hour());
				System.out.println(process.getHttp_referer_domain());
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Process Process = Process.filterPVs(line).filterIPs(line); String[] arr =
		 * line.split(" ");
		 * 
		 * Process.setRemote_addr(arr[0]); Process.setRemote_user(arr[1]);
		 * Process.setTime_local(arr[3].substring(1)); Process.setRequest(arr[6]);
		 * Process.setStatus(arr[8]); Process.setBody_bytes_sent(arr[9]);
		 * Process.setHttp_referer(arr[10]); Process.setHttp_user_agent(arr[11] + " " +
		 * arr[12]); System.out.println(Process);
		 */

	}

}

