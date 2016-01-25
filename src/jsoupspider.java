import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class jsoupspider {

	/**
	 * @param args
	 * @throws IOException
	 */
	static Set<String> URLseted = new HashSet<String>(); // 已经访问过的网址集合
	static Set<String> URLset = new HashSet<String>(); // 尚未访问过的网址集合
	static FileOutputStream fo = null;

	/**
	 * name：href_to_url
	 * 
	 * @param s
	 * @return re function：将<a href=
	 *         "jobtaskshistory.jsp?logFile=file:/home/wzhuo/hadoop-1.1.2/logs/history/done/version-1/test164_1388647146547_/2014/01/04/000000/job_201401021519_0018_1388800958885_wzhuo_native%2Bjoin&amp;taskType=MAP&amp;status=SUCCESS"
	 *         > 298</a>
	 *         转化为URL，http://172.19.0.164:50030/jobtaskshistory.jsp?logFile
	 *         =file:/home/wzhuo/hadoop-1.1.2/logs/history/done/version-1/
	 *         test164_1388647146547_/2014/01/04/000000/
	 *         job_201401021519_0018_1388800958885_wzhuo_native
	 *         %2Bjoin&taskType=MAP&status=SUCCESS
	 */
	public static String href_to_url(String s) {
		String re = null;
		s = s.substring(9);
		s = s.replace('"', '>');
		s = s.split(">")[0];
		s = s.replaceAll("amp;", "");
		re = "http://172.19.0.164:50030/" + s;
		return re;
	}

	/**
	 * name:del
	 * 
	 * @param s
	 *            function:将<...>abc中的<>之间的部分去掉，结果为abc
	 */
	public static String del(String s) {
		Pattern p = Pattern.compile("<.*?>");
		Matcher m = p.matcher(s);
		ArrayList<String> strs = new ArrayList<String>();
		while (m.find()) {
			s = s.replaceAll(m.group(0), "");
		}
		return s;
	}

	/**
	 * 
	 * @param s
	 * @throws IOException
	 */
	public static void get_map_info(String s) throws IOException {
		Document doc = Jsoup.connect(s).get();
		Elements links = doc.select("a[href]");
		Element mapEl = links.get(8);// maptask链接；
		// System.out.println(mapEl);
		String mapstr = mapEl.toString();
		String mapUrl = href_to_url(mapstr);
		Document docmap = Jsoup.connect(mapUrl).get();
		/**
		 * 进入map总体页面，获取数据，在此添加代码
		 */
		Elements map_links = docmap.select("a[href]");
		map_links.remove(0);
		for (Element link : map_links) {
			String str = link.toString();
			String linkUrl = href_to_url(str);
			Document map_task = Jsoup.connect(linkUrl).get();
			/**
			 * 进入某个maptask页面，获取数据,在此添加代码
			 */
			Element map_task_els = map_task.select("table").get(0);
			Elements els = map_task_els.getElementsContainingOwnText(":");
			// for(Element el:els)
			// {
			// System.out.println(el);
			// }
			Elements map_task_links = map_task.select("a[href]");// 只有一个counters链接
			String map_task_counters_URL = href_to_url(map_task_links.get(4)
					.toString());//
			// System.out.println(map_task_counters_URL);
			Document map_task_counter = Jsoup.connect(map_task_counters_URL)
					.get();
			/**
			 * 进入某个maptask Counters页面，获取数据，在此添加代码
			 */
			Elements map_task_counters_els = map_task_counter
					.select("td[align]");
			for (Element el : map_task_counters_els) {
				fo.write(del(map_task_counters_els.toString()).getBytes());
			}
		}
	}

	/**
	 * 
	 * @param s
	 * @throws IOException
	 */
	public static void get_reduce_info(String s) throws IOException {
		Document doc = Jsoup.connect(s).get();
		Elements links = doc.select("a[href]");
		Element reduceEl = links.get(12);// total reduce任务链接

		String redstr = reduceEl.toString();
		String redUrl = href_to_url(redstr);

		Document docred = Jsoup.connect(redUrl).get();
		Elements red_links = docred.select("a[href]");
		/**
		 * 进入reduce总体界面，获取数据，在此添加代码
		 **/
		for (Element link : red_links) {
			String str = link.toString();
			String linkUrl = href_to_url(str);
			// System.out.println(linkUrl);
			Document reduce_task = Jsoup.connect(linkUrl).get();

			/**
			 * 进入所有reducetask的主页面
			 */
			Element reduce_task_els = reduce_task.select("table").get(0);
			Elements els = reduce_task_els.getElementsContainingOwnText(":");

			Elements red_task_links = reduce_task.select("a[href]");// 只有一个counters链接
			String red_task_counters_URL = href_to_url(red_task_links.get(4)
					.toString());//
			// System.out.println(red_task_counters_URL);

			/**
			 * 
			 */
			Document red_task_counter = Jsoup.connect(red_task_counters_URL)
					.get();
			// System.out.println(red_task_counter);
			/**
			 * 进入某个reducetask Counters页面，获取数据，在此添加代码
			 */
			Elements red_task_counters_els = red_task_counter.select("td");
			int t = red_task_counters_els.size();
			Element reduce_counters_final = red_task_counters_els.remove(t - 1);

			// for (Element el : red_task_counters_els) {
			// System.out.println(del(reduce_counters_final.toString()));
			String output = del(reduce_counters_final.toString());
			if (output.length() > 1) {
				fo.write(output.getBytes());
				System.out.println(output);
				fo.write('\n');
			}

			// }
		}// for
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		/**
		 * 写文件的目录
		 */
		String s = "/home/wzhuo/example/out.txt";
		fo = new FileOutputStream(s);

		/**
		 * 遍历所有href的href标签，读取各个job的主页面
		 */
		String jobhistory = "http://172.19.0.164:50030/jobhistoryhome.jsp";
		Document jobdoc = Jsoup.connect(jobhistory).get();
		Elements links = jobdoc.select("a[href]");
		//System.out.println("links！！！" + links);
		//System.out.println("links!!!"+links.size());
		
//		Element eachjobhis =null;
//		String eachjob = null;
		
//		int i = 0;
//		while(i < links.size()){
//			eachjobhis = links.get(i);
//			eachjob = eachjobhis.toString();
//			
//			if(eachjob.length()>150){
//				String onejoburl = href_to_url(eachjob);
//				System.out.println(onejoburl);
//				get_reduce_info(onejoburl);
//				fo.write(eachjob.getBytes());
//				fo.write('\n');
//				i++;
//			}else{
//				i++;
//			}
//			
//		}
		
//		Element eachjobhis = links.get(7);// 第一个jobhistory的链接入口
//
//		String eachjob = eachjobhis.toString(); // 将url转换为string

		/**
		 * url 为某job的入口url
		 */
		 String onejoburl =
		 "http://172.19.0.164:50030/jobdetailshistory.jsp?logFile=file:/home/wzhuo/hadoopyouli/logs/history/done/version-1/test164_1453440492163_/2016/01/22/000000/job_201601221328_0001_1453440557348_wzhuo_german%2Bd20%2Bz10";

		//String onejoburl = href_to_url(eachjob);
		// System.out.println("onejobburl!!!!" + onejoburl);
		// get_map_info(url); //调用map的统计函数
		get_reduce_info(onejoburl); // 调用reduce输入的统计函数
		fo.close();
	}// main
}
