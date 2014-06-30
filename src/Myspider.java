import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



public class Myspider {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String weburl = "http://172.19.0.164:50030/jobdetailshistory.jsp?logFile=file:/home/wzhuo/hadoop-1.1.2/logs/history/done/version-1/test164_1389096344073_/2014/01/07/000000/job_201401072005_0003_1389097095784_wzhuo_native%2BWordCount";
	
		Myspider myspider = new Myspider();
		myspider.getJobdoc(weburl);
	}
	
	public void getJobdoc(String weburl) throws IOException{
		Document jobdoc = Jsoup.connect(weburl).get();
		String jobtitle = jobdoc.title();
		
		//System.out.println(jobdoc);
		
		//maptask：Successful tasks对应的位置
		Element link = jobdoc.select("td").get(79);
		System.out.println(link);
			
		
				
		
		
		
	}

}
