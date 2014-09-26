import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

public class Max_Min {
	static ArrayList<Long> data = new ArrayList<Long>();
	static int num = 0;

	/**
	 * init()
	 * 
	 * @return 均值
	 * @throws FileNotFoundException
	 * @throws IOException
	 *             function:初始化data,获取num的值
	 */
//	public static long init() throws FileNotFoundException, IOException {
//		File file = new File("/home/wzhuo/example/out.txt");
//		BufferedReader br = new BufferedReader(new FileReader(file));
//		String str;
//		str = br.readLine();
//
//		long sum = 0;
//		while (str != null) {
//			str = str.replaceAll(",", "");
//			long each = Long.parseLong(str);
//			if (each >= 0) {
//				data.add(each);
//				num++;
//				sum = sum + each;
//				str = null;
//				str = br.readLine();
//			}
//		}
//		
//		long avg = (long) (sum / num);
//		// System.out.println("num是：" + num );
//		 System.out.println("sum是：" + sum );
//		// System.out.println("avg是：" +avg);
//		return avg;
//	}

	/**
	 * @param args
	 * @throws FileNotFoundException
	 * @throws IOException
	 *             需要注意的是k是否越界；
	 */
	public static void main(String[] args) throws IOException,
			FileNotFoundException {
		// TODO Auto-generated method stub
		File file = new File("/home/wzhuo/example/out.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String str;
		str = br.readLine();
		
		long max = 0;
		long min = Long.MAX_VALUE;

		while (str != null) {
			str = str.replaceAll(",", "");
			long each = Long.parseLong(str);
			
			if (each >= 0) {
				if(each > max){
					max = each;
				}
				if(each < min){
					min = each;
				} 
			}
			
			str = br.readLine();
		}
		
		// System.out.println("sum" + sum);
		System.out.println("最大值是:" + max);

		System.out.println("最小值是：" + min);
	}
}
