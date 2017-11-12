import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyUdf extends UDF {
	public Text evaluate(Text input) {
		if (input == null) {
			return null;
		}
		int kilobyte = Integer.valueOf(input.toString());
		return new Text(String.valueOf(kilobyte / 1024));
	}
}
