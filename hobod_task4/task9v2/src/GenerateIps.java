import java.util.ArrayList;
import java.util.Random;
import java.math.BigInteger;
import java.nio.ByteBuffer;
 
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
 


public class GenerateIps extends GenericUDTF {
 
  private static Random rand = new Random();

  @Override
  public void close() throws HiveException {
	
  }
 
  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("possible_ip");
    fieldNames.add("raw_ip");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }
 
  @Override
  public void process(Object[] args) throws HiveException {
    	String ip = (String)args[0];
	String mask = (String)args[1];
	

	int[] ipOctets = new int[4];
	int k = 0;
 	for (String octet : ip.split("\\.")) {
		ipOctets[k++] = Integer.valueOf(octet);
	}


	int[] maskOctets = new int[4];
	k = 0;
	for (String octet :  mask.split("\\.")) {
		maskOctets[k++] = Integer.valueOf(octet);
	}

	
	int first = (ipOctets[0] & maskOctets[0]) % 256;
	do {
		int second = (ipOctets[1] & maskOctets[1]) % 256;
		do {
			int third = (ipOctets[2] & maskOctets[2]) % 256;
			do {
				int fourth = (ipOctets[3] & maskOctets[3]) % 256;
				do {
					long number = (first << 24) + (second << 16) + (third << 8) + fourth;
					BigInteger res = new BigInteger(1, ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(number).array());
					BigInteger res1 = new BigInteger(1, 
						new byte[]{(byte)(first % 256), (byte)(second % 256), 
							(byte)(third % 256), (byte)(fourth % 256)});
					forward(new Object[]{String.valueOf(first) + "." + String.valueOf(second) + "." 
						+ String.valueOf(third) + "." + String.valueOf(fourth), res1.toString()});
				} while (fourth++ < (255 - maskOctets[3]) % 256); 
			} while (third++ < (255 - maskOctets[2]) % 256);
		} while (second++ < (255 - maskOctets[1]) % 256);
	} while (first++ < (255 - maskOctets[0]) % 256);
  }
 
}


