import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jzy.redis.util.JedisClusterUtil;
import com.jzy.redis.util.RedisUtil;

public class test {
  

  
    public static void main(String[] args) {  
    	  
    	   String str="c093a57347204e83bb8f6141e9081724_jzy_";
           Map<String,Integer> addArray=new  HashMap();
           addArray.put("192.168.1.136", 7000);
           addArray.put("192.168.1.137", 7000);
           RedisUtil jdisutil=new RedisUtil(addArray,str);
//           jdisutil.setString("age", "145");
//           System.out.println(jdisutil.getString("age"));  
          
           long s = System.currentTimeMillis();

        
           try {
               // batch write
               for (int i = 0; i < 10000; i++) {
            	   jdisutil.setString("c093a57347204e83bb8f6141e9081724_jzy_k" + i, "v1" + i);
               }

               // batch read
               for (int i = 0; i < 10000; i++) {
            	   jdisutil.getString("c093a57347204e83bb8f6141e9081724_jzy_k" + i);
               }
           } finally {
        	  // jdisutil.close();
           }

           // output time 
           long t = System.currentTimeMillis() - s;
           System.out.println(t);

    }  
}
