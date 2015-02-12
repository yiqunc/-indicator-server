package au.edu.csdila.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class MathUtils {
	
	  public static double round(double value, int places) {
		    if (places < 0) throw new IllegalArgumentException();

		    BigDecimal bd = new BigDecimal(value);
		    bd = bd.setScale(places, RoundingMode.HALF_UP);
		    return bd.doubleValue();
		}
	  
	  public static boolean doubleEquals(double v1, double v2, double threshold) {
		    
		    return Math.abs(v1 - v2) < threshold? true : false;
		}
}