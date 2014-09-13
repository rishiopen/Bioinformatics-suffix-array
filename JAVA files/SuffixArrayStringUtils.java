import java.util.*;
import java.io.*;

public class SuffixArrayStringUtils {
	public static int countMatches(String str, String sub) {
		if (str.isEmpty() || sub.isEmpty()) {
			return 0;
		}
		int count = 0;
		int idx = 0;
		while ((idx = str.indexOf(sub, idx)) != -1) {
			count++;
			idx += sub.length();
		}
		return count;
	}
}
