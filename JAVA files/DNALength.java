import java.io.*;
import java.util.*;

public class DNALength {
	public static void main(String args[]) {
		try {
			long length = 0;
			BufferedReader in = new BufferedReader(new FileReader(args[0]));
			BufferedWriter fi = new BufferedWriter(new FileWriter("new"
					+ args[0]));
			String line;

			while ((line = in.readLine()) != null) {
				length += line.length();
				fi.write(line.toUpperCase());
			}
			System.out.println("DNA Length = " + length);
			System.out.print("Enter split value :   ");
			Scanner sc = new Scanner(System.in);
			long i = sc.nextLong();
			BufferedWriter out = new BufferedWriter(new FileWriter("input"));

			long range = length / i;
			long si = 0;
			long ei = 0;
			while (si < length) {
				ei = si + range;
				if (si + range >= length)
					ei = length - 1;
				out.write(si + "," + ei);
				out.newLine();
				si = si + range + 1;

			}
			out.close();
			fi.close();
		} catch (Exception e) {
		}
	}
}
