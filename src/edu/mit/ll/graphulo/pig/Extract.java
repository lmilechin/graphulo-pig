package edu.mit.ll.graphulo.pig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.TreeMap;

/**
 * 
 * @author ti26350
 *
 */
public class Extract {
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("/Users/ti26350/Downloads/test.json"));
		PrintWriter pw = new PrintWriter("/Users/ti26350/Desktop/test.dat");
		TreeMap<String,Integer> tm = new TreeMap<String,Integer>();
		int rowValue = 1;

		String str = br.readLine();
		while (str != null) {

			int startHash = str.indexOf("[")+1;
			int endHash = str.indexOf("]");
			int startUser = str.indexOf("user_id\": \"")+11;
			int endUser = str.indexOf("\", \"urls");
			String[] hashtags = str.substring(startHash, endHash).split(",");
			String user = str.substring(startUser,endUser);

			for(int i=0; i<hashtags.length; i++) {
				hashtags[i] = hashtags[i].trim();
				hashtags[i] = hashtags[i].substring(1, hashtags[i].length()-1);
				
				int hashtagVal = rowValue;
				if(tm.containsKey(hashtags[i])) {
					hashtagVal = tm.get(hashtags[i]);
				} else {
					tm.put(hashtags[i], rowValue);
					rowValue++;
				}
				
				int userVal = rowValue;
				if(tm.containsKey(user)) {
					userVal = tm.get(user);
				} else {
					tm.put(user, rowValue);
					rowValue++;
				}
				
				pw.println(hashtagVal + " " + userVal + " 1");
				pw.println(userVal + " " + hashtagVal + " 1");
			}
			str = br.readLine();
		}
		pw.close();
		br.close();
	}
}
