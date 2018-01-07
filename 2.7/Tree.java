package cs.bigdata.Tutorial2;

import java.util.Arrays;
import java.io.IOException;
import java.util.ArrayList;

public class Tree {
	public static void main(String[] args) throws IOException {

	}
	
	public static void show(String line) throws IOException {
		String[] splitted = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		
		String year = splitted[5];
		String height = splitted[6];
		
		if(year == null | year.length() == 0) {
			year = " NA ";
		}
		
		if(height == null | height.length() == 0) {
			height = " NA ";
		}
		
		System.out.println("Year : " + year + " - Height : " + height);
	}
}
