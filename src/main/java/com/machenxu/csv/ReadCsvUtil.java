package com.machenxu.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadCsvUtil {
	public static void main(String[] args) {
		// 1. .csv文件的路径。注意只有一个\的要改成\\
		File csv = new File(
				"C:\\Users\\57641\\Desktop\\758566225749698609.csv"); // CSV文件路径
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csv));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line = "";
		String everyLine = "";
		int i =0;
		int row = 15;
		try {
			List<String> allString = new ArrayList<>();
			while ((line = br.readLine()) != null) // 读取到的内容给line变量
			{
				if(i == row||i == row-1||i == row+1) {
					//everyLine = line;
					System.out.println(i+">>>"+line);
				}
				i++;
				if(i>row+5) {
					
					break;
				}
				//allString.add(everyLine);
			}
			System.out.println("csv表格中所有行数：" + i);
		} catch (IOException e) {
			e.printStackTrace();
		}
 
	}


}
