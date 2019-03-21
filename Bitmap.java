import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;
import java.util.Vector;

public class Bitmap implements Serializable{

	static final int maxTuplesPerIndex = 2;
	ArrayList<BitmapPair> list;
	Bitmap(String tableName,String colName) throws IOException{
		int counter=0;
		list= new ArrayList();
		ArrayList<String> uniqueValues=getUniqueValues(tableName, colName);
		Collections.sort(uniqueValues);
		File file = new File("pages/");
		String[] paths = file.list();
		paths = DBApp.sortPaths(paths);
		int y=getIndexofColumn(tableName, colName);
		for(String value:uniqueValues) {
			StringBuilder sb=new StringBuilder();
			for (int j = 0; j < paths.length; j++) {
				String[] n = paths[j].split("_");
				//if path start with the tableName will search for the value of unique value of loop i
				if(n[0].equals(tableName)) {
					Vector<Object> v = DBApp.getNumberOfTuples("pages/" + paths[j]);
					for(int k=0;k<v.size();k++) {
						Object[] oneRow = (Object[]) v.get(k);
						// should be add into the bit map  and the page number will be n[1]
						sb.append(oneRow[y].toString().equals(value)?"1":"0");
						
					}
					for(int i=v.size();i<DBApp.maxTuplesPerPage;i++)
						sb.append("0");
//					sb.append("-");
				}
			}

			list.add(new BitmapPair(value,sb.toString()));
			if(list.size()==maxTuplesPerIndex)
			{
				try {
					FileOutputStream fileOut = new FileOutputStream("bitmaps/"+tableName+colName+"index"+counter++);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(list);
					out.close();
					fileOut.close();

				} catch (IOException i) {
					i.printStackTrace();
				}
				list.clear();
			}
		}
		if(list.isEmpty())
			return ; 
		try {
			FileOutputStream fileOut = new FileOutputStream("bitmaps/"+tableName+colName+"index"+counter);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(list);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
		
		
	}
	public static ArrayList<String> getUniqueValues(String strTableName, String columnName) throws IOException {
		ArrayList<String> result = new ArrayList<>();
		int location = getIndexofColumn(strTableName, columnName);
		File file = new File("pages/");
		String[] paths = file.list();
		paths = DBApp.sortPaths(paths);
		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {

				Vector<Object> v = DBApp.getNumberOfTuples("pages/" + paths[j]);
				// getting every page and compare every column
				for (int i = 0; i < v.size(); i++) {
					Object[] oneRow = (Object[]) v.get(i);
					boolean fl1 = false;
					for (int k = 0; k < result.size(); k++) {
						if ((oneRow[location] + "").equals(result.get(k)))
							fl1 = true;
					}
					if (!fl1)
						result.add(oneRow[location] + "");

				}

			}
		}
		return result;

	}

	public static int getIndexofColumn(String tableName, String colName) {
		int result = 0;
		File meta = new File("meta.csv");
		try {
			Scanner inputStream = new Scanner(meta);
			while (inputStream.hasNextLine()) {
				String s = inputStream.nextLine();
				if (s.split(", ")[0].equals(tableName)) {
					if ((s.split(", ")[1]).equals(colName)) {
						result++;
						inputStream.close();
						return result;
					} else {
						result++;

					}
				}
			}
			inputStream.close();
			return -1;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return -1;
	}

//	public static void mapCreation(String tableName, String columnName) throws IOException {
//		if (ExistTable(tableName)) {
//			ArrayList<String> x = getUniqueValues(tableName, columnName);
//			int y = getIndexofColumn(tableName, columnName);
//			File file = new File("pages/");
//			String[] paths = file.list();
//			paths = sortPaths(paths);
//			// looping on unique values
//			for (int i = 0; i < x.size(); i++) {
//				//looping on every path
//				for (int j = 0; j < paths.length; j++) {
//					String[] n = paths[j].split("_");
//					//if path start with the tableName will search for the value of unique value of loop i
//					if(n[0].equals(tableName)) {
//						Vector<Object> v = getNumberOfTuples("pages/" + paths[j]);
//						for(int k=0;k<v.size();k++) {
//							Object[] oneRow = (Object[]) v.get(k);
//							if(oneRow[y].equals(x.get(i))) {
//								// should be add into the bit map  and the page number will be n[1]
//							}
//						}
//						
//					}
//				}
//
//			}
//
//		}
//	}

	
}
