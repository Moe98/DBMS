import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
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
					FileOutputStream fileOut = new FileOutputStream("bitmaps/"+tableName+"_"+colName+"_"+"index"+"_"+counter++);
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
		updateMetaBitMap(tableName, colName);
		if(list.isEmpty())
			return ; 
		try {
			FileOutputStream fileOut = new FileOutputStream("bitmaps/"+tableName+"_"+colName+"_"+"index"+"_"+counter);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(list);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
		
		
	}
	public static void updateMetaBitMap(String tableName,String colName) throws IOException {
		File meta = new File("meta.csv");
		ArrayList<String []>newMetaData = new ArrayList();
		try {
			Scanner inputStream = new Scanner(meta);
			while (inputStream.hasNextLine()) {
				String s = inputStream.nextLine();
				
				String []splitted=s.split(", ");
				if (s.split(", ")[0].equals(tableName) && s.split(", ")[1].equals(colName)) 
					 splitted[4]="True";
				newMetaData.add(splitted);		
				
				
			
		
			
		}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		File dir = new File("meta.csv");
		FileWriter fileWriter = new FileWriter(dir);

		BufferedWriter bw = new BufferedWriter(fileWriter);
		PrintWriter out = new PrintWriter(bw);
		for(String []line:newMetaData) {
			for(int i=0;i<line.length;i++)
			{
				out.print(line[i]+(i+1==line.length?"":", "));
			}
			out.println();
		}
		out.flush();
		out.close();
		fileWriter.close();
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
	// assuming parameters are 1-indexed
	public static int getBitPosition(int pageNumber,int posInPage) {
		return (pageNumber-1) * DBApp.maxTuplesPerPage + posInPage -1;
		
	}
	public static void updateOnDelete(int pageNumber,int posInPage, String tableName ) throws IOException
	{
		System.out.println(posInPage+"  "+pageNumber);
		int start=pageNumber * DBApp.maxTuplesPerPage ,end = start+ DBApp.maxTuplesPerPage;
		System.out.println(start+"  "+end);
		String[] paths = new File("bitmaps").list();
		int location = pageNumber * DBApp.maxTuplesPerPage + posInPage;
		for(String path:paths)
		{
			String []splitted=path.split("_");
			if(splitted[0].equals(tableName)) {
				ArrayList<BitmapPair> list = getBitMapPair("bitmaps/"+path);
				for(BitmapPair pair:list) {
					String bitmap = pair.bitmap;
					String before=bitmap.substring(0,start), after = bitmap.substring(end,bitmap.length());
					String x = bitmap.substring(start,location), y =bitmap.substring(location+1,end);
					String s= before+x+y+"0"+after;
					pair.bitmap=s;
					System.out.println(pair.value+". before:"+bitmap +". afterDeletion:"+pair.bitmap);
					

					
				}
				FileOutputStream fileOut = new FileOutputStream("bitmaps/"+path);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(list);
				out.close();
				fileOut.close();
			}
		}
	}

	
	
	static ArrayList<BitmapPair> getBitMapPair(String className) {
		ArrayList<BitmapPair> v;
		try {
			FileInputStream fileIn = new FileInputStream(className);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (ArrayList<BitmapPair>) in.readObject();
			in.close();
			fileIn.close();
			return v;
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			System.out.println("class not found");
			c.printStackTrace();
			return null;
		}
	}


	
}
