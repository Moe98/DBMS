import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

	static int maxTuplesPerPage = 5;
	static ArrayList<Table> tables;
	static String deprecated = "$";
	static String nullEntry = "";

	public static void init() {
		tables = new ArrayList();

		// initialize tables array using metadata
		//
	}

	public static Boolean ExistTable(String strTableName) throws IOException {
		// gives error
		List<List<String>> allinfoInMetaData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				// validate types
				if (values[0].equals(strTableName)) {
					return true;
				}
			}

		} catch (Exception ex) {
			return false;
		}
		return false;
	}

	public static Table getTable(String strTableName) {
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).strTableName == strTableName)
				return tables.get(i);
		}
		// method not implemented yet !!!
		return null;
	}

	// try new method
	public static void writeTable() throws IOException {

		try {
			FileOutputStream fileOut = new FileOutputStream("tables1");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tables);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static void readTable() throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream("tables1");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		tables = (ArrayList<Table>) in.readObject();
		in.close();
		fileIn.close();
	}

	public static ArrayList<Pair> search(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		ArrayList<Pair> pageIndex = new ArrayList<>();
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		tuple = getValueInOrder(strTableName, htblColNameValue);
		for (int jj = 0; jj < paths.length; jj++) {
			String[] x = paths[jj].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[jj];
				counter++;
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					for (int j = 0; j < o.length && j < tuple.length; j++) {
						if (!o[j].equals(tuple[j]) && !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					}
					if (equals)
						pageIndex.add(new Pair(paths[jj], i));
				}
			}
		}
		return pageIndex;
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		if (!ExistTable(strTableName))
			new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		else
			throw new DBAppException();
		// updates metadata.CSV

	}

	static int getIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		String fileNameDefined = "meta.csv";
		int whichClusCol = 0;
		File meta = new File(fileNameDefined);
		try {
			Scanner inputStream = new Scanner(meta);
			while (inputStream.hasNextLine()) {
				String data = inputStream.nextLine();
				if (data.split(", ")[0].equals(strTableName)) {
					if ((data.split(", ")[3]).equals("False")) {
						whichClusCol++;
					} else {
						whichClusCol++;
						break;
					}
				}
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int index = 0;
		File file = new File("pages/");
		String[] paths = file.list();
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		tuple = getValueInOrder(strTableName, htblColNameValue);
		/////////////// sorting the paths///////////////////////////
		paths = sortPaths(paths);
		///////////////////////////////////////////////////
		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[j];
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					if (o[whichClusCol] instanceof java.lang.Integer) {
						if (((Integer) o[whichClusCol]).compareTo(((Integer) tuple[whichClusCol])) >= 0) {
							return index;
						}
					} else {
						if (o[whichClusCol] instanceof java.lang.String) {
							if (((String) o[whichClusCol]).compareTo((String) tuple[whichClusCol]) >= 0) {
								return index;
							}
						} else {
							if (o[whichClusCol] instanceof java.lang.Double) {
								if (((Double) o[whichClusCol]).compareTo((Double) tuple[whichClusCol]) >= 0) {
									return index;
								}
							} else {
								if (o[whichClusCol] instanceof Date) {
									if (((Date) o[whichClusCol]).compareTo((Date) tuple[whichClusCol]) >= 0) {
										return index;
									}
								} else {
									if (o[whichClusCol] instanceof java.lang.Boolean) {
										if (((Boolean) o[whichClusCol]).compareTo((Boolean) tuple[whichClusCol]) >= 0) {
											return index;
										}
									}
								}
							}
						}
					}
					index++;
				}
			}
		}
		return index;
	}

	static void validateEntry(String strTableName, Hashtable<String, Object> htblColNameValue, boolean insert)
			throws DBAppException, FileNotFoundException, IOException {
		List<List<String>> allinfoInMetaData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			int f = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					co++;
					Object givenValue = htblColNameValue.get(values[1].substring(1));
					if (givenValue == null)
						continue;
					if (values[2].substring(1).equals("java.lang.String"))
						if (!(givenValue instanceof String))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.Integer"))
						if (!(givenValue instanceof Integer))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.double"))
						if (!(givenValue instanceof Double))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.Boolean"))
						if (!(givenValue instanceof Boolean))
							throw new DBAppException();

					if (values[2].substring(1).equals("java.util.Date"))
						if (!(givenValue instanceof Date))
							throw new DBAppException();
					if (givenValue != null && values[3].substring(1).equals("True"))
						f = 1;
				}
			}
			co = insert ? co + 1 : co;
			if (f == 0)
				throw new DBAppException();
		}
	}

	public static void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
		validateEntry(strTableName, htblColNameValue, true);
		int p1 = getIndex(strTableName, htblColNameValue);
		pushDown(p1 + 1, strTableName, htblColNameValue);
	}

	static Object[] getValueInOrder(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {

		Object[] tuple;
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName))
					co++;
			}
			tuple = new Object[Math.max(htblColNameValue.size(), co + 1)];
		}
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			tuple[0] = htblColNameValue.get("TouchDate");
			int idx = 1;

			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					Object givenValue = htblColNameValue.get(values[1].substring(1));
					tuple[idx++] = givenValue;
				}
			}
			for (int i = 0; i < tuple.length; i++) {
				if (tuple[i] == null)
					tuple[i] = (Object) "empty cell";
			}
		}
		return tuple;
	}

	static void pushDown(int where, String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		where--;
		Object[] tuple = new Object[htblColNameValue.size()];
		int pushed = 0;
		tuple = getValueInOrder(strTableName, htblColNameValue);
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;

		int remainingFlag = 0;
		int enteries = 0;

		paths = sortPaths(paths);

		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				curPath = paths[j];

				StringTokenizer st = new StringTokenizer(paths[j], "_");
				String num = "";
				while (st.hasMoreTokens())
					num = st.nextToken();
				counter = Math.max(counter, Integer.parseInt(num));

				Vector<Object> newV = new Vector();
				Vector<Object> oldV = getNumberOfTuples("pages/" + curPath);

				if (remainingFlag == 1) {
					newV.add(tuple);
					remainingFlag = 0;
				}
				if (pushed == 0)
					if (enteries + oldV.size() < where) {
						enteries += oldV.size();
						continue;
					} else {
						int i = 0;
						while (true) {
							if (enteries + i == where) {
								newV.add(tuple);
								break;
							}
							newV.add(oldV.remove(0));
							i++;
						}
						pushed = 1;
					}
				newV.addAll(oldV);
				if (newV.size() > maxTuplesPerPage) {
					tuple = (Object[]) newV.remove(newV.size() - 1);
					remainingFlag = 1;
				} else {
					remainingFlag = 0;
				}
				try {
					FileOutputStream fileOut = new FileOutputStream("pages/" + curPath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(newV);
					out.close();
					fileOut.close();
				} catch (IOException i) {
					i.printStackTrace();
				}
			}
		}

		if (pushed == 0)
			remainingFlag = 1;
		if (remainingFlag == 1) {
			++counter;
			Vector<Object> v = new Vector();
			v.add(tuple);
			try {
				FileOutputStream fileOut = new FileOutputStream("pages/" + strTableName + "_" + counter);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(v);
				out.close();
				fileOut.close();

			} catch (IOException i) {
				i.printStackTrace();
			}
		}

	}

	static String[] sortPaths(String[] paths) {
		for (int i = 0; i < paths.length - 1; i++) {
			for (int j = i + 1; j < paths.length; j++) {
				StringTokenizer st = new StringTokenizer(paths[i], "_");
				String crr = "";
				while (st.hasMoreTokens()) {
					crr = st.nextToken();
				}
				int ii = Integer.parseInt(crr);
				st = new StringTokenizer(paths[j], "_");
				while (st.hasMoreTokens()) {
					crr = st.nextToken();
				}
				int jj = Integer.parseInt(crr);
				if (ii >= jj) {
					String temp = paths[i];
					paths[i] = paths[j];
					paths[j] = temp;
				}
			}
		}
		return paths;
	}

	static void readTables(String strTableName) {
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;
		paths = sortPaths(paths);
		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				curPath = paths[j];
				Vector<Object> curV = getNumberOfTuples("pages/" + curPath);
				System.out.println("pages/" + curPath);
				for (int i = 0; i < curV.size(); i++) {
					Object[] v = (Object[]) curV.get(i);
					for (Object o : v) {
						System.out.print(o.toString() + ", ");
					}
					System.out.println();
				}
				System.out.println("----------new page---------");
			}
		}
	}

	static Vector<Object> getNumberOfTuples(String className) {
		Vector<Object> v;
		try {
			FileInputStream fileIn = new FileInputStream(className);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector) in.readObject();
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

	public static void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException {
		validateEntry(strTableName, htblColNameValue, false);
		File file = new File("pages/");
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size() + 1];
		int idx = 1;
		int idxStrKey = 0;
		String col;
		String metaFile = "meta.csv";
		File meta = new File(metaFile);
		boolean found = false;
		try {
			Scanner sc = new Scanner(meta);
			while (sc.hasNext() && !found) {
				String data = sc.nextLine();
				String x[] = data.split("_");
				if (x[0].startsWith(strTableName)) {
					idxStrKey++;
					if ((data.split(", ")[3]).equals("True")) {
						found = true;
						break;
					}
				}
			}
			sc.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		ArrayList<Object> updatedObjects = new ArrayList<>();
		int count = 0;
		while (true) {
			boolean tupleFound = false;
			String[] paths = sortPaths(file.list());
			for (int j = 0; j < paths.length; j++) {
				// paths = sortPaths(file.list());
				String[] x = paths[j].split("_");
				if (x[0].equals(strTableName)) {
					last = paths[j];
					counter++;
					Vector<Object> v = getNumberOfTuples("pages/" + last);
					Object[] o;
					int prevJ = j == 0 ? j : j - 1;
					for (int i = 0; i < v.size();) {
						o = (Object[]) (v.get(i));
						boolean equals = false;
						if (o[idxStrKey] instanceof String)
							equals = ((String) (o[idxStrKey])).equals(strKey);
						if (o[idxStrKey] instanceof Double)
							equals = ((Double) o[idxStrKey]) == Double.parseDouble(strKey);
						if (o[idxStrKey] instanceof Integer)
							equals = ((Integer) o[idxStrKey]) == Integer.parseInt(strKey);
						if (o[idxStrKey] instanceof Boolean)
							equals = ((Boolean) o[idxStrKey]) == Boolean.parseBoolean(strKey);
						if (o[idxStrKey] instanceof Date)
							equals = ((Date) o[idxStrKey]).equals(new Date(strKey));
						if (updatedObjects.contains(v.get(i)))
							continue;
						if (equals) {
							// updatedObjects.add(v.get(i));
							htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
							tuple = getValueInOrder(strTableName, htblColNameValue);
							v.remove(i);
							count++;
							// j = j == 0 ? j : j - 1;
							j = 0;
							i = 0;
							tupleFound = true;
						} else
							i++;
						File deletedFile = new File("pages/" + last);
						deletedFile.delete();
						if (v.size() > 0) {
							try {
								FileOutputStream fileOut = new FileOutputStream("pages/" + last);
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(v);
								out.close();
								fileOut.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						// if (equals) {
						// insertIntoTable(strTableName, htblColNameValue);
						// }
						if (i == 0 && j == 0 && equals)
							break;
					}
				}
				paths = sortPaths(file.list());
				if (j == paths.length)
					j = 0;
			}
			if (!tupleFound)
				break;
		}
		for (int i = 0; i < count; i++)
			insertIntoTable(strTableName, htblColNameValue);
	}

	public static void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException {
		File file = new File("pages/");
		String[] paths = sortPaths(file.list());
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
		tuple = getValueInOrder(strTableName, htblColNameValue);
		for (int jj = 0; jj < paths.length; jj++) {
			String[] x = paths[jj].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[jj];
				counter++;
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size();) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					for (int j = 1; j < o.length; j++)
						if (!(o[j] == null) && !o[j].equals(tuple[j])
								&& !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					if (equals)
						v.remove(i);
					else
						i++;
					File deletedFile = new File("pages/" + last);
					deletedFile.delete();
					if (v.size() > 0) {
						try {
							FileOutputStream fileOut = new FileOutputStream("pages/" + last);
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(v);
							out.close();
							fileOut.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	////////////////////////////////////////////////////////////////////////////
	///////////////////////////// END OF MILESTONE 1////////////////////////////
	////////////////////////////////////////////////////////////////////////////

	public static String retrieveBitmapIndex(String strTableName, String strColumnName, String strColumnValue) {
		String startsWithFolderName = strTableName + strColumnName + "index";
		File file = new File("bitmaps/");
		String[] paths = file.list();
		for (int i = 0; i < paths.length; i++) {
			if (paths[i].startsWith(startsWithFolderName)) {
				Vector<Object> v = getNumberOfTuples("bitmaps/" + paths[i]);
				for (int j = 0; j < v.size(); j++) {
					BitmapPair bp = (BitmapPair) v.get(j);
					if (bp.value.equals(strColumnValue))
						return bp.bitmap;
				}
			}
		}
		return "";
	}

	public static String XOR(String columnValue1, String columnValue2) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < Math.min(columnValue1.length(), columnValue2.length()); i++) {
			int valueColumn1 = Integer.parseInt(columnValue1.charAt(i) + "");
			int valueColumn2 = Integer.parseInt(columnValue2.charAt(i) + "");
			if (((valueColumn1 + valueColumn2) & 1) == 1)
				result.append(1);
			else
				result.append(0);
		}
		if (result.length() == 0)
			for (int i = 0; i < Math.max(columnValue1.length(), Math.max(maxTuplesPerPage, columnValue2.length())); i++)
				result.append(0);
		return result.toString();
	}

	public static String OR(String columnValue1, String columnValue2) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < Math.min(columnValue1.length(), columnValue2.length()); i++) {
			int valueColumn1 = Integer.parseInt(columnValue1.charAt(i) + "");
			int valueColumn2 = Integer.parseInt(columnValue2.charAt(i) + "");
			if ((valueColumn1 + valueColumn2) > 0)
				result.append(1);
			else
				result.append(0);
		}
		if (result.length() == 0)
			for (int i = 0; i < Math.max(columnValue1.length(), Math.max(maxTuplesPerPage, columnValue2.length())); i++)
				result.append(0);
		return result.toString();
	}

	public static String NOTEQUAL(String strTableName, String strColumnName, String strColumnValue) {
		String bitmap = retrieveBitmapIndex(strTableName, strColumnName, strColumnValue);
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < bitmap.length(); i++)
			if (bitmap.charAt(i) == '0')
				result.append(1);
			else
				result.append(0);
		return result.toString();
	}

	public static String AND(String columnValue1, String columnValue2) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < Math.min(columnValue1.length(), columnValue2.length()); i++) {
			int valueColumn1 = Integer.parseInt(columnValue1.charAt(i) + "");
			int valueColumn2 = Integer.parseInt(columnValue2.charAt(i) + "");
			if (valueColumn1 == 1 && valueColumn2 == 1)
				result.append(1);
			else
				result.append(0);
		}
		if (result.length() == 0)
			for (int i = 0; i < Math.max(columnValue1.length(), Math.max(maxTuplesPerPage, columnValue2.length())); i++)
				result.append(0);
		return result.toString();
	}

	public static String GREATERTHAN(String strTableName, String strColumnName, String strColumnValue) {
		String result = "";
		String startsWithFolderName = strTableName + strColumnName + "index";
		File file = new File("bitmaps/");
		String[] paths = file.list();
		for (int i = 0; i < paths.length; i++) {
			if (paths[i].startsWith(startsWithFolderName)) {
				Vector<Object> v = getNumberOfTuples("bitmaps/" + paths[i]);
				for (int j = 0; j < v.size(); j++) {
					BitmapPair bp = (BitmapPair) v.get(j);
					if (bp.value.equals(strColumnValue)) {
						int length = bp.bitmap.length();
						result = "";
						for (int k = 0; k < length; k++)
							result += "0";
						// System.out.println("Result Before: " + result);
						for (int k = j + 1; k < v.size(); k++) {
							BitmapPair ORed = (BitmapPair) v.get(k);
							// System.out.println("Bitmap: " + ORed.bitmap);
							// System.out.println("Result: " + result);
							result = OR(result, ORed.bitmap);
							// System.out.println(result + " " + k);
						}
						return result;

					}
				}
			}
		}
		return result;
	}

	public static String GREATERTHANOREQUAL(String strTableName, String strColumnName, String strColumnValue) {
		String result = "";
		String startsWithFolderName = strTableName + strColumnName + "index";
		File file = new File("bitmaps/");
		String[] paths = file.list();
		for (int i = 0; i < paths.length; i++) {
			if (paths[i].startsWith(startsWithFolderName)) {
				Vector<Object> v = getNumberOfTuples("bitmaps/" + paths[i]);
				for (int j = 0; j < v.size(); j++) {
					BitmapPair bp = (BitmapPair) v.get(j);
					if (bp.value.equals(strColumnValue)) {
						int length = bp.bitmap.length();
						result = "";
						for (int k = 0; k < length; k++)
							result += "0";
						for (int k = j; k < v.size(); k++) {
							BitmapPair ORed = (BitmapPair) v.get(k);
							result = OR(result, ORed.bitmap);
						}
						return result;
					}
				}
			}
		}
		return result;
	}

	public static String LESSTHAN(String strTableName, String strColumnName, String strColumnValue) {
		String result = "";
		String startsWithFolderName = strTableName + strColumnName + "index";
		File file = new File("bitmaps/");
		String[] paths = file.list();
		for (int i = 0; i < paths.length; i++) {
			if (paths[i].startsWith(startsWithFolderName)) {
				Vector<Object> v = getNumberOfTuples("bitmaps/" + paths[i]);
				for (int j = 0; j < v.size(); j++) {
					BitmapPair bp = (BitmapPair) v.get(j);
					if (bp.value.equals(strColumnValue)) {
						int length = bp.bitmap.length();
						result = "";
						for (int k = 0; k < length; k++)
							result += "0";
						for (int k = j - 1; k >= 0; k--) {
							BitmapPair ORed = (BitmapPair) v.get(k);
							result = OR(result, ORed.bitmap);
						}
						return result;
					}
				}
			}
		}
		return result;
	}

	public static String LESSTHANOREQUAL(String strTableName, String strColumnName, String strColumnValue) {
		String result = "";
		String startsWithFolderName = strTableName + strColumnName + "index";
		File file = new File("bitmaps/");
		String[] paths = file.list();
		for (int i = 0; i < paths.length; i++) {
			if (paths[i].startsWith(startsWithFolderName)) {
				Vector<Object> v = getNumberOfTuples("bitmaps/" + paths[i]);
				for (int j = 0; j < v.size(); j++) {
					BitmapPair bp = (BitmapPair) v.get(j);
					if (bp.value.equals(strColumnValue)) {
						int length = bp.bitmap.length();
						result = "";
						for (int k = 0; k < length; k++)
							result += "0";
						for (int k = j; k >= 0; k--) {
							BitmapPair ORed = (BitmapPair) v.get(k);
							result = OR(result, ORed.bitmap);
						}
						return result;
					}
				}
			}
		}
		return result;
	}

	public static String ControlUnit(String strTableName, String strColumnName, String strColumnValue,
			String strOperator) {
		String result = "";
		switch (strOperator) {
		case ">":
			// System.out.println("in here");
			result = GREATERTHAN(strTableName, strColumnName, strColumnValue);
			break;
		case ">=":
			result = GREATERTHANOREQUAL(strTableName, strColumnName, strColumnValue);
			break;
		case "<":
			result = LESSTHAN(strTableName, strColumnName, strColumnValue);
			break;
		case "<=":
			result = LESSTHANOREQUAL(strTableName, strColumnName, strColumnValue);
			break;
		case "!=":
			result = NOTEQUAL(strTableName, strColumnName, strColumnValue);
			break;
		case "=":
			result = retrieveBitmapIndex(strTableName, strColumnName, strColumnValue);
			break;
		}
		return result;
	}

	public static void main(String[] args) throws DBAppException, IOException, ParseException {
		System.out.println(ControlUnit("table", "number", "1", "="));
		// System.out.println(retrieveBitmapIndex("table", "number", "3"));
		// System.out.println(retrieveBitmapIndex("table", "number", "2"));
		// System.out.println(retrieveBitmapIndex("table", "number", "1"));
		// BitmapPair[] bp = new BitmapPair[3];
		// bp[0] = new BitmapPair("moe", "10101010");
		// bp[1] = new BitmapPair("joe", "01010101");
		// bp[2] = new BitmapPair("fadi", "00000000");
		// Vector<Object> v = new Vector<>();
		// v.add(bp[0]);
		// v.add(bp[1]);
		// v.add(bp[2]);
		// FileOutputStream fileOut = new FileOutputStream("bitmaps/" +
		// "tablenameindex_1");
		// ObjectOutputStream out = new ObjectOutputStream(fileOut);
		// out.writeObject(v);
		// out.close();
		// BitmapPair[] bpTest = new BitmapPair[5];
		// bpTest[0] = new BitmapPair("1", "101001000");
		// bpTest[1] = new BitmapPair("2", "010000000");
		// bpTest[2] = new BitmapPair("3", "000100000");
		// bpTest[3] = new BitmapPair("4", "000010000");
		// bpTest[4] = new BitmapPair("5", "000000111");
		// Vector<Object> vTest = new Vector<>();
		// vTest.add(bpTest[0]);
		// vTest.add(bpTest[1]);
		// vTest.add(bpTest[2]);
		// vTest.add(bpTest[3]);
		// vTest.add(bpTest[4]);
		// // System.out.println(vTest.toString());
		// FileOutputStream fileOut = new FileOutputStream("bitmaps/" +
		// "tablenumberindex_1");
		// ObjectOutputStream out = new ObjectOutputStream(fileOut);
		// out.writeObject(vTest);
		// out.close();
		// System.out.println(retrieveBitmapIndex("table", "name", "joe"));
		// System.out.println(retrieveBitmapIndex("table", "name", "moe"));
		// System.out.println(retrieveBitmapIndex("table", "name", "zizo"));
		// String columnValue1 = retrieveBitmapIndex("table", "name", "joe");
		// String columnValue2 = retrieveBitmapIndex("table", "name", "moe");
		// System.out.println(AND(columnValue1, columnValue2));
		// System.out.println(NOTEQUAL("table", "name", "joe"));
		// System.out.println(v.toString());

		// String strTableName = "teamDB";
		// String string = "January 2, 2009";

		// Hashtable htblColNameType = new Hashtable();
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
		// htblColNameType.put("date", "java.util.Date");
		// table has already been created
		// createTable(strTableName, "id", htblColNameType);

		// Hashtable htblColNameValue = new Hashtable();
		// htblColNameValue.put("id", new Integer(2));
		// // htblColNameValue.put("name", new String("z"));
		// htblColNameValue.put("gpa", new Double(0.95));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		////////////////////////////////////////////////////

		//////////////////////////////////////////
		// string = "January 2, 2007";
		// htblColNameValue.put("id", new Integer(1));
		// // htblColNameValue.put("name", new String("d"));
		// htblColNameValue.put("gpa", new Double(1.25));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//////////////////////////////////////////////////////
		// string = "January 2, 2006";
		// htblColNameValue.put("name", new String("c"));
		// htblColNameValue.put("date", new Date(string));
		// htblColNameValue.put("id", new Integer(11));
		// // htblColNameValue.put("name", new String("c"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		////////////////////////////////////////////////
		// string = "January 2, 2006";
		// htblColNameValue.put("id", new Integer(7));
		// // htblColNameValue.put("name", new String("c"));
		// // htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		///////////////////////////////////////////////////////

		// string = "January 2, 2017";
		// htblColNameValue.put("id", new Integer(5));
		// htblColNameValue.put("name", new String("a"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		//////////////////////////////////////////////////////
		// string = "January 2, 2018";
		// htblColNameValue.put("id", new Integer(6));
		// htblColNameValue.put("name", new String("d"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//////////////////////////////////////////////////////
		// string = "January 2, 1950";
		// htblColNameValue.put("name", new String("y"));
		// htblColNameValue.put("gpa", new Double(1.3));
		// htblColNameValue.put("id", new Integer(11));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		//////////////////////////////////////////////////////
		// string = "December 15, 1998";
		// htblColNameValue.put("id", new Integer(15));
		// htblColNameValue.put("name", new String("Moe"));
		// htblColNameValue.put("gpa", new Double(2.7));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// readTables(strTableName);

		////////////////////////////////////////////////////
		// string = "January 2, 1999";
		// htblColNameValue.put("id", new Integer(2));
		// htblColNameValue.put("name", new String("abcdefg"));
		// htblColNameValue.put("gpa", new Double(4.4255));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// readTables(strTableName);

		///////////////////////////////////////////////////
		// string = "January 2, 1999";
		// htblColNameValue.put("id", new Integer(3));
		// htblColNameValue.put("name", new String("abcdefg"));
		// htblColNameValue.put("gpa", new Double(4.0));
		// htblColNameValue.put("date", new Date(string));
		// updateTable(strTableName, "2", htblColNameValue);
		// htblColNameValue.clear();
		// readTables(strTableName);
		///////////////////////////////////////////////////
		// htblColNameValue.put("id", new Integer(11));
		// htblColNameValue.put("name", deprecated);
		// htblColNameValue.put("gpa", deprecated);
		// htblColNameValue.put("date", deprecated);
		// deleteFromTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// readTables(strTableName);
	}

	static class Pair {
		String path;
		int idx;

		Pair(String a, int b) {
			path = a;
			idx = b;
		}

		public String toString() {
			return "Path: " + path + "Index: " + idx + "\n";
		}
	}

	static class BitmapPair implements Serializable {
		String value, bitmap;

		BitmapPair(String a, String b) {
			value = a;
			bitmap = b;
		}

		public String toString() {
			return "Value: " + value + " Bitmap: " + bitmap + '\n';
		}
	}
}
