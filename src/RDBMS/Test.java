package RDBMS;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class Test {

	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		String strTableName = "Student";
		String strTableName2 = "People";
		Hashtable htblColNameType = new Hashtable();
		Hashtable htblColNameValue = new Hashtable();
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gender", "java.lang.Boolean");
		// htblColNameType.put("date", "java.lang.Date");
		// DBApp.createTable(strTableName, "gender", htblColNameType);
		//
		// htblColNameValue.put("id", new Integer( 1600));
		// htblColNameValue.put("name", new String("Zoz?"));
		// htblColNameValue.put("gender", new Boolean( "false" ));
		// htblColNameValue.put("date", new Date(1997 - 1900, 1 - 1, 19));
		// DBApp.updateTable(strTableName2, "Sun Jan 19 00:00:00 GMT+02:00 1997",
		// htblColNameValue);
		// DBApp.deleteFromTable(strTableName2, htblColNameValue);
		// DBApp.insertIntoTable(strTableName2, htblColNameValue);
		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 1200 ));
		// htblColNameValue.put("name", new String("Joe" ) );
		// htblColNameValue.put("gpa", new Double( 3 ) );
		// DBApp.updateUsingBitmap(strTableName, "2000",htblColNameValue);
		// DBApp.insertIntoTable(strTableName , htblColNameValue );
		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 1400 ));
		// htblColNameValue.put("name", new String("Dalia Noor" ) );
		// htblColNameValue.put("gpa", new Double( 1.25 ) );
		// DBApp.updateUsingBitmap(strTableName, "2000",htblColNameValue);
		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 200 ));
		// htblColNameValue.put("name", new String("John Noor" ) );
		// htblColNameValue.put("gpa", new Double( 1.5 ) );
		// DBApp.insertIntoTable( strTableName , htblColNameValue );
		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer(7842));
		// htblColNameValue.put("name", new String("Yes"));
		// htblColNameValue.put("gender", new Boolean(true));
		// htblColNameValue.put("date", new Date(1997 - 1900, 14 - 1, 12));
		// DBApp.insertIntoTable(strTableName, htblColNameValue);
		// DBApp.readTables("Student");
		DBApp.readTables(strTableName);
		// DBApp.createBitmapIndex(strTableName2, "date");
		System.out.println(DBApp.EQUALNONINDEXED(strTableName, "date", new Date(1997 - 1900, 1 - 1, 19) + ""));
		// Bitmap.readBitmap(strTableName, "id");
		Bitmap.readBitmap(strTableName2, "date");
	}

}
