package edu.neu.hw7;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertApplication {

	private static Configuration conf = null;

	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
	}

	/**
	 * Create a table
	 */
	public static void creatTable(String tableName, String[] familys) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	/**
	 * Delete a table
	 */
	public static void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		//	System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Put (or insert) a row
	 */
	public static void addRecord(HTable table, String rowKey, String family, String qualifier, String value)
			throws Exception {
		try {

			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
		//	System.out.println("insert recored " + rowKey + " to table " + table.getName() + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 */
	public static void delRecord(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
	//	System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * Get a row
	 */
	public static void getOneRecord(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			/*
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
			*/
		}
	}

	/**
	 * Scan (or list) a table
	 */
	public static void getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					/*
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
					*/
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] agrs) throws IOException {
		//Movies
		try {
			String tablename = "Movies";
			String[] familys = { "Info" };
			InsertApplication.creatTable(tablename, familys);

			// movie file
			String movieFile = "doc/movies.dat";
			File filem = new File(new File(movieFile).getAbsolutePath());

			try {
				BufferedReader brm = new BufferedReader(new FileReader(filem));
				String line = null;
				// List<org.bson.Document> movies = new
				// ArrayList<org.bson.Document>();
				// int counter = 0;
				HTable table = new HTable(conf, tablename);
				while ((line = brm.readLine()) != null) {
					String data = line;
					// data = data.replaceAll(",", ",,");
					String[] value = data.split("::");
					String movieId = "";
					String title = "";
					String genres = "";
					if (value.length == 3) {
						// movieId = Integer.parseInt(value[0]);
						movieId = value[0];
						title = value[1];
						genres = value[2];
						InsertApplication.addRecord(table, movieId, familys[0], "title", title);
						InsertApplication.addRecord(table, movieId, familys[0], "genres", genres);
					} else if (value.length == 2) {
						movieId = value[0];
						title = value[1];
						InsertApplication.addRecord(table, movieId, familys[0], "title", title);
					} else {
						movieId = value[0];
						InsertApplication.addRecord(table, movieId, familys[0], "", "");
					}
					/*
					 * sparse --> don't have to store too many columns
					 * counter++; if (counter > 500) { movie.insertMany(movies);
					 * movies.clear(); counter = 0; }
					 */
					// System.out.println(movieId +"**"+ title +"**"+genres);

				}

				System.out.println("===========get one record========");
				InsertApplication.getOneRecord(tablename, "1");
				/*
				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(tablename);

				System.out.println("===========del one record========");
				InsertApplication.delRecord(tablename, "2");
				InsertApplication.getAllRecord(tablename);

				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(tablename);
				*/
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* User Table */
		try {
			String userTable = "Users";
			String[] familys = { "Info" };
			InsertApplication.creatTable(userTable, familys);

			// user file
			String userFile = "doc/users.dat";
			File fileu = new File(new File(userFile).getAbsolutePath());

			try {
				BufferedReader bru = new BufferedReader(new FileReader(fileu));
				String line = null;
				// List<org.bson.Document> movies = new
				// ArrayList<org.bson.Document>();
				// int counter = 0;
				HTable table = new HTable(conf, userTable);
				while ((line = bru.readLine()) != null) {
					String data = line;
					// data = data.replaceAll(",", ",,");
					String[] value = data.split("::");
					String userId = "";
					String gender = "";
					String age = "";
					String occupation = "";
					String zip = "";
					if (value.length == 5) {
						// movieId = Integer.parseInt(value[0]);
						userId = value[0];
						gender = value[1];
						age = value[2];
						occupation = value[3];
						zip = value[4];
						InsertApplication.addRecord(table, userId, familys[0], "age", age);
						InsertApplication.addRecord(table, userId, familys[0], "gender", gender);
						InsertApplication.addRecord(table, userId, familys[0], "occupation", occupation);
						InsertApplication.addRecord(table, userId, familys[0], "zipCode", zip);
					} else if (value.length == 4) {
						userId = value[0];
						gender = value[1];
						age = value[2];
						occupation = value[3];
						InsertApplication.addRecord(table, userId, familys[0], "age", age);
						InsertApplication.addRecord(table, userId, familys[0], "gender", gender);
						InsertApplication.addRecord(table, userId, familys[0], "occupation", occupation);
					} else if (value.length == 3) {
						userId = value[0];
						gender = value[1];
						age = value[2];
						InsertApplication.addRecord(table, userId, familys[0], "age", age);
						InsertApplication.addRecord(table, userId, familys[0], "gender", gender);
					} else if (value.length == 2) {
						userId = value[0];
						gender = value[1];
						InsertApplication.addRecord(table, userId, familys[0], "gender", gender);
					} else {
						userId = value[0];
						InsertApplication.addRecord(table, userId, familys[0], "age", age);
					}
					/*
					 * sparse --> don't have to store too many columns
					 * counter++; if (counter > 500) { movie.insertMany(movies);
					 * movies.clear(); counter = 0; }
					 */
					// System.out.println(movieId +"**"+ title +"**"+genres);

				}

				System.out.println("===========get one record========");
				InsertApplication.getOneRecord(userTable, "1");
				/*
				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(userTable);

				System.out.println("===========del one record========");
				InsertApplication.delRecord(userTable, "2");
				InsertApplication.getAllRecord(userTable);

				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(userTable);
				*/
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* Rating File */
		try {
			String ratingTable = "Ratings";
			String[] familys = { "Info" };
			InsertApplication.creatTable(ratingTable, familys);

			// rating file
			String ratingFile = "doc/ratings.dat";
			File filer = new File(new File(ratingFile).getAbsolutePath());

			try {
				BufferedReader brr = new BufferedReader(new FileReader(filer));
				String line = null;
				// List<org.bson.Document> movies = new
				// ArrayList<org.bson.Document>();
				// int counter = 0;
				HTable table = new HTable(conf, ratingTable);
				while ((line = brr.readLine()) != null) {
					String data = line;
					// data = data.replaceAll(",", ",,");
					String[] value = data.split("::");
					String userId = "";
					String movieId = "";
					String ratings = "";
					String timestamp = "";
					if (value.length == 4) {
						// movieId = Integer.parseInt(value[0]);
						userId = value[0];
						movieId = value[1];
						ratings = value[2];
						timestamp = value[3];
						//specific length of userID/movieID (rowkey)
						StringBuffer userID = new StringBuffer(userId);
						while (userID.length() < 4) {
							userID.insert(0, '0');
						}
						StringBuffer movieID = new StringBuffer(movieId);
						while (movieID.length() < 4) {
							movieID.insert(0, '0');
						}
						InsertApplication.addRecord(table, userID.toString()+movieID.toString(), familys[0], "ratings", ratings);
						InsertApplication.addRecord(table, userID.toString()+movieID.toString(), familys[0], "timestamp", timestamp);
					} else if (value.length == 3) {
						userId = value[0];
						movieId = value[1];
						ratings = value[2];
						
						StringBuffer userID = new StringBuffer(userId);
						while (userID.length() < 4) {
							userID.insert(0, '0');
						}
						StringBuffer movieID = new StringBuffer(movieId);
						while (movieID.length() < 4) {
							movieID.insert(0, '0');
						}
						InsertApplication.addRecord(table, userID.toString()+movieID.toString(), familys[0], "ratings", ratings);

					} else {
						userId = value[0];
						movieId = value[1];

						StringBuffer userID = new StringBuffer(userId);
						while (userID.length() < 4) {
							userID.insert(0, '0');
						}
						StringBuffer movieID = new StringBuffer(movieId);
						while (movieID.length() < 4) {
							movieID.insert(0, '0');
						}
						
						InsertApplication.addRecord(table, userID.toString()+movieID.toString(), familys[0], "", "");

					}
					/*
					 * sparse --> don't have to store too many columns
					 * counter++; if (counter > 500) { movie.insertMany(movies);
					 * movies.clear(); counter = 0; }
					 */
					// System.out.println(movieId +"**"+ title +"**"+genres);

				}

				System.out.println("===========get one record========");
				InsertApplication.getOneRecord(ratingTable, "1");
				/*
				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(ratingTable);

				System.out.println("===========del one record========");
				InsertApplication.delRecord(ratingTable, "2");
				InsertApplication.getAllRecord(ratingTable);

				System.out.println("===========show all record========");
				InsertApplication.getAllRecord(ratingTable);
				*/
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*Ask to perform operation*/
	//	 System.out.println("Which information you need?\n1.Movies\n2.Users\n3.Ratings");
		Scanner sc=new Scanner(System.in);  
		   System.out.println("Which information you need?\n1.Movies\n2.Users\n3.Ratings");  
		   String tableName = null;  
		   int tableChoice = sc.nextInt();
		   if (tableChoice == 1) {
			   tableName = "Movies";
		   }else if (tableChoice == 2) {
			   tableName = "Users";
		   }else if (tableChoice == 3) {
			   tableName = "Ratings";
		   }else {
			   System.out.println("Please Input appropriate number");
			   tableChoice = sc.nextInt();
		   }
		   HTable table = new HTable(conf, tableName);
		   System.out.println("What do you want to do?(Enter Number Please.)\n1.Get All Information.\n2.Search");  
		   int choice=sc.nextInt();  
		   if (choice == 1) {
			   try {
					//	HTable table = new HTable(conf, tableName);
						Scan s = new Scan();
						ResultScanner rs = table.getScanner(s);
						for (Result r : rs) {
							for (KeyValue kv : r.raw()) {
								System.out.print(new String(kv.getRow()) + " ");		
								System.out.print(new String(kv.getFamily()) + ":");
								System.out.print(new String(kv.getQualifier()) + " ");
								System.out.print(kv.getTimestamp() + " ");
								System.out.println(new String(kv.getValue()));					
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
		   }else if (choice == 2) {
				   System.out.println("Please input id:");  
				   String rowKey= sc.next();  
				   try{
					   Get get = new Get(rowKey.getBytes());
						Result rs = table.get(get);
						for (KeyValue kv : rs.raw()) {
							System.out.print(new String(kv.getRow()) + " ");	
							System.out.print(new String(kv.getFamily()) + ":");
							System.out.print(new String(kv.getQualifier()) + " ");
							System.out.print(kv.getTimestamp() + " ");
							System.out.println(new String(kv.getValue()));
						}
				   }catch(Exception e) {
					  System.out.println("No Such Information");
				   }
			   
		   }else {
			   System.out.println("No Such Option");
		   }
		  
		   sc.close();   
		}
	}

