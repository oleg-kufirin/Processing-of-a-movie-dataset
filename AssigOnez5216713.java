/*

==============+==============+==========+===============
              | Assignment 1 |          | Oleg Kufirin |
==============+==============+==========+===============

The program consists of two jobs chained together job_1 -> job_2.

The general idea is first to find all movies/ratings watched by one user.
After that discard users who watched only one movie, and then produce combinations of pairs of movies for one user
and then combine the results of pairs of movies for all users.

--- Job_1 ---
It has one mapper (Mapper_First) and one reducer (Reducer_First).

Mapper_First reads the input file, parses each string and extracts the following information:
	- User ID for using as an output key
	- Movie ID and Rating as a custom structure Movie_Rating.class which treats Movie ID as Text and Rating as Integer
	  This structure is used as an output value
As a result Mapper_First produces pairs of (Movie ID, Rating) for each User ID.

Reducer_First receives as an input all Movie ID and Ratings watched by one User ID.
It parses each input record and discards all users who watched only one movie since we are not interested in them.
Then the reducer simply converts all Movie ID and Rating pairs in Text along with User ID and outputs in an temporary text file.
It has the following format "UserID\tMovieID,Rating\tMovieID,Rating\t...".

--- Job_2 ---
It has one mapper (Mapper_Second) and one reducer (Reducer_Second).

Mapper_second reads the temp file and for each user generates all movie pairs, where first value is greater than second,
and corresponding User ID and ratings tuple for each pair.
For pairs of movies I used a custom structure Movie_pair.class and for User ID and two ratings a custom structure User_Rating_pair.class.

Reducer_Second receives Movie_pair class as an input key and a list of User_Rating_pair class as an input value.
Then it simply copies the latter in an ArrayWritable object, which is passed to the output as value.
As the output key I used the input key of Movie_pair class.

At the end the resulting file is produced in the output folder and the intermediate temp file is deleted.

*/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AssigOnez5216713 {

//********************************************************************************
//		PART 1 - for job_1
//********************************************************************************
	
	// class representing movie and rating pairs (Movie_ID, Rating)
	// used as output class value of the first mapper
	public static class Movie_Rating implements Writable {
		
		private Text movie;
		private IntWritable rating;
		
		public Movie_Rating() {
			this.movie = new Text("");
			this.rating = new IntWritable(-1);
		}
		
		public Movie_Rating(Text movie, IntWritable rating) {
			super();
			this.movie = movie;
			this.rating = rating;
		}
		
		public Text getMovie() {
			return movie;
		}
		public void setMovie(Text movie_) {
			movie = movie_;
		}
		public IntWritable getRating() {
			return rating;
		}
		public void setRating(IntWritable rating_) {
			rating = rating_;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie.readFields(data);
			this.rating.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie.write(data);
			this.rating.write(data);	
		}
		
		public String toString() {
			return this.movie.toString() + "," + this.rating.toString() + "\t"; // using TAB as a delimiter
		}
		
	}

	// first mapper - see the description at the top
	public static class Mapper_First extends Mapper<LongWritable, Text, Text, Movie_Rating> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Movie_Rating>.Context context)
				throws IOException, InterruptedException {
			
			String[] parts = value.toString().split("::");
			Movie_Rating mr = new Movie_Rating();
			mr.setMovie(new Text(parts[1]));
			mr.setRating(new IntWritable(Integer.parseInt(parts[2])));
			
			context.write(new Text(parts[0]), mr);
			
		}
		
	}
	
	// first reducer - see the description at the top
	public static class Reducer_First extends Reducer<Text, Movie_Rating, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Movie_Rating> values,
				Reducer<Text, Movie_Rating, Text, Text>.Context context) throws IOException, InterruptedException {
			
			Movie_Rating value = new Movie_Rating();
			String s = "";
			int count = 0;
			
			for (Movie_Rating a: values) {
				value.setMovie(new Text(a.getMovie().toString()));
				value.setRating(new IntWritable(a.getRating().get()));
				s += value.toString();
				count += 1;
			}
			
			if (count > 1) // discard if user watched only one movie
				context.write(key, new Text(s));
			
		}
		
	}
	
// ********************************************************************************
// 		PART 2 - for job_2
// ********************************************************************************
	
	// class to produce the final output value in the second reducer
	public static class MyArrayWritable extends ArrayWritable {

		public MyArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
			super(valueClass, values);
		}
		
		@Override
		public String toString() {
			String ret = null;
			for (Writable w: this.get())
				if (ret == null)
					ret = "[" + w.toString();
				else
					ret += "," + w.toString();
			return ret + "]";
		}
		
	}
	
	// class to produce pairs of movies (Movie_1, Movie_2) as an output key class in the second mapper and the second reducer
	public static class Movie_pair implements WritableComparable<Movie_pair> {
		
		private Text movie_1;
		private Text movie_2;
		
		public Movie_pair() {
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
		}
		
		public Movie_pair(Text movie_1, Text movie_2) {
			super();
			this.movie_1 = movie_1;
			this.movie_2 = movie_2;
		}

		public Text getMovie_1() {
			return movie_1;
		}
		
		public void setMovie_1(Text movie_1) {
			this.movie_1 = movie_1;
		}
		
		public Text getMovie_2() {
			return movie_2;
		}
		
		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_1.readFields(data);
			this.movie_2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_1.write(data);
			this.movie_2.write(data);
		}
		
		public String toString() {
			return "(" + this.movie_1.toString() + "," + this.movie_2.toString() + ")";
		}

		@Override // sorting by the first movie in ascending order
		public int compareTo(Movie_pair o) {
			int cmp = movie_1.compareTo(o.movie_1);
			if (0 != cmp)
				return cmp;
			return movie_2.compareTo(o.movie_2);
		}		

	}
	
	// class of user rating tuples (User_ID, Rating_1, Rating_2) to be used as an output value class in the second mapper
	public static class User_Rating_pair implements Writable {
		
		private Text user;
		private IntWritable rating_1;
		private IntWritable rating_2;
		
		public User_Rating_pair() {
			this.user = new Text("");
			this.rating_1 = new IntWritable(-1);
			this.rating_2 = new IntWritable(-1);
		}
		
		public User_Rating_pair(Text user, IntWritable rating_1, IntWritable rating_2) {
			super();
			this.user = user;
			this.rating_1 = rating_1;
			this.rating_2 = rating_2;
		}
		
		public Text getUser() {
			return user;
		}
		
		public void setUser(Text user) {
			this.user = user;
		}
		
		public IntWritable getRating_1() {
			return rating_1;
		}
		
		public void setRating_1(IntWritable rating_1) {
			this.rating_1 = rating_1;
		}
		
		public IntWritable getRating_2() {
			return rating_2;
		}
		
		public void setRating_2(IntWritable rating_2) {
			this.rating_2 = rating_2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rating_1.readFields(data);
			this.rating_2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.rating_1.write(data);
			this.rating_2.write(data);
		}
		
		public String toString() {
			return "(" + this.user.toString() + "," + this.rating_1.toString() + "," + this.rating_2.toString() + ")";
		}
		
	}
	
	// second mapper - see details at the top comment
	public static class Mapper_Second extends Mapper<LongWritable, Text, Movie_pair, User_Rating_pair> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Movie_pair, User_Rating_pair>.Context context)
				throws IOException, InterruptedException {

			String[] parts = value.toString().split("\t");
			
			for (int i = 1; i < parts.length; i++) {
				Text m1_m = new Text(parts[i].toString().split(",")[0]);
				IntWritable m1_r = new IntWritable(Integer.parseInt(parts[i].toString().split(",")[1]));
				for (int j = i+1; j < parts.length; j++) {
					Text m2_m = new Text(parts[j].toString().split(",")[0]);
					IntWritable m2_r = new IntWritable(Integer.parseInt(parts[j].toString().split(",")[1]));
					
					User_Rating_pair urp = new User_Rating_pair();
					urp.user = new Text(parts[0].toString());
					Movie_pair mp = new Movie_pair();
					
					if (m1_m.compareTo(m2_m) < 0) {
						mp.movie_1 = new Text(m1_m);
						mp.movie_2 = new Text(m2_m);
						urp.rating_1 = m1_r;
						urp.rating_2 = m2_r;
					}
					else {
						mp.movie_1 = new Text(m2_m);
						mp.movie_2 = new Text(m1_m);
						urp.rating_1 = m2_r;
						urp.rating_2 = m1_r;
					}
					
					context.write(mp, urp);
				}
				
			}
			
		}
		
	}

	// second reducer - see details at the top comment
	public static class Reducer_Second extends Reducer<Movie_pair, User_Rating_pair, Movie_pair, MyArrayWritable> {

		@Override
		protected void reduce(Movie_pair key, Iterable<User_Rating_pair> values,
				Reducer<Movie_pair, User_Rating_pair, Movie_pair, MyArrayWritable>.Context context)
				throws IOException, InterruptedException {
				
				ArrayList<User_Rating_pair> vals = new ArrayList<User_Rating_pair>();
				
				while (values.iterator().hasNext()) {			
					User_Rating_pair a = new User_Rating_pair();
					a = values.iterator().next();
					vals.add(new User_Rating_pair(new Text(a.getUser()), new IntWritable(a.getRating_1().get()), new IntWritable(a.getRating_2().get())));
				}
				
				Writable[] my_array = new User_Rating_pair[vals.size()];
				
				for (int i=0; i < vals.size(); i++) {
					my_array[i] = vals.get(i);
				}
				
				MyArrayWritable val = new MyArrayWritable(User_Rating_pair.class, my_array);

				context.write(key, val);
				
		}

	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// *******************************************************
		// JOB 1
		// *******************************************************
		
		Job job_1 = Job.getInstance(conf, "Assignment_1_1");
		
		job_1.setMapperClass(Mapper_First.class);
		job_1.setReducerClass(Reducer_First.class);
		
		job_1.setMapOutputKeyClass(Text.class);
		job_1.setMapOutputValueClass(Movie_Rating.class);
		
		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(Movie_Rating.class);
		
		job_1.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job_1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_1, new Path("temp_output"));

		job_1.waitForCompletion(true);
		
		// *******************************************************
		// JOB 2
		// *******************************************************
		
		Job job_2 = Job.getInstance(conf, "Assignment_1_2");
		
		job_2.setMapperClass(Mapper_Second.class);
		job_2.setReducerClass(Reducer_Second.class);

		job_2.setMapOutputKeyClass(Movie_pair.class);
		job_2.setMapOutputValueClass(User_Rating_pair.class);
		
		job_2.setOutputKeyClass(Movie_pair.class);
		job_2.setOutputValueClass(MyArrayWritable.class);
		
		job_2.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job_2, new Path("temp_output/part-r-00000"));
		FileOutputFormat.setOutputPath(job_2, new Path(args[1]));

		job_2.waitForCompletion(true);
		
		// deleting the temp directory
		FileUtils.deleteDirectory(new File("temp_output"));
		
	}

}
