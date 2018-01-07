package cs.bigdata.Lab2;

import org.apache.hadoop.util.ProgramDriver;

public class AllDriver {

	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();

		try {
			// Add all the join main classes
			pgd.addClass("Part 1", WordCountDriver.class, "Fréquence des mots par document");
			pgd.addClass("Part 2", WordCountsPerDocDriver.class, "Nbre de mots par doc");
			pgd.addClass("Part 3", WordDocCountDriver.class, "Calcul du TDIDF");

			pgd.driver(args);

			exitCode = 0;
			
		} 
		catch (Throwable e) 
		{
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
