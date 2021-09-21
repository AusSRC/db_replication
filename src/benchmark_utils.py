import csv

# Output file for results
OUTPUTFILE = "results.csv"
STARTING_ID = 50000


class BenchmarkUtils:
    """Utilities for benchmarking

    """
    def __init__(self, output = OUTPUTFILE):
        self.output = output
        self.fieldsnames = ['started','ended','rows','operation']

    def addStats(self, row, nrows, operation = None):
        """Creates stats for each individual operation performed within unit tests functions
        
        """
        output_stats = {'started':row[0], 
          'ended':row[1], 
          'rows':nrows,
          'operation': operation}

        with open(self.output, 'a', newline='') as csvfile:               
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldsnames)
            writer.writerow(output_stats)
    
    
    def buildBatch(self, nrows, table, operation):
        """Creates a batch of sentences to execute if query is insert or delete

        """
        ## Open table template
        with open(table + "_"+ operation + ".template", 'r') as fb:               
            template_lines = fb.readlines()
            
        if operation == "insert":
            with open(str(nrows) + "_"+ operation + ".sql", 'w') as fb:               
                fb.write(template_lines[0])
                for i in range(STARTING_ID,STARTING_ID + nrows-1):
                    fb.write(template_lines[1].replace("{nrow}",str(i)) + ",")
                fb.write(template_lines[1].replace("{nrow}",str(i+1)) + ";")
        elif operation == "delete":
            with open(str(nrows) + "_"+ operation + ".sql", 'w') as fb:
                fb.write(template_lines[0].replace("{name}","benchmark") + ";")
        
    def getBatch(self,nrows, table, operation):
        """Return sentences generated
        
        """
        with open(str(nrows) + "_"+ operation + ".sql", 'r') as f:
            contents = f.read()
        return contents


