import csv

# Output file for results
OUTPUTFILE = "results.csv"


class BenchmarkUtils:
    """Utilities for Benchmark

    """
    def __init__(self, output = OUTPUTFILE):
        self.output = output
        self.fieldsnames = ['started','ended','rows','operation']

    def addStats(self, row, operation = None):
        
        output_stats = {'started':row[0], 
          'ended':row[1], 
          'rows':row[2],
          'operation': operation}

        with open(self.output, 'a', newline='') as csvfile:               
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldsnames)
            writer.writerow(output_stats)
    
    
    def buildBatch(self, nrows, table, operation):
        ## Open table template
        with open(table + "_"+ operation + ".template", 'r') as fb:               
            template_lines = fb.readlines()
            
        with open(str(nrows) + "_"+ operation + ".sql", 'w') as fb:               
            fb.write(template_lines[0])
            for i in range(0,nrows-1):
                fb.write(template_lines[1].replace("{nrow}",str(i)) + ",")
            fb.write(template_lines[1].replace("{nrow}",str(i)) + ";")
        


