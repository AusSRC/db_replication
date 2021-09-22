import csv
import os,binascii

# Output file for results
OUTPUTFILE = "results.csv"
STARTING_ID = 50000
PRODUCT_SIZE = 1024


class BenchmarkUtils:
    """Utilities for benchmarking

    """
    def __init__(self, output = OUTPUTFILE):
        self.output = output
        self.fieldsnames = ['started','ended','rows','operation']

    def addStats(self, row, nrows, operation = None, table = None):
        """Creates stats for each individual operation performed within unit tests functions
        
        """
        output_stats = {'started':row[0], 
          'ended':row[1], 
          'rows':nrows,
          'operation': operation,
          'table': table}

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
                # Details to populate wallaby.run (see wallaby.run_insert.template)
                if table=="wallaby.run":
                    fb.write(template_lines[0])
                    for i in range(STARTING_ID,STARTING_ID + nrows-1):
                        fb.write(template_lines[1].replace("{nrow}",str(i)) + ",")
                    # Write last row :)
                    fb.write(template_lines[1].replace("{nrow}",str(i+1)) + ";")
                elif table=="wallaby.instance":
                    fb.write(template_lines[0])
                    for i in range(STARTING_ID,STARTING_ID + nrows-1):
                        fb.write(template_lines[1].replace("{nrow}",str(i))
                                                  .replace("{detection_id}",str(i)) + ",")
                    # Write last row :)
                    fb.write(template_lines[1].replace("{nrow}",str(i+1))
                                              .replace("{detection_id}",str(i+1)) + ";")                    
                # Details to populate wallaby.products (wallaby.products_insert.template)
                # Pay attention: cube size is fixed to 10MBytes
                elif table=="wallaby.products":
                    fb.write(template_lines[0])
                    for i in range(STARTING_ID,STARTING_ID + nrows-1):
                        fb.write(template_lines[1].replace("{nrow}",str(i))
                                                  .replace("{benchmark_nrow}",str(i))
                                                  .replace("{cube}",str(binascii.b2a_hex(os.urandom(PRODUCT_SIZE)).hex())) + ",")
                    # Write last row :)
                    fb.write(template_lines[1].replace("{nrow}",str(i+1))
                                                  .replace("{benchmark_nrow}",str(i+1))
                                                  .replace("{cube}",str(binascii.b2a_hex(os.urandom(PRODUCT_SIZE)).hex())) + ";")
        elif operation == "delete":
            
            with open(str(nrows) + "_"+ operation + ".sql", 'w') as fb:
                # Details to delete wallaby.run (see wallaby.run_delete.template)
                if table=="wallaby.run":
                    fb.write(template_lines[0].replace("{name}","benchmark") + ";")
                # Details to delete wallaby.products (see wallaby.products_delete.template)
                elif table=="wallaby.products":
                    fb.write(template_lines[0].replace("{benchmark_nrow}",STARTING_ID) + ";")
        
    def getBatch(self,nrows, table, operation):
        """Return sentences generated
        
        """
        with open(str(nrows) + "_"+ operation + ".sql", 'r') as f:
            contents = f.read()
        return contents


