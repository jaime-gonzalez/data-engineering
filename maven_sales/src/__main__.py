import os
import luigi
import subprocess

from integration import extractAndLoad

class ExtractAndLoadTask(luigi.Task):
    output_schema = luigi.Parameter()
    output_table = luigi.Parameter()
    file_name = luigi.Parameter()

    def run(self):
        extractAndLoad(output_schema=self.output_schema, output_table=self.output_table, file_name=self.file_name)

if __name__ == '__main__':
    tasks = [
        ExtractAndLoadTask(output_schema='staging', output_table='inventory', file_name='inventory.csv'),
        ExtractAndLoadTask(output_schema='staging', output_table='products', file_name='products.csv'),
        ExtractAndLoadTask(output_schema='staging', output_table='sales', file_name='sales.csv'),
        ExtractAndLoadTask(output_schema='staging', output_table='stores', file_name='stores.csv')
    ]

    luigi.build(tasks, local_scheduler=True, workers=1)

    dbt_project_directory = "/home/jaimegonzalez/Desktop/data-engineering/maven_sales/dbt"
    os.chdir(dbt_project_directory)

    dbt_commands = [
        "cd {} && source /home/jaimegonzalez/dbt-env/bin/activate && dbt run".format(dbt_project_directory),
        "cd {} && source /home/jaimegonzalez/dbt-env/bin/activate && dbt docs generate".format(dbt_project_directory)
    ]

    for command in dbt_commands:
        subprocess.run(command, shell=True, executable='/bin/bash')
