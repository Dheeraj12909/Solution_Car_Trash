import configs.meta_config as configs
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from Analysis import *
import sys
def do_imports():
    global spark
    from session.spark_session import get_session
    spark = get_session()
    print("spark_application_id:<{}>".format(spark.sparkContext.applicationId))


class Solution:
    """
    Solution Class
    """
    def __init__(self):
        """
        Defining the attributes of the class Solution
        """
        self.primary_person_use_file = None
        self.restrict_use_file = None
        self.units_use_file = None
        self.damages_use_file = None
        self.charges_use_file = None
        self.endorse_use_file = None
        self.session = None
        self.params_set = self.set_params()
        do_imports()

    def set_params(self):
        """
        Function is to set the values for the Entity class parameters.
        """
        try:
            self.primary_person_use_file = configs.primary_person_use_file
            self.restrict_use_file = configs.restrict_use_file
            self.units_use_file = configs.units_use_file
            self.damages_use_file = configs.damages_use_file
            self.charges_use_file = configs.charges_use_file
            self.endorse_use_file = configs.endorse_use_file
        except Exception as e:
            print(e)
            return False

    def run_job(self):
        print("Analysis 1 is starting")
        print("*"*100)
        analysis_1(spark, self.primary_person_use_file)
        print("Analysis 2 is starting")
        print("*" * 100)
        analysis_2(spark, self.units_use_file)
        print("Analysis 3 is starting")
        print("*" * 100)
        analysis_3(spark, self.primary_person_use_file)
        print("Analysis 4 is starting")
        print("*" * 100)
        analysis_4(spark, self.units_use_file)
        print("Analysis 5 is starting")
        print("*" * 100)
        analysis_5(spark, self.units_use_file, self.primary_person_use_file)
        print("Analysis 6 is starting")
        print("*" * 100)
        analysis_6(spark, self.primary_person_use_file, self.units_use_file)
        print("Analysis 7 is starting")
        print("*" * 100)
        analysis_7(spark, self.units_use_file, self.damages_use_file)
        print("Analysis 8 is starting")
        print("*" * 100)
        analysis_8(spark, self.primary_person_use_file, self.units_use_file)





def main():
    e = Solution()
    flag = e.run_job()
    if flag:
        sys.exit(0)
    else:
        sys.exit(-1)


if __name__ == '__main__':
    main()
