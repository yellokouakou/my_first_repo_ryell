from pyspark.sql.types import StructType


class ExtractorSQL():
    def __init__(self, spark, is_oracle, driver, url, user, password):
        schema = StructType([])
        self.spark = spark
        self.driver = driver
        self.jdbc_url = url
        self.user = user
        self.password = password
        self.df = spark.createDataFrame([], schema)
        self.is_oracle = is_oracle

    def load(self, sql):
        if self.is_oracle:
            self.df = self.spark.read.format('jdbc').option('driver', self.driver).option('url', self.jdbc_url).option('dbtable', '({sql}) src'.format(sql=sql)).option('user', self.user).option('password', self.password).load()
        else:
            self.df = self.spark.read.format('jdbc').option('driver', self.driver).option('url', self.jdbc_url).option('dbtable', '({sql}) as src'.format(sql=sql)).option('user', self.user).option('password', self.password).load()

    def save(self, filename):
        self.df.write.save(filename, format="csv")
        print("Dataframe saved")

    def data(self):
        return self.df

    def to_table(self, tab):
        self.df.createOrReplaceTempView(tab)
