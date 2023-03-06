from distutils.version import LooseVersion

import dxdata
import dxpy
import pyspark
import pyspark.sql.functions as fx


class Participant:
    def __init__(self):
        self.participant = self.get_participant_dataset()

    @staticmethod
    def get_participant_dataset():
        """
        get participant dataset from dispensed dataset
        :return: participant dataset
        """
        # get dispensed dataset id
        dispensed_dataset = dxpy.find_one_data_object(typename="Dataset",
                                                      name="app*.dataset",
                                                      folder="/",
                                                      name_mode="glob")
        dispensed_dataset_id = dispensed_dataset["id"]

        # get dataset participant from dispensed dataset
        dataset = dxdata.load_dataset(id=dispensed_dataset_id)
        participant = dataset["participant"]

        return participant

    def field_by_id(self, field_id):
        """
        get field title & name by field id
        :param field_id: UKB showcase field id
        :return: field title & name
        """
        # get field
        field_id = str(field_id)
        fields = self.participant.find_fields(name_regex=r'^p{}(_i\d+)?(_a\d+)?$'.format(field_id))
        fields = sorted(fields, key=lambda f: LooseVersion(f.name))

        return {f.title: f.name for f in fields}

    def field_by_keyword(self, keyword):
        """
        get field title & names by keyword
        :param keyword: keyword of interest
        :return: field titles & names
        """
        # get field
        fields = list(self.participant.find_fields(lambda f: keyword.lower() in f.title.lower()))
        fields = sorted(fields, key=lambda f: LooseVersion(f.name))

        return {f.title: f.name for f in fields}

    def get_data(self, field_name_dict, eid_list=None, year_of_last_event_data=None):
        """
        get covariates by eid & field names
        :param eid_list: eid of participants of interest
        :param field_name_dict: dict of field names and their description, e.g., {<description>:<field name>}
        :param year_of_last_event_data: spark df contain year of last event for all participants;
                                        should be generated using method get_year_of_last_event_data from class Database
        :return: return spark df of selected participants data
        """
        # get participant covariates
        spark_df = self.participant.retrieve_fields(names=list(field_name_dict.values()), engine=dxdata.connect())
        if eid_list:
            final_df = spark_df.filter(spark_df["eid"].isin(eid_list))
        else:
            final_df = spark_df
        final_df = final_df.toDF(*list(field_name_dict.keys()))

        # generate age_at_last_event column
        final_df = final_df.join(year_of_last_event_data,
                                 final_df["person_id"] == year_of_last_event_data["eid"], "left")
        final_df = final_df.withColumn("age_at_last_event",
                                       final_df["year_of_last_event"] - final_df["year_of_birth"])
        # keep relevant columns
        cols = list(field_name_dict.keys()) + ["age_at_last_event"]
        final_df = final_df.select(*cols)

        return final_df


class Database:

    def __init__(self):
        self.sc = pyspark.SparkContext()
        self.spark = pyspark.sql.SparkSession(self.sc)
        self.spark.sql("USE " + self.get_database())

    @staticmethod
    def get_database():
        """
        get dispensed database for querying
        :return: dispensed database
        """
        # get dispensed dataset id
        dispensed_database = dxpy.find_one_data_object(classname="database",
                                                       name="app*", folder="/",
                                                       name_mode="glob",
                                                       describe=True)
        dispensed_database_name = dispensed_database["describe"]["name"]

        return dispensed_database_name

    def table_names_by_keyword(self, keyword):
        """
        :param keyword: search keyword
        :return: spark dataframe contain tables having keyword in their names
        """
        tables = self.get_query("SHOW TABLES")
        table_df = tables.filter(tables["tableName"].contains(str(keyword).lower()))
        table_names = [row[0] for row in table_df.select("tableName").collect()]

        return table_names

    def get_table_by_name(self, table_name):
        """
        :param table_name: exact name of table
        :return: spark dataframe
        """
        query = f"""
        SELECT
            *
        FROM
            {table_name}
        """
        table = self.get_query(query)

        return table

    def get_query(self, query):
        """
        get data by SQL query
        :param query: SQL query
        :return: spark dataframe
        """
        return self.spark.sql(query)

    def get_year_of_last_event_data(self):
        """
        :return: spark df contain year of last event data for all participants
        """

        # query to select gp_clinical table
        query = f"""
        SELECT
            *
        FROM
            gp_clinical
        """
        gp_clinical = self.get_query(query)
        year_of_last_gp_event = gp_clinical.groupby("eid").agg(fx.max(fx.year("event_dt")))
        year_of_last_gp_event = year_of_last_gp_event.toDF(*["eid", "year_of_last_event"])

        return year_of_last_gp_event
