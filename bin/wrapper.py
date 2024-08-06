def main():
    input_data = read_data(app_config.json)
    code_master_obj - CodeMaster(data=input_data,)



def read_data(config:dict) -> dict:
    input_data = {}
    for table in config["input_tables"]:
        df = spark.read.table(table)
        input_data[table] = df

    return input_data

