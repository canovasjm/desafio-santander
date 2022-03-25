## Exercise 1  

Usage: `python main.py config_csv.env`

For this exercise I created a function called `ingest_data()` which is called from `main.py`

Depending on the `.env` file passed to the script, the function uses some of the following functions which are defined in `/helper/helpers.py`. These functions are:

- `create_spark_session()`

- `read_file_with_header()`

- `read_file_with_no_header()`

- `write_file_to_parquet()`

For more details about what each function does, please refer to the corresponding docstring.