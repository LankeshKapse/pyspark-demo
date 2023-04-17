import os
import subprocess
from zipfile import ZipFile

main_file = "src/pspark/demo1.py"
extract_main_file_loc = ""
with ZipFile("../../dist/pyspark-demo-0.0.1.zip", "r") as zipper:
    list_of_file_names = zipper.namelist()

    for name in list_of_file_names:
        if name.endswith(main_file):
            run_file_name = name[-name[::-1].index("/"):]
            zipper.extract(member=name)
            extract_main_file_loc = name

print(extract_main_file_loc)

my_env = os.environ.copy()
run_script_path = extract_main_file_loc
python_script_folder = "C:\\Users\\Lucky\\Documents\\learning\\project-interview-2023\\git\\pyspark-demo\\.venv" \
                       "\\Scripts"
python_activate_cmd = f"{python_script_folder}\\activate.bat"

python_set_cmd = f"SET PYSPARK_PYTHON={python_script_folder}\\python.exe"
submit_cmd = my_env["SPARK_HOME"] + "\\bin\\spark-submit --master local --deploy-mode client " + run_script_path
spark_job_submit_cmd = f"{python_set_cmd} && {submit_cmd}"
print(spark_job_submit_cmd)
with open("output.txt", "w") as f:
    p1 = subprocess.Popen(spark_job_submit_cmd, shell=True, stdout=f, text=True)
#
# print(p1.stdout)
# print(p1.stderr)
