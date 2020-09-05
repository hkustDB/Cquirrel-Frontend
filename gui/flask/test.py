import subprocess

json_file_save_path = './uploads/' + 'test.json'
codegen_file_path = './codegen-1.0-SNAPSHOT.jar'
flink_address = './'
cmd_str = 'java -jar' + ' ' + codegen_file_path + ' ' + json_file_save_path + ' ' + flink_address

output = subprocess.check_output(cmd_str, shell=True)
print(output)