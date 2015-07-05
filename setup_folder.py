import subprocess
import os
import sys
import datetime
import paramiko

# ssh connexion
ssh=paramiko.SSHClient()
ssh.load_system_host_keys()
ssh.connect(hostname='localhost', username='haiprasan86', password='secret')

# Get configuration file data
with open('conf.json') as conf_file:    
    conf = json.load(conf_file)
    conf_file.close()
    
# Create folder on host server
subprocess.call(["mkdir -p -m 777 %s" % (conf["inpath"]), shell=TRUE])
subprocess.call(["mkdir -p -m 777 %s" % (conf["outpath"]), shell=TRUE])

# Create folder on SAS server
def exe_command(command):
    (stdin, stdout, stderr) = ssh.exec_command(command)
    for line in stdout.readlines():
        print line
 
exe_command("mkdir -p -m 777 %s" % (conf["sasdata"])
exe_command("mkdir -p -m 777 %s" % (conf["sasresults"])

# Copy manually input data in conf["sasdata"] repository

# Extract data from SAS server
p="%"
subprocess.call(["nohup sas-crffr /analytics/crffr/.../rpcm_extraction.sas %slet date_fin=%s &" % (p, conf[date_fin]), shell=TRUE])

# Copy SAS data from sas Server
exe_command("mv %s %s" % (conf["sasresults"]+'/*', conf["extract_bucket"])

# Close ssh connexion
ssh.close()
