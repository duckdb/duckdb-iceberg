import re
import os

# Read the log file (hopefully it isn't too big)
with open("polaris_catalog/polaris-server.log", "r") as file:
    log_content = file.read()

# Regular expression to capture the credentials
match = re.search(r"realm: POLARIS root principal credentials: (\w+):(\w+)", log_content)

if match:
    root_user = match.group(1)
    root_password = match.group(2)
    if root_user and root_password:
        # Write client_id and client_secret to separate files
        with open("polaris_root_id.txt", "w") as id_file:
            id_file.write(root_user)

        with open("polaris_root_password.txt", "w") as secret_file:
            secret_file.write(root_password)



else:
    print("Credentials not found in the log file.")
    exit(1)
