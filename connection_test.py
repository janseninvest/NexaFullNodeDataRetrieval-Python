import subprocess

def run_shell_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    
    if output:
        return output.decode("utf-8")
    else:
        return ""


if __name__ == "__main__":
    
    command = "C:/Program Files/Nexa/daemon/nexa-cli    -rpcpassword=mypassword -rpcuser=myusername help"
    output = run_shell_command(command.split())
    print(output)