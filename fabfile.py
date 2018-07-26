from invoke import task, run, collection, Exit, Collection, Responder
from invoke.context import Context
from fabric2 import Connection
from fabric2 import Config
from fabric2.transfer import Transfer
from patchwork.files import append
from patchwork.transfers import rsync
import os

'''
For Windows 10, ensure that cygwin is installed

https://www.cygwin.com/setup-x86_64.exe

And install <Base>, bash, dash, all cygwin lib and utils and rsync and ssh/openssh

pip install fabric2

sudo apt install python3-pip
'''

# BUGS:
# https://github.com/fabric/fabric/issues/1823

HOSTS = {
    "dev0": {
        "host": "pcdinh@192.168.1.113",
        "password": "x",
        "key_filename": "id_rsa"
    }
}
# ssh_config
# Host dev0
#     HostName     192.168.1.113
#     IdentityFile id_rsa
#     User         pcdinh
# Config.ssh_config_path = "ssh_config"


def get_base_path():
    import os
    return os.path.dirname(os.path.realpath(__file__))


def convert_to_posix_path(path):
    r"""Converts MSDOS-style path(C:\Users) to POSIX path (/cygdrive/c/Users)
    """
    if os.name == 'nt':
        path = os.path.normpath(path).replace('\\', '/')
        return '/cygdrive/%s/%s' % (path[0].lower(), path[3:])
    return path


def get_connection(host_name):
    config = HOSTS[host_name]
    return Connection(
               config["host"],
               connect_kwargs={
                   "key_filename": config["key_filename"],
                   "password": config["password"]
               }
           )


def get_project_path():
    from fabric.main import program
    program.load_collection()
    print(program)
    return program.collection.loaded_from


def synchronize_code(context):
    import os
    import sys
    # Path /cygdrive/d/cygwin64/bin/rsync
    rsync_path = os.environ.get("RSYNC_PATH", None) or "rsync"
    ssh_path = os.environ.get("SSH_PATH", None) or "/cygdrive/d/cygwin64/bin/ssh" if sys.platform == "win32" else "ssh"
    remote_base_path = "/home/pcdinh/code"
    ssh_identity_file_path = convert_to_posix_path(os.path.normpath("".join(context.ssh_config["identityfile"])))
    if sys.platform == "win32":
        rsync_cmd = r"{}  -pthrvz --chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r --rsh='{} -i {} -p 22 ' {} {}@{}:{}"
        context.local(
            rsync_cmd.format(
                rsync_path,
                ssh_path,
                # context.ssh_config => dict(hostname, port, user, identityfile)
                ssh_identity_file_path,
                convert_to_posix_path(get_base_path()),
                context.ssh_config["user"],
                context.ssh_config["hostname"],
                remote_base_path
            )
        )
    else:
        rsync(
            context,
            convert_to_posix_path(get_base_path()),
            "/home/pcdinh/code",
            rsync_opts='--chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r --verbose'
        )


def start_server(context):
    """Start API Server processes"""
    # ensure that live server is killed first
    command = "python3 /home/pcdinh/code/sv/server.py -e dev1 -w 1 api_server"
    stop_server(context)
    _run_background(
        context,
        "{}".format(command),
        out_file="/home/pcdinh/code/sv/output.log",
        err_file="/home/pcdinh/code/sv/error.log"
    )


def stop_server(context):
    """Stop API Server processes"""
    context.run("kill $(pgrep -f api_server | grep -v ^$$\$)", warn=True)


def _run_background(context, command, out_file="/dev/null", err_file=None, shell="/bin/bash", pty=False):
    # Re: nohup {} >{} 2>{} </dev/null &
    cmd = 'nohup {} >{} 2>{} &'.format(command, out_file, err_file or '&1')
    print("Running: {}".format(cmd))
    context.run(cmd, shell=shell, pty=pty, warn=True)


@task
def deploy(context):
    """Execute deploy task
    Linux: fab2 -ssh-config=~/.ssh/config -H dev0 deploy
    Windows: fab2 --ssh-config=C:/Users/pcdinh/.ssh/config -H dev0 deploy
             ENV_NAME=dev0 fab2 -H dev0 deploy

    ~/.ssh/config

    Host dev0
        HostName 192.168.1.113
        Port 22
        User pcdinh
        IdentityFile ~/.ssh/FileString_Stg.pem
    :param context:
    :return:
    """
    # context.config['env'] = {'PATH': r'D:\cygwin64\bin'}
    context.config.run['replace_env'] = False
    synchronize_code(context)
    start_server(context)


@task
def deploy3(context, host_name):
    """Execute deploy task
    fab2 deploy <env_name>
    fab2 -f ./fabfile.py deploy dev1

    :param context:
    :param host_name:
    :return:
    """
    try:
        print(f"Connecting to {host_name}")
        conn = get_connection(host_name)
    except KeyError:
        print(f"Error: Undefined host name: {host_name}")
        return
    conn.run("uname -s")


@task
def deploy2(context, host_name):
    """Execute deploy task
    Linux: fab2 -i ~/.ssh/id_rsa --ssh-config=~/.ssh/config -H mango deploy2 dev0
    Windows: fab2 -i C:/Users/pcdinh/.ssh/id_rsa --ssh-config=C:/Users/pcdinh/.ssh/config -H mango deploy2 dev0

    ~/.ssh/config

    Host dev0
        HostName 192.168.1.113
        Port 22
        User pcdinh
        IdentityFile ~/.ssh/FileString_Stg.pem
    :param context:
    :param host_name:
    :return:
    """
    try:
        print(f"Connecting to {host_name}")
        conn = Connection(host_name)
    except KeyError:
        print(f"Error: Undefined host name: {host_name}")
        return
    conn.run("uname -s")
