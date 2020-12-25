#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from subprocess import Popen, PIPE
from multiprocessing import Pool


# ===========================================================================================
# FONCTIONS 
# ===========================================================================================

def Upload_script_SSH(Username, Remote_computers):
    """
    If the connection via ssh could be done, the file SLAVE.py is upload in the directory /tmp/"Username"/
    """

    process = Popen(
        f'ssh -o \"StrictHostKeyChecking=no\" {Username}@{Remote_computers} hostname', 
        stdout=PIPE, stderr=PIPE, text=True,
        shell=True
        )
    process.wait()
    timeout = 10

    try:
        
        error = process.communicate(timeout=timeout)[1]

        if error == "":

            # Création du répertoire "/tmp/mnourry" sur l'ordinateur distant
            mkdir_process = Popen(
                f'ssh {Username}@{Remote_computers} mkdir -p /tmp/{Username}', 
                stderr=PIPE, stdout=PIPE,
                shell=True
                )
            mkdir_process.wait()
            mkdir_process.kill()

            # Copie du fichier SLAVE.py dans le répertoire "/tmp/"Username"" sur l'ordinateur distant
            scp_process = Popen(
                f'scp {os.getcwd() + "/SLAVE_v3_1.py"} {Username}@{Remote_computers}:/tmp/{Username}', 
                stderr=PIPE, stdout=PIPE,
                shell=True
                )
            scp_process.wait()
            scp_process.kill()

            process.kill()
            return (Remote_computers, "OK")

        else:

            process.kill()
            return (Remote_computers, "Echec")

    except:

        process.kill()
        return (Remote_computers, "Echec TimeOut")


def Deploy(Username, Remote_computers):

    Pair_username_computer = zip(
        len(Remote_computers) * [Username],
        Remote_computers
    )

    with Pool() as p:
        Log = p.starmap(
                Upload_script_SSH,
                Pair_username_computer
            )

    Available_remote_computers = [i[0] for i in Log if i[1]=='OK' in i]

    # Ecriture du nombre de connexions établies dans le fichier LOG.txt
    with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
        f.write("====================================================\n")
        f.write("Test computers and deployment of SLAVE_v3_1.py\n")
        f.write(f"Successful connections : {len(Available_remote_computers)}\n")
        [f.write(f"| {i}\n") for i in Available_remote_computers]
        f.write(f"Failed connections : {len(Remote_computers)-len(Available_remote_computers)}\n")
        [f.write(f"| {i[0]}\n") for i in Log if i[1]=='Echec' or i[1] == 'Echec TimeOut' in i]
        f.write("====================================================\n")

    # Ecriture du nombre d'ordinateur distants disponibles dans le fichier Available_computers.txt
    with open(os.getcwd() + "/Available_remote_computers.txt", "w") as f:
        for computer in Available_remote_computers:
            f.write(f"{computer}\n")
        f.close()
    
    return Available_remote_computers
