#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from subprocess import Popen, PIPE
from multiprocessing import Pool


def Delete_script_SSH(Username, Remote_computer):
    """
    If the connection via ssh could be done, the file SLAVE.py is removed from the directory /tmp/"Username"
    """

    Test_computer_process = Popen(
        f"ssh -o \"StrictHostKeyChecking=no\" {Username}@{Remote_computer} hostname", 
        stdout=PIPE, stderr=PIPE, text=True,
        shell=True
        )
    timeout=10

    try:

        error = Test_computer_process.communicate(timeout=timeout)[1]

        if error == "":

            Remove_files_process = Popen(
                f'ssh {Username}@{Remote_computer} rm -rf /tmp/{Username}',
                shell=True
                )
            Remove_files_process.wait()
            Remove_files_process.kill()
            
            Test_computer_process.kill()
            return ("OK", f"| Remote computer: {Remote_computer:<15s}  CLEAN: OK")

        else:

            Test_computer_process.kill()
            return ("Error", f"| Remote computer: {Remote_computer:<15s}  CLEAN: Error")
    except:
        
        Test_computer_process.kill()
        return ("Error", f"| Remote computer: {Remote_computer:<15s}  CLEAN: Timeout Error")

def Clean(Username, Remote_computers):

    Pair_username_computer = zip(
        len(Remote_computers) * [Username],
        Remote_computers
    )

    with Pool() as p:
        CLEAN_log = p.starmap(
            Delete_script_SSH,
            Pair_username_computer
            )


    # Ecriture du nettoyage effectué dans le fichier LOG.txt
    with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
        f.write("====================================================\n")
        [f.write(i[1] + "\n") for i in CLEAN_log]

        if "Error" in [i[0] for i in CLEAN_log]:
            f.write("CLEAN operation has failed in part, the MAP/REDUCE can't continue without potentially making big mistakes.\n")
        else:
            f.write("CLEAN operation successfully completed.\n")

        f.write("====================================================\n")

    return [i[0] for i in CLEAN_log]
    
