#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from sys import argv
from subprocess import Popen, PIPE
from multiprocessing import Pool
from re import findall
from time import time
from shutil import rmtree
import pandas as pd

from DEPLOY_v3_1 import Deploy
from CLEAN_v3_1 import Clean


# =============================================================================
# SUPPORT FUNCTIONS
# =============================================================================

def CMD_instruction(command, print_output):
    """
    Launch an instruction in command line.

    Prameters:
    ---------
    
    command (str) : Command instruction
    print_output (bool) : Specify if the function has to return 
                          the output of the command instruction                     
    """

    process = Popen(
        command, 
        stderr=PIPE, stdout=PIPE, text=True,
        shell=True
        )
    process.wait()
    output, error = process.communicate()
    
    # If the argument "print_output" is set to False, 
    # then the function return only a string : "OK" or "Error"
    # to inform about the running process
    if print_output == False:
        process.kill()
        if error == "":   
            return "OK"
        else:
            return "Error"
        
    # If the argument "print_output" is set to True, 
    # then the function only returns a tuple with : 
    # - for 1st element -> a string "OK" or "Error". 
    #                      to inform about the progress of the order
    # - for the 2nd element -> a string corresponding to the result of the command
    else:
        process.kill()
        if error == "":   
            return ("OK", output)
        else:
            return ("Error", None)
    

# =============================================================================
# PROJECT FUNCTIONS
# =============================================================================

def Split_construction(inputfile, number_remote_computers):
    """
    Construction of split files
    """

    # Purge locally existing Split files
    if os.path.exists(os.getcwd() + "/Splits"):
        rmtree(os.getcwd() + "/Splits")

    # Creation of a new blank Split folder
    os.mkdir(os.getcwd() + "/Splits")

    # Splitting the file using the Linux split method 
    # The file is divided into as many parts as there are remote computers available
    # while respecting that each file ends with a line end with the term l/
    split_process = CMD_instruction(
        f'cd {os.getcwd() + "/Splits"} && split {inputfile} Split_ --additional-suffix=.txt -d -n l/{number_remote_computers}',
        print_output=False
    )

    if split_process == "OK":
        Splits_files = os.listdir(os.getcwd() + "/Splits")
        return Splits_files
    else:
        raise Exception("The split stage seems to bee unfeasible.")


def Split_broadcasting(Remote_computer, Split_file):
    """
    Diffusion of a split file in the directory 
    "/tmp/username/splits" on a remote computer
    """

    # Creation of the "/tmp/username/splits" directory on the remote computer.
    mkdir_process = CMD_instruction(
        f'ssh {username}@{Remote_computer} mkdir -p /tmp/{username}/splits',
        print_output=False
        )

    # Copy the split file to the "/tmp/username/splits" folder on the remote computer.
    scp_process = CMD_instruction(
        f'scp {os.getcwd()}/Splits/{Split_file} {username}@{Remote_computer}:/tmp/{username}/splits',
        print_output=False
        )

    # If an error occurred during the splits broadcast phase on 
    # the remote computer, the log line is specified as "Error" 
    # otherwise it is specified as "OK".
    if mkdir_process == "OK" and scp_process == "OK":
        return (
            ("OK", Remote_computer, Split_file),
            f"| Remote computer: {Remote_computer:<15s}  Split file: {Split_file:<17s}  Broadcast: OK"
        )
    else:
        return (
            ("Error", Remote_computer, Split_file),
            f"| Remote computer: {Remote_computer:<15s}  Split file: {Split_file:<17s}  Broadcast: Error"
        )


def MAP(Remote_computer, Split_file):
    """
    MAP phase on a remote computer
    from a split file
    """

    # Launching the python file SLAVE_v3_1.py :
    # in mode -m = 0 to carry out the map phase
    # with the inputfile -i = Split_file.
    MAP_process = CMD_instruction(
        f'ssh {username}@{Remote_computer} /tmp/{username}/SLAVE_v3_1.py -m 0 -u {username} -i /tmp/{username}/splits/{Split_file}',
        print_output=False
    )

    # If an error occurred during the MAP phase
    # on the remote computer, the log line is specified
    # in "Error" otherwise it is specified "OK".
    if MAP_process == "OK":
        return (
            ("OK", Remote_computer, Split_file),
            f"| Remote computer: {Remote_computer:<15s}  MAP: OK"
        )
    else:
        return (
            ("Error", Remote_computer, Split_file),
            f"| Remote computer: {Remote_computer:<15s}  MAP: Error"
        )


def rcomputer_list_broadcasting(Remote_computer):
    """
    Deployment phase of the machines.txt 
    file on a remote computer
    """

    # Broadcasting the list of computers 
    # in the cluster to all the computers in the cluster
    list_broadcasting_process = CMD_instruction(
        f'scp {os.getcwd()}/Available_remote_computers.txt {username}@{Remote_computer}:/tmp/{username}',
        print_output=False
    )

    # If an error occurred during the list diffusion phase 
    # from the cluster computers to the remote computer, the log line 
    # is specified as "Error" otherwise it is specified as "OK".
    if list_broadcasting_process == "OK":
        return f"| Remote computer: {Remote_computer:<15s}  Broadcast: OK"
    else:
        return f"| Remote computer: {Remote_computer:<15s}  Broadcast: Error"


def SHUFFLE(Remote_computer, Split_file):
    """
    SHUFFLE phase
    """

    Start = time()

    # Construction of the UM file name
    # corresponding to the Split file 
    # present on the remote computer
    number_split_file = findall(r"\d+", Split_file)[0]
    UM_file = f'UM_{number_split_file}.txt'

    # Launching the python file SLAVE_v3_1.py :
    # in mode -m = 1 to perform the shuffle phase
    # with the inputfile -i = Unsorted_map_file
    SHUFFLE_process = CMD_instruction(
        f'ssh {username}@{Remote_computer} /tmp/{username}/SLAVE_v3_1.py -m 1 -u {username} -i /tmp/{username}/maps/{UM_file}',
        print_output=True
    )

    End = time() - Start

    # If an error has occurred during the 
    # SHUFFLE on the remote computer, the log line 
    # is specified as "Error" otherwise it is specified as "OK".
    if SHUFFLE_process[0] == "OK" and "Error" in SHUFFLE_process[1]:
        return (
            ("Partially", Remote_computer),
            f"| Remote computer: {Remote_computer:<15s}  Time: {str(round(End,2)) + 's':<10s}  SHUFFLE: Partially (sending of few files failed)"
        )
    elif SHUFFLE_process[0] == "OK" and "Error" not in SHUFFLE_process[1]:
        return (
            ("OK", Remote_computer),
            f"| Remote computer: {Remote_computer:<15s}  Time: {str(round(End,2)) + 's':<10s}  SHUFFLE: OK"
        ) 
    else:
        return (
            ("Error", Remote_computer),
            f"| Remote computer: {Remote_computer:<15s}  SHUFFLE: Error"
        )


def Count_SM_files(Log_Shuffle_sending, Remote_computer):
    """
    In addition to the SHUFFLE phase, the number of Sorted Map files 
    (in the shufflereceived directory) present on the remote computer 
    for the DAG visualisation is counted.
    """

    CountSM_process = CMD_instruction(
        f'ssh {username}@{Remote_computer} /tmp/{username}/SLAVE_v3_1.py -m 2 -u {username} -i None',
        print_output=True
    )

    if CountSM_process[0] == "OK" and Log_Shuffle_sending == "OK":
        return ("OK", Remote_computer, CountSM_process[1].replace("\n",""))
    elif CountSM_process[0] == "OK" and Log_Shuffle_sending == "Partially":
        return ("Partially", Remote_computer, CountSM_process[1].replace("\n",""))
    else:
        return ("Error", Remote_computer, None)


def REDUCE(Log_count_SM_files, Remote_computer, Number_SM_files):
    """
    REDUCE phase
    """

    if Number_SM_files == "0":

        REDUCE_process = "OK"

    else:

        # Launching the python file _v3_1.py :
        # in mode -m = 2 to perform the reduce phase
        # with input file -i = None (No input file required)
        REDUCE_process = CMD_instruction(
            f'ssh {username}@{Remote_computer} /tmp/{username}/SLAVE_v3_1.py -m 3 -u {username} -i None',
            print_output=False
        )

        CMD_instruction(
            f'scp {username}@{Remote_computer}:/tmp/{username}/reduce/reduce_{Remote_computer}.txt {os.getcwd()}/Output/Reduces',
            print_output=False
        )

    # If an error has occurred during the 
    # REDUCE on the remote computer, the log line 
    # is specified as "Error" otherwise it is specified as "OK".
    if REDUCE_process == "OK" and Log_count_SM_files == "OK":
        return (
            ("OK", Remote_computer, Number_SM_files),
            f"| Remote computer: {Remote_computer:<15s}  REDUCE: OK"
            )
    elif REDUCE_process == "OK" and Log_count_SM_files == "Partially":
        return (
            ("Partially", Remote_computer, Number_SM_files),
            f"| Remote computer: {Remote_computer:<15s}  REDUCE: OK"
            )
    else:
        return (
            ("Error", Remote_computer, Number_SM_files),
            f"| Remote computer: {Remote_computer:<15s}  REDUCE: Error"
            )


def DAG_construction(DAG_file, Node_iterator, Source, Target, Label, x_position, Time, Output_filename=None):
    """
    DAG completion
    """

    # Time part
    Source_time_node_id = f"{Source}_time"
    Target_time_node_id = f"{Target}_time"
    Target_time_node_label = Target
    Timeline_weight = f"{Target} - {Time:.2f}s"

    DAG_file.write(
        "\n".join([
            f'{{"data": {{"id": "{Target_time_node_id}", "label": "{Target_time_node_label}"}}, "classes": "Time_node", "position": {{"x": {x_position}, "y": 25}}}}',
            f'{{"data": {{"source": "{Source_time_node_id}", "target": "{Target_time_node_id}", "weight":"{Timeline_weight}"}}, "classes": "Time_line"}}\n'
        ])
    )

    # DAG part       
    if Source == "Input" and Target == "MAP":

        DAG_file.write(f'{{"data": {{"id": "{Label}_files"}}}}\n')

        for index, value in enumerate(Node_iterator):

            Target_DAG_node1_id = f"{value[1]}_{Target}"
            Parent_target_DAG_node_id = f"{Label}_files"
            Source_DAG_node_id = Source
            Target_DAG_node2_id = f"{value[1]}_{Target}"

            if value[0] == "OK":
                Target_DAG_node1_label = f"{value[1]} - {value[2][:-4]}"
                DAG_file.write(f'{{"data": {{"id": "{Target_DAG_node1_id}", "label": "{Target_DAG_node1_label}", "parent": "{Parent_target_DAG_node_id}"}}, "classes": "OK_DAG_node", "position": {{"x": {x_position}, "y": {125+index*50}}}}}\n')
                DAG_file.write(f'{{"data": {{"source": "{Source_DAG_node_id}", "target": "{Target_DAG_node2_id}"}}, "classes": "DAG_line"}}\n')

            else:
                Target_DAG_node1_label = value[1]
                DAG_file.write(f'{{"data": {{"id": "{Target_DAG_node1_id}", "label": "{Target_DAG_node1_label}", "parent": "{Parent_target_DAG_node_id}"}}, "classes": "Error_DAG_node", "position": {{"x": {x_position}, "y": {125+index*50}}}}}\n')

    elif Source == "MAP" and Target == "SHUFFLE":

        if "Partially" in [i[0] for i in Node_iterator]:
            DAG_file.write(f'{{"data": {{"id": "{Label}_files"}}, "classes":"Warning_DAG_node"}}\n')
        else:
            DAG_file.write(f'{{"data": {{"id": "{Label}_files"}}}}\n')

        for index, value in enumerate(Node_iterator):

            Target_DAG_node1_id = f"{value[1]}_{Target}"
            Parent_target_DAG_node_id = f"{Label}_files"
            Source_DAG_node_id = f"{value[1]}_{Source}"
            Target_DAG_node2_id = f"{Label}_files"

            if value[0] in ("OK", "Partially") and value[2] != "0":
                Target_DAG_node1_label = f"{value[1]} - {value[2]} {Label}"
                DAG_file.write(f'{{"data": {{"id": "{Target_DAG_node1_id}", "label": "{Target_DAG_node1_label}", "parent": "{Parent_target_DAG_node_id}"}}, "classes": "OK_DAG_node", "position": {{"x": {x_position}, "y": {125+index*50}}}}}\n')
                DAG_file.write(f'{{"data": {{"source": "{Source_DAG_node_id}", "target": "{Target_DAG_node2_id}"}}, "classes": "DAG_line"}}\n')

            elif value[0] in ("OK", "Partially") and value[2] == "0":
                Target_DAG_node1_label = f"{value[1]} - {value[2]} {Label}"
                DAG_file.write(f'{{"data": {{"id": "{Target_DAG_node1_id}", "label": "{Target_DAG_node1_label}", "parent": "{Parent_target_DAG_node_id}"}}, "classes": "Neutral_DAG_node", "position": {{"x": {x_position}, "y": {125+index*50}}}}}\n')
                DAG_file.write(f'{{"data": {{"source": "{Source_DAG_node_id}", "target": "{Target_DAG_node2_id}"}}, "classes": "DAG_line"}}\n')

            else:
                Target_DAG_node1_label = value[1]
                DAG_file.write(f'{{"data": {{"id": "{Target_DAG_node1_id}", "label": "{Target_DAG_node1_label}", "parent": "{Parent_target_DAG_node_id}"}}, "classes": "Error_DAG_node", "position": {{"x": {x_position}, "y": {125+index*50}}}}}\n')


    elif Source == "SHUFFLE" and Target == "REDUCE":

        if len([value[0] for value in Node_iterator if value[0] == "OK"]) == len(Node_iterator):
            DAG_file.write(f'{{"data": {{"id": "{Label}", "label": "{Output_filename}"}}, "classes": "OK_DAG_node", "position": {{"x": {x_position}, "y": 125}}}}\n')

        elif len([value[0] for value in Node_iterator if value[0] == "Partially"]) > 0 :
            DAG_file.write(f'{{"data": {{"id": "{Label}", "label": "{Output_filename}"}}, "classes": "Warning_DAG_node", "position": {{"x": {x_position}, "y": 125}}}}\n')

        else:
            DAG_file.write(f'{{"data": {{"id": "{Label}", "label": "{Output_filename}"}}, "classes": "Error_DAG_node", "position": {{"x": {x_position}, "y": 125}}}}\n')


        for index, value in enumerate(Node_iterator):

            Source_DAG_node_id = f"{value[1]}_{Source}"
            Target_DAG_node2_id = Label

            if value[0] in ("OK", "Partially") and value[2] != "0":
                DAG_file.write(f'{{"data": {{"source": "{Source_DAG_node_id}", "target": "{Target_DAG_node2_id}"}}, "classes": "DAG_line"}}\n')

            else:
                pass


# =============================================================================
# BODY OF THE PROGRAMM
# =============================================================================


# ------------------------------------------------
# ----     Recovery of launch variables       ----
# ------------------------------------------------

opts = [opt for opt in argv[1:] if opt.startswith("-")]
args = [arg for arg in argv[1:] if not arg.startswith("-")] 

remote_computers = open(args[opts.index("-rc")]).read().split()
username = args[opts.index("-u")]
inputfile = args[opts.index("-i")]


# ------------------------------------------------
# ----        DAG et LOG initialisation       ----
# ------------------------------------------------

with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
    f.write("\n****************** START WORCOUNT ******************\n\n")

with open(os.getcwd() + "/Output/DAG.txt", "a") as f:
    f.write('{"data": {"id": "Input_time", "label": "Input"}, "classes": "Time_node", "position": {"x": 150, "y": 25}}\n')
    f.write(f'{{"data": {{"id": "Input", "label": "{min(os.path.basename(inputfile)[:30] + "...", os.path.basename(inputfile), key=len)}"}}, "classes": "OK_DAG_node", "position": {{"x": 150, "y": 125}}}}\n')
    f.close()


# ------------------------------------------------
# ----          Cleaning phase of the         ----
# ----         /tmp/username directory        ----
# ----           of remote computers          ----
# ------------------------------------------------

CLEAN_log = Clean(username, remote_computers)

if "Error" in CLEAN_log:
    raise Exception("Clean operation has failed partially")


# ------------------------------------------------
# ----  Deployment phase of the SLAVE_v3_1.py ----
# ----       script on remote computers       ----
# ------------------------------------------------

Start_DEPLOY = time()

available_remote_computers = Deploy(username, remote_computers)

End_DEPLOY = time() - Start_DEPLOY


# ------------------------------------------------
# ----   Split file creation and deployment   ----
# ----      phase on remote computers         ----
# ------------------------------------------------

Start_MAP = time()

# If no computer is available, MAP/REDUCE 
# is not possible and ends immediately.
if len(available_remote_computers) == 0:

    raise Exception("No available remote computer, the process can't continue.")

else:

    # Creation of split files
    # If the split phase presents a problem, 
    # MAP/REDUCE is stopped immediately.
    Splits = Split_construction(
        inputfile, len(available_remote_computers)
        )

Pair_machine_split = list(zip(available_remote_computers, Splits))

# Parallelisation of the splits diffusion phase 
# on each remote computer
with Pool() as p:
    Split_log = p.starmap(
        Split_broadcasting,
        Pair_machine_split
    )


# ------------------------------------------------
# ----      MAP phase on remote computers     ----
# ------------------------------------------------

with Pool() as p:
    MAP_log = p.starmap(
        MAP,
        Pair_machine_split
    )

End_MAP = time() - Start_MAP

# Completion of the LOG file
with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
    f.write("====================================================\n")
    f.write("MAP phase\n")
    [f.write(i[1] + "\n") for i in MAP_log]
    f.write(f"=> MAP FINISHED realised in {End_MAP:.2f}s\n")
    f.write("====================================================\n")
    f.close()

# Completion of DAG Input_file -> UM_files
DAG_construction(
    DAG_file = open(os.getcwd() + "/Output/DAG.txt", "a"),
    Node_iterator = [i[0] for i in MAP_log], 
    Source = "Input",
    Target = "MAP",
    Label = "UM",
    x_position = 400,
    Time = End_MAP
)


# ------------------------------------------------
# ----         Deployment phase of the        ----
# ----       Available_computers.txt file     ----
# ----           on remote computers          ----
# ------------------------------------------------

Start_BROADCAST_COMPUTERS = time()

with Pool() as p:
    Spread_info_computers_log = p.map(
        rcomputer_list_broadcasting,
        available_remote_computers
    )

END_BROADCAST_COMPUTERS = time() - Start_BROADCAST_COMPUTERS

# Completion of the LOG file
with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
    f.write("====================================================\n")
    f.write("Broadcast 'Available_remote_computers.txt' on remote computer\n")
    [f.write(i + "\n") for i in Spread_info_computers_log]
    f.write(f"=> BROADCAST REMOTE COMPUTERS LIST realised in {END_BROADCAST_COMPUTERS:.2f}s\n")
    f.write("====================================================\n")
    f.close()


# ------------------------------------------------
# ----    SHUFFLE phase on remote computers   ----
# ------------------------------------------------

Start_SHUFFLE = time()

with Pool() as p:
    SHUFFLE_log = p.starmap(
        SHUFFLE,
        Pair_machine_split
    )

with Pool() as p:
    pair_computer_SM_files = p.starmap(
        Count_SM_files,
        [i[0] for i in SHUFFLE_log]
    )

End_SHUFFLE = time() - Start_SHUFFLE

# Completion of the LOG file
with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
    f.write("====================================================\n")
    f.write("SHUFFLE phase\n")
    [f.write(i[1] + "\n") for i in SHUFFLE_log]
    f.write(f"=> SHUFFLE FINISHED realised in {End_SHUFFLE:.2f}s\n")
    f.write("====================================================\n")
    f.close()

# Completion of DAG UM_files -> SM_files
DAG_construction(
    DAG_file = open(os.getcwd() + "/Output/DAG.txt", "a"),
    Node_iterator = pair_computer_SM_files,
    Source = "MAP",
    Target = "SHUFFLE",
    Label = "SM",
    x_position = 650,
    Time = End_SHUFFLE
)


# ------------------------------------------------
# ----    REDUCE phase on remote computers    ----
# ------------------------------------------------

Start_REDUCE = time()

with Pool() as p:
    REDUCE_log = p.starmap(
        REDUCE,
        pair_computer_SM_files
    )

# Aggregate the REDUCE files from each computer 
# in the cluster to form the final Wordcount.
list_files = os.listdir(f"{os.getcwd()}/Output/Reduces")
for file in list_files:
    if file == list_files[0]:  
        Reduce_count = pd.read_csv(
            f"{os.getcwd()}/Output/Reduces/{file}", 
            sep="\t", header=None, quoting=3
            )
    else:
        words_count = pd.read_csv(
            f"{os.getcwd()}/Output/Reduces/{file}", 
            sep="\t", header=None, quoting=3
            )
        Reduce_count = pd.concat(
            [Reduce_count, words_count], 
            axis="rows"
            )
Reduce_count[1] = Reduce_count[1].astype(int)

End_REDUCE = time() - Start_REDUCE

# Completion of the LOG file
with open(os.getcwd() + "/Output/LOG.txt", "a") as f:
    f.write("====================================================\n")
    f.write("REDUCE phase\n")
    [f.write(i[1] + "\n") for i in REDUCE_log]
    f.write(f"=> REDUCE FINISHED realised in {End_REDUCE:.2f}s\n")
    f.write("====================================================\n")
    f.write("\n*************** WORDCOUNT TERMINATE ****************\n")
    f.close()

# Completion of DAG SM_files -> RM_files
DAG_construction(
    DAG_file = open(os.getcwd() + "/Output/DAG.txt", "a"),
    Node_iterator = [i[0] for i in REDUCE_log],
    Source = "SHUFFLE",
    Target = "REDUCE",
    Label = "Output",
    x_position = 900,
    Time = End_REDUCE,
    Output_filename = f'Wordcount {min(os.path.basename(inputfile)[:20] + "...", os.path.basename(inputfile), key=len)}'
) 


# ------------------------------------------------
# ----   Conversion of the character string   ----
# ----    "Final_results" then sorting of     ----
# ----   the words according to occurrence.   ----
# ------------------------------------------------

Start_SORT = time()

Reduce_count.sort_values(by=1, ascending=False, inplace=True)

End_SORT = time() - Start_SORT


# ------------------------------------------------
# ----      Writing the final results on      ---- 
# ----          the manager computer          ----
# ------------------------------------------------

Global_time = End_DEPLOY + End_MAP + END_BROADCAST_COMPUTERS + End_SHUFFLE + End_REDUCE + End_SORT

with open(os.getcwd() + "/Output/LOG.txt", "a") as f:

    # Write down the processing time of the analysed file
    f.write(f"\n=> {inputfile}\n")
    f.write("--- Version : v3.1\n")
    f.write(f"--- Cluster size: {len(remote_computers)}\n")
    f.write(f"--- Total time: {Global_time:.2f}s\n")
    f.write(f"--- | DEPLOY : {End_DEPLOY:.2f}s\n")
    f.write(f"--- | MAP : {End_MAP:.2f}s\n")
    f.write(f"--- | BROADCAST COMPUTERS LIST : {END_BROADCAST_COMPUTERS:.2f}s\n")
    f.write(f"--- | SHUFFLE : {End_SHUFFLE:.2f}s\n")
    f.write(f"--- | REDUCE : {End_REDUCE:.2f}s\n")
    f.write(f"--- | SORT : {End_SORT:.2f}s\n")
    f.write("--- The 5 words with the most occurrences in the text:\n")    
    f.close

# Ecriture des 5 premières valeurs
Reduce_count.head(5).to_csv(
    os.getcwd() +"/Output/LOG.txt",
    mode="a", index=False, sep="\t", header=None
)
