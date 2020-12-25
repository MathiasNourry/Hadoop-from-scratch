#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sys import argv
from os import path, mkdir, listdir
from re import findall
from socket import gethostname
from subprocess import Popen, PIPE
from hashlib import sha256
from multiprocessing import Pool
import pandas as pd
import numpy as np


# ===========================================================================================
# FONCTIONS
# ===========================================================================================

def Hash_function(word):
    """
    Fonction de hashage
    """

    hash_score = int(sha256(word.encode("utf-8")).hexdigest(), 16) % 10**8

    return hash_score


def Create_SHUFFLE(word):

    # Hostname de l'ordinateur sur lequel est exécuté 
    # le script SLAVE.py
    local_computer = gethostname()

    # Hostname de l'ordinateur receveur
    receiver_computer = computers_list[Hash_function(word)%len(computers_list)]

    # Définition du nom du fichier de shuffle
    # Nom_ordinateur_emetteur_Nom_ordinateur_receveur.txt
    filename_SHUFFLE = f"{local_computer}_{receiver_computer}.txt"

    with open(f'/tmp/{username}/shuffles/{filename_SHUFFLE}', "a") as f:
        f.write(f"{word}\t1\n")
        f.close()


def Send_SHUFFLE(shuffle_file):

    receiver_computer = shuffle_file[shuffle_file.index("_")+1:-4]

    # -------------------------------------------------------
    # --    Envoi des fichiers shuffles sur l'ordinateur   --
    # --      distant centralisant les iso-clés selon      --
    # --              la fonction de hashage               --
    # -------------------------------------------------------

    # Création du répertoire sufflesreceived 
    mkdir_shufflesreceived_process = Popen(
        f"ssh {username}@{receiver_computer} mkdir -p /tmp/{username}/shufflesreceived",
        stderr=PIPE, stdout=PIPE,
        shell=True
    )
    mkdir_shufflesreceived_process.wait()
    mkdir_shufflesreceived_process.kill()

    # Envoi du fichier shuffle vers l'ordinateur
    # distant centralisant les iso-clés
    broadcast_shuffle_files_process = Popen(
        f"scp /tmp/{username}/shuffles/{shuffle_file} {username}@{receiver_computer}:/tmp/{username}/shufflesreceived",
        stderr=PIPE, stdout=PIPE, text=True,
        shell=True
    )

    error = broadcast_shuffle_files_process.communicate()[1]
    if error != "":
        return "Error"
    else:
        return "OK"

    broadcast_shuffle_files_process.wait()
    broadcast_shuffle_files_process.kill()


# ===========================================================================================
# CORPS DU PROGRAMME 
# ===========================================================================================

opts = [opt for opt in argv[1:] if opt.startswith("-")]
args = [arg for arg in argv[1:] if not arg.startswith("-")]

if "-m" and "-u" and "-i" in opts:

    mode = int(args[opts.index("-m")])
    username = args[opts.index("-u")]
    inputfile = args[opts.index("-i")]

    # ==================================================
    # ====              Phase de MAP                ====
    # ====       sur les ordinateurs distants       ====
    # ==================================================
    if mode == 0:      

        numéro_split = findall(r"\d+", inputfile)[0]
        words = open(inputfile).read().split()

        # Création du répertoire maps
        if not path.exists(f"/tmp/{username}/maps"):
            mkdir(f"/tmp/{username}/maps")

        # Création du fichier UMx.txt 
        # correspondant au wordcount pour le split Sx
        with open(f"/tmp/{username}/maps/UM_{numéro_split}.txt", "w") as f:
            f.write("\n".join(words))
            f.close()


    # ==================================================
    # ====            Phase de SHUFFLE              ====
    # ====       sur les ordinateurs distants       ====
    # ==================================================
    elif mode == 1:
        
        computers_list = open(f"/tmp/{username}/Available_remote_computers.txt").read().split()
        words = open(inputfile).read().split("\n")

        # Création du répertoire shuffles
        if not path.exists(f"/tmp/{username}/shuffles"):
            mkdir(f"/tmp/{username}/shuffles")

        # Creation des fichiers dans le shuffles
        with Pool() as p:
            Create_shuffles_process = p.map(
                Create_SHUFFLE,
                words
            )

        # Récupération de la liste des fichiers shuffles créés
        list_shufflefiles = listdir(f'/tmp/{username}/shuffles')
            
        # Envoi de chaque fichier à l'ordinateur 
        # distant attribué à l'aide du score de hashage
        with Pool() as p:
            Send_shufflefiles_process = p.map(
                Send_SHUFFLE,
                list_shufflefiles
            )

        print(Send_shufflefiles_process)


    elif mode == 2:

        # Si l'ordinateur distant ne possède pas de fichier 
        # de shuffle, il n'a pas de phase de reduce à effectuer
        if not path.exists(f"/tmp/{username}/shufflesreceived"):
            number_SM_files = 0

        else:
            number_SM_files = len(listdir(f"/tmp/{username}/shufflesreceived"))

        print(number_SM_files)


    # ==================================================
    # ====             Phase de REDUCE              ====
    # ====       sur les ordinateurs distants       ====
    # ==================================================
    elif mode == 3:

        # Si l'ordinateur distant ne possède pas de fichier 
        # de shuffle, il n'a pas de phase de reduce à effectuer
        if not path.exists(f"/tmp/{username}/shufflesreceived"):
            pass

        else:

            # -------------------------------------------------------
            # --              Préparation du REDUCE                --
            # --     Création des fichiers reduces comptant le     --
            # --      nombre d'occurence de chaque mot parmis      --
            # --            les fichiers shuffles reçus            --
            # -------------------------------------------------------
                    
            # Création du répertoire reduces
            if not path.exists(f"/tmp/{username}/reduce"):
                mkdir(f"/tmp/{username}/reduce")

            list_files = listdir(f"/tmp/{username}/shufflesreceived")

            for file in list_files:

                if file == list_files[0]:  
                    Reduce_count = pd.read_csv(
                        f"/tmp/{username}/shufflesreceived/{file}", 
                        sep="\t", header=None, quoting=3
                        ).rename(
                            columns={0:"words", 1:"count"}
                            )
                else:
                    words_count = pd.read_csv(
                        f"/tmp/{username}/shufflesreceived/{file}", 
                        sep="\t", header=None, quoting=3
                        ).rename(
                            columns={0:"words", 1:"count"}
                            )
                    Reduce_count = pd.concat([Reduce_count, words_count], axis="rows")

            # Comptage de l'occurence de chaque mot
            Reduce_count = Reduce_count.groupby("words")["count"].sum().reset_index()

            # -------------------------------------------------------
            # --        Concaténation des fichiers REDUCE          --
            # --          et envoi à l'ordinateur MASTER           --
            # -------------------------------------------------------

            # Hostname de l'ordinateur sur lequel est exécuté 
            # le script SLAVE.py
            local_computer = gethostname()

            Reduce_count.to_csv(
                f"/tmp/{username}/reduce/reduce_{local_computer}.txt",
                mode="w", index=False, sep="\t", header=None
                )


else:

    print("Syntax needed : SLAVE.py -m <mode> -u <username> -i <inputfile>")

