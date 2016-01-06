#!/usr/bin/env python
import sys
import ConfigParser
import subprocess
import os
import logging as log
import argparse
from datetime import datetime
from collections import namedtuple

# structure of task : (task_number, actual_task, try_number, status)
task = namedtuple ('task', 'task_number, command, try_number, status')

# backup_queue_head : (before/after, queue_head_position)
backup_queue_head = namedtuple ('backup_queue_head', 'state_of_queue_head, queue_head_position')

# create logger with 'log_main' application
log_main = log.getLogger('get_logger')
log_main.setLevel(log.DEBUG)

# create file handler which logs even debug messages
fh = log.FileHandler(datetime.now().strftime('get_backup_of_desired_state_machine.log.%d-%m-%Y__%H-%M-%S'))
fh.setLevel(log.DEBUG)

# create formatter and add it to the handlers
formatter = log.Formatter('%(asctime)s-%(levelname)s:%(message)s')
fh.setFormatter(formatter)

# add the handlers to the logger
log_main.addHandler(fh)


def log_info_to_file (string):
    print string
    log_main.info (string)

def is_cycle_present (source, destination):
    if (source == destination):
        return True
    if (len(source) <= len(destination)):
        if (source == destination[:len(source)]):
                return True
    return False

def are_config_values_correct (config_parser, path_to_file_containing_paths_to_backup):

    incorrect_config_flag = True

    # to check config_values values
    try:

        backup_path = config_parser.get ('default_config_values', 'backup_path')
        if not os.path.exists (backup_path):
            incorrect_config_flag = False
            log_main.error ("backup_config:: No such directory: " + backup_path)
            raise IOError

        # if the options are not boolean, ConfigParser will throw exceptions
        boolean_backup_globally_installed_packages = config_parser.getboolean ('default_config_values', 'backup_globally_installed_packages')
        boolean_value = boolean_backup_globally_installed_packages and config_parser.getboolean ('default_config_values', 'backup_locally_installed_packages')
        
        # if the number of retries should be non-negative
        if (config_parser.getint ('default_config_values', 'number_of_retries_to_do_backup_steps_if_failed') < 0):
            incorrect_config_flag = False
            log_main.error ('backup_config: Number of retries should be non-negative.')

        
        # there are extra files to backup
        if (path_to_file_containing_paths_to_backup):
            if not os.path.isfile (path_to_file_containing_paths_to_backup):
                log_main.error (path_to_file_containing_paths_to_backup + ":: not a file or does not exists..!!")
                return False

            if (os.path.exists (path_to_file_containing_paths_to_backup)):
                path_of_paths_file = open (path_to_file_containing_paths_to_backup, "r")
                l_paths = path_of_paths_file.read ().split('\n')
                l_paths.pop()
                path_of_paths_file.close()

                for path in l_paths:
                    #check if all the paths exists or not
                    if not os.path.exists(path):
                        incorrect_config_flag = False
                        log_main.critical (path_to_file_containing_paths_to_backup + ": \"" + path + "\" does not exists..!!")
                        continue

                    # paths mentioned should be absolute paths only
                    if not os.path.isabs(path):
                        incorrect_config_flag = False
                        log_main.critical (path_to_file_containing_paths_to_backup + ": Provide absolute path for \"" + path + "\".")

                    # check for recusive paths if exists
                    if (is_cycle_present (path, os.path.abspath (path_to_file_containing_paths_to_backup))):
                        incorrect_config_flag = False
                        log_main.critical (path_to_file_containing_paths_to_backup + ": Cannot copy  \"" + path + "\" into itself.")
            else:
                raise IOError
    except:
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        return False

    return incorrect_config_flag


def get_command_to_make_tar (backup_path, path):
    dest = os.path.join (backup_path, os.path.basename(path))
    return "tar -PcJf "+ dest + ".xz " + path

def get_input_from_user_for_continuation ():
    log_info_to_file ("\n\n********************\n")
    log_info_to_file ("There is a situation here. You have selected to backup globally installed packages, and it requires internet connection.")
    log_info_to_file ("This machine is not connected to the internet. So select one of the options from below (0/1/2) to continue.")
    log_info_to_file ("`0`: Continue anyway. (default)")
    log_info_to_file ("`1`: Do not backup globally installed packages and continue with the rest.")
    log_info_to_file ("`2`: Abort the program.")
    
    # Note : I know that I am hard coding the below value!!
    n_attempts = 3
    switch = ''
    while (n_attempts):
        switch = raw_input ("Your input: ")
        if (switch == '0' or switch == '' or switch == '1' or switch == '2'):
            break
        else:
            n_attempts -= 1
            log_info_to_file ("Invalid input! Please provide a valid input.")
            log_info_to_file ("Number of tries left: "), n_attempts

    if not n_attempts:
        log_info_to_file ("Number of attempts exhausted.")
        log_info_to_file ("Continuing with the default option: 0")
        switch = '0'
   
    if (switch == ''):
        switch = '0'

    log_info_to_file ("Choice selected: "+ switch)
    return switch

def get_l_tasks_and_task_number_for_globally_installed_packages (l_tasks,
                                                                 task_number,
                                                                 backup_path,
                                                                 try_number,
                                                                 status):
    dpkg_get_selections_dest = os.path.join (backup_path, "dpkg_get_selections")
    get_dpkg_selections = "dpkg --get-selections > " + dpkg_get_selections_dest
    copy_tar_files = "cp /var/cache/apt/archives/*.deb " + backup_path
    generate_urls = "grep -v deinstall " + dpkg_get_selections_dest + " | sed -e \"s/\s\+\(.*\)//g\" | xargs apt-get install --reinstall --print-uris --yes | grep -oP \"\047.*\047\" | sed \"s/'//g\" > urls_of_dpkg_get_selections"
    download_packages = "wget -c --no-clobber --input-file=urls_of_dpkg_get_selections -o wget_log"

    l_tasks.append(task (task_number, get_dpkg_selections, try_number, status))
    task_number += 1

    l_tasks.append(task (task_number, copy_tar_files, try_number, status))
    task_number += 1
    
    l_tasks.append(task (task_number, generate_urls, try_number, status))
    task_number += 1

    l_tasks.append(task (task_number, download_packages, try_number, status))
    task_number += 1

    return l_tasks, task_number


def generate_lists_of_tasks (config_parser, path_to_file_containing_paths_to_backup):

    l_tasks = []
    task_number = 0
    try_number = 0
    status = "undone"
    backup_path = config_parser.get ('default_config_values', 'backup_path')

    if (config_parser.getboolean ('default_config_values', 'backup_globally_installed_packages')):

        # if backup globally installed packages, the system must be connected to internet
        log_info_to_file ("Checking internet connectivity ..")
        
        return_value = subprocess.call ("wget google.co.in --timeout=20 --tries=3 > /dev/null 2> /dev/null", shell = True)
        
        if (return_value != 0):
            log_info_to_file ("No internet connectivity !!")
            choice = get_input_from_user_for_continuation ()
            if (choice == '0'):
                l_tasks, task_number = get_l_tasks_and_task_number_for_globally_installed_packages (l_tasks,
                                                                                                    task_number,
                                                                                                    backup_path,
                                                                                                    try_number,
                                                                                                    status)

            elif (choice == '1'):
                log_info_to_file ("Skipping globally installed packages and continuing with the rest.")
                pass
                
            else:
                log_info_to_file ("Aborting ....")
                sys.exit (1)

        else:
            log_info_to_file ("Internet connectivity: OK")
            l_tasks, task_number = get_l_tasks_and_task_number_for_globally_installed_packages (l_tasks,
                                                                                                task_number,
                                                                                                backup_path,
                                                                                                try_number,
                                                                                                status)

    if (config_parser.getboolean ('default_config_values', 'backup_locally_installed_packages')):
        l_locally_installed_packages = subprocess.check_output ("echo -n $PATH | tr ':' '\n' | sort | uniq | grep $HOME | sed 's/\/install\/bin//g'", shell=True).split('\n')

        # to remove last empty element from the list:
        l_locally_installed_packages.pop()
        
        string_of_all_names_of_locally_installed_packages = "printf \""
        l_names_of_locally_installed_packages = map (os.path.basename, l_locally_installed_packages)
        for name in l_names_of_locally_installed_packages:
            string_of_all_names_of_locally_installed_packages += name + ":"

        # to remove last comma
        string_of_all_names_of_locally_installed_packages = string_of_all_names_of_locally_installed_packages[:-1]

        write_names_of_locally_installed_packages = string_of_all_names_of_locally_installed_packages + "\" > "
        write_names_of_locally_installed_packages += os.path.join (backup_path, "list_of_locally_installed_packages")

        l_tasks.append (task (task_number, write_names_of_locally_installed_packages, try_number, status))
        task_number += 1

        for path in l_locally_installed_packages:
            make_tar = get_command_to_make_tar (backup_path, path)
            l_tasks.append (task (task_number, make_tar, try_number, status))
            task_number += 1

        # backup local bashrc to maintain PATH
        make_tar = get_command_to_make_tar (backup_path, "$HOME/.bashrc")
        l_tasks.append (task (task_number, make_tar, try_number, status))
        task_number += 1


    if (path_to_file_containing_paths_to_backup):
        all_paths_to_backup_file = open (path_to_file_containing_paths_to_backup, "r")
        l_paths = all_paths_to_backup_file.read().split('\n')
        all_paths_to_backup_file.close()

        l_paths.pop()

        for path in l_paths:
            make_tar = get_command_to_make_tar (backup_path, path)
            l_tasks.append (task (task_number, make_tar, try_number, status))
            task_number += 1

    return l_tasks

def are_list_of_tasks_same (l_tasks, l_tasks_read_from_file):
    
    # tasks may be different or perhaps incompletely written
    if (len (l_tasks_read_from_file) < len (l_tasks)):
        return False
    
    for i in range (0, len(l_tasks)):
        if l_tasks[i] != l_tasks_read_from_file[i]:
            return False

    # all tasks are same
    return True

def convert_list_of_csv_tasks_to_namedtuple (l_csv_namedtuple):
    l_tasks_read_from_file = []
    for i in range (0, len(l_csv_namedtuple)):
        l_fields = l_csv_namedtuple[i].split(',')
        l_tasks_read_from_file.append (task (int (l_fields[0]), l_fields[1], int (l_fields[2]), l_fields[3]))
    return l_tasks_read_from_file

def convert_list_of_csv_queue_head_to_namedtuple (l_csv_namedtuple):
    l_queue_head_read_from_file = []
    for i in range (0, len(l_csv_namedtuple)):
        l_fields = l_csv_namedtuple[i].split(',')
        l_queue_head_read_from_file.append (backup_queue_head (l_fields[0], l_fields[1]))
    return l_queue_head_read_from_file

def log_current_queue_head_to_file (current_queue_head):
    string_current_queue_head = str (current_queue_head.state_of_queue_head) + ","  
    string_current_queue_head += str (current_queue_head.queue_head_position) + "\n"

    file_queue_head = open ("backup_queue_head", "a")

    file_queue_head.write (string_current_queue_head)
    file_queue_head.flush()
    
    file_queue_head.close()
    

def log_task_to_file (task_to_write):
    string_task_to_do = str (task_to_write.task_number) + ","
    string_task_to_do += str (task_to_write.command) + ","
    string_task_to_do += str (task_to_write.try_number) + ","
    string_task_to_do += str (task_to_write.status) + "\n"

    file_lists_of_tasks_to_do = open ("backup_task_queue", "a")

    file_lists_of_tasks_to_do.write (string_task_to_do)
    file_lists_of_tasks_to_do.flush()
    
    file_lists_of_tasks_to_do.close()


def log_list_of_tasks_to_file (l_tasks):
    for task_to_write in l_tasks:
        log_task_to_file (task_to_write)

def execute_task (current_queue_head, current_task, index, task_try_number):
    current_queue_head = current_queue_head._replace (state_of_queue_head = "before",
                                                      queue_head_position =  str(index))
    log_current_queue_head_to_file (current_queue_head)

    # execute command
    return_value_of_task = subprocess.call (current_task.command, shell=True)

    # if a command failed, append it to backup_task_queue with status="failed"
    if (return_value_of_task != 0):
        current_task = current_task._replace (status = "failed",
                                              try_number = str (task_try_number+1))
        log_info_to_file ("Failed:: " + current_task.command + ":: Return value = " + str(return_value_of_task))

    else:
        current_task = current_task._replace (status = "success",
                                              try_number = str (task_try_number+1))
        log_info_to_file ("Success:: " + current_task.command)

    current_queue_head = current_queue_head._replace (state_of_queue_head = "after",
                                                      queue_head_position =  str(index))
    log_current_queue_head_to_file (current_queue_head)
    return current_task

def execute_list_of_tasks (l_tasks, current_queue_head, config_parser):
    queue_head_position = int (current_queue_head.queue_head_position)
    state_of_queue_head = current_queue_head.state_of_queue_head
    
    max_number_of_retries_to_backup = config_parser.get ('default_config_values',
                                                         'number_of_retries_to_do_backup_steps_if_failed')

    if (state_of_queue_head == "after"):
        queue_head_position += 1
    
    for i in range (queue_head_position, len(l_tasks)):
        current_task = l_tasks[i]
        task_try_number = l_tasks[i].try_number
        task_status = l_tasks[i].status

        # skip the tasks which are already successfully executed
        # or which were cancelled
        if (task_status == "success" or task_status == "cancelled"):
            current_queue_head = current_queue_head._replace (state_of_queue_head = "after",
                                                              queue_head_position =  str(i-1))
            log_current_queue_head_to_file (current_queue_head)
            continue
        
        elif ((task_status == "failed") and (task_try_number < (max_number_of_retries_to_backup + 1))):
            current_task = execute_task (current_queue_head, l_tasks[i], i, task_try_number)

        # number of retries are exceeded
        elif (task_try_number == max_number_of_retries_to_backup):
                current_task = l_tasks[i]._replace (status = "cancelled")
                log_info_to_file ("Cancelled:: " + l_tasks[i].command + ":: Number of retries exceeded")

        # undone tasks
        else:
            current_task = execute_task (current_queue_head, l_tasks[i], i, task_try_number)

        l_tasks.append (current_task)
        log_task_to_file (current_task)



def get_current_queue_head_in_namedtuple_format_from_file (current_queue_head):
    try:
        file_queue_head = open (current_queue_head, "r")
        l_queue_head_csv = (file_queue_head.read()).split('\n')
        l_queue_head_csv.pop()
        current_queue_head = convert_list_of_csv_queue_head_to_namedtuple (l_queue_head_csv)[-1]
        file_queue_head.close ()
    except:
        log_info_to_file ("Exception occured. Please check the log !")
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        sys.exit (1)

    return current_queue_head


def get_list_of_tasks_in_namedtuple_format_from_file (backup_task_queue):
    try:
        file_lists_of_tasks_to_do = open (backup_task_queue, "r")
        l_tasks_csv = (file_lists_of_tasks_to_do.read()).split('\n')
        l_tasks_csv.pop()
        l_tasks_read_from_file = convert_list_of_csv_tasks_to_namedtuple (l_tasks_csv)
        file_lists_of_tasks_to_do.close()

    except:
        log_info_to_file ("Exception occured. Please check the log !")
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        sys.exit (1)

    return l_tasks_read_from_file

# will be called if --forced_retry argument will be passed with the script            
# Note : This method assumes that 'backup_task_queue' file and 'backup_queue_head' file are
# present after the normal attempt
def try_forcefully_running_the_script ():
    log_info_to_file ("Attempting forcefull retry for failed/cancelled tasks.")
    l_tasks = get_list_of_tasks_in_namedtuple_format_from_file ("backup_task_queue")
    current_queue_head = backup_queue_head ("before", "0")
    queue_head_position = int (current_queue_head.queue_head_position)
    state_of_queue_head = current_queue_head.state_of_queue_head
    
    failed_or_cancelled_tasks = False

    for i in range (0, len(l_tasks)):
        current_task = l_tasks[i]
        task_status = l_tasks[i].status

        # skip the tasks having status as "success" or "undone"
        if (task_status == "success" or task_status == "undone"):
            #current_queue_head = current_queue_head._replace (state_of_queue_head = "after",
                                                              #queue_head_position =  str(i))
            #log_current_queue_head_to_file (current_queue_head)
            continue
        
        # failed or cancelled tasks found..
        failed_or_cancelled_tasks = True
        log_info_to_file ("Found tasks to retry ..!!")
        current_task = execute_task (current_queue_head, current_task, i)

        l_tasks.append (current_task)
        log_task_to_file (current_task)

    return failed_or_cancelled_tasks
    
def main():
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument ("--path_to_file_containing_paths_to_backup=",
                              help="Absolute path containing list of absolute paths to backup",
                              dest="path_to_file_containing_paths_to_backup")

    args_parser.add_argument ("--forced_retry",
                              help="Retry to run the script after number of tries to run a command after failure has been exhausted ",
                              dest="forced_retry",
                              action="store_true")
    config_parser = ConfigParser.RawConfigParser()

    args = args_parser.parse_args()

    # if forced retry
    if (args.forced_retry ):
        if not try_forcefully_running_the_script():
            log_info_to_file ("No failed or cancelled tasks found.")
            log_info_to_file ("Perhaps the script was aborted or it was succesfully terminated!")
            sys.exit (0)

    # check config values
    try:
        log_info_to_file ("Checking if invalid config values are present ...")
        config_parser.read ('backup_config')
        if not are_config_values_correct (config_parser, args.path_to_file_containing_paths_to_backup):
            raise ValueError

    except:
        print >> sys.stderr, "Incorrect config file..!! Please check the log..!!"
        print >> sys.stderr, "Aborting ..."
        sys.exit (1)

    # generate list of tasks
    log_info_to_file ("Config values seems to be correct ...")
    l_tasks = generate_lists_of_tasks (config_parser, args.path_to_file_containing_paths_to_backup)
    if not l_tasks:
        log_info_to_file ("Nothing to backup..\n exiting from the script..")
        sys.exit (1)
    else:
        log_info_to_file ("Config values : OK !!\nGenerated list of tasks to do !")

    try:
        # check for 1st run..
        current_queue_head = backup_queue_head ("before", "0")
        l_tasks_read_from_file = []

        log_info_to_file ("checking for 1st run ..")
        if (os.path.exists ("backup_task_queue")):
            # not 1st run 
            # check if the tasks are same as of now
            log_info_to_file ("Not the 1st run ..!!\nReading tasks from the existing file ..")
            l_tasks_read_from_file = get_list_of_tasks_in_namedtuple_format_from_file ("backup_task_queue")

            if (are_list_of_tasks_same (l_tasks, l_tasks_read_from_file)):
                log_info_to_file ("All tasks from existing file matches with the current tasks to do. ")
                # check for queue head
                if (os.path.exists ("queue_head")):
                    # continue from where it had stopped
                    log_info_to_file ("queue_head file found..!!")
                    current_queue_head = get_current_queue_head_in_namedtuple_format_from_file ("queue_head")

                    state_of_queue_head = current_queue_head.state_of_queue_head
                    index_in_tasks_list = current_queue_head.queue_head_position
                    log_info_to_file ("Resuming tasks from " + state_of_queue_head + " index " + index_in_tasks_list + 
                                      " in task list ..")
                    execute_list_of_tasks (l_tasks_read_from_file, current_queue_head, config_parser)

            else:
                # tasks are different, so update the file with new tasks
                # and start from first task

                log_info_to_file ("All tasks from existing file DOES NOT MATCH with the current tasks to do ..")

                if (os.path.exists ("backup_task_queue")):
                    log_info_to_file ("Deleting existing backup_task_queue file.")
                    subprocess.call ("rm -f backup_task_queue", shell=True)

                if (os.path.exists ("queue_head")):
                    log_info_to_file ("Deleting existing backup_queue_head file.")
                    subprocess.call ("rm -f backup_queue_head", shell=True)
    
                log_list_of_tasks_to_file (l_tasks)

                log_info_to_file ("Created new backup_task_queue file. Starting executions of tasks ..")
                execute_list_of_tasks (l_tasks, current_queue_head, config_parser)
        # if 1st run of the script
        else:
            log_list_of_tasks_to_file (l_tasks)
            log_info_to_file ("Created backup_task_queue file.")

            if (os.path.exists ("queue_head")):
                log_info_to_file ("Deleting existing backup_queue_head file.")
                subprocess.call ("rm -f backup_queue_head", shell=True)

            log_info_to_file ("Starting execution of tasks ..")
            execute_list_of_tasks (l_tasks, current_queue_head, config_parser)

    except:
        exception_value = sys.exc_info()[1]
        log_main.critical (sys.exc_info())
        print >> sys.stderr, "Exception occured.. please check the log file.."
        sys.exit (1)

    log_info_to_file ("Backup successfully done..!! :-)")

main()
