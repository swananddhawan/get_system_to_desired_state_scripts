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

# queue_head : (before/after, queue_head_position)
queue_head = namedtuple ('queue_head', 'state_of_queue_head, queue_head_position')

# create logger with 'log_main' application
log_main = log.getLogger('get_logger')
log_main.setLevel(log.DEBUG)

# create file handler which logs even debug messages
fh = log.FileHandler(datetime.now().strftime('restore_system_to_desired_state.log.%d-%m-%Y__%H-%M-%S'))
fh.setLevel(log.DEBUG)

# create formatter and add it to the handlers
formatter = log.Formatter('%(asctime)s-%(levelname)s:%(message)s')
fh.setFormatter(formatter)

# add the handlers to the logger
log_main.addHandler(fh)


def log_info_to_file (string):
    print string
    log_main.info (string)

def get_list_locally_installed_package_file_name_from_file (backup_path, file_name):
    try:
        file_handle = open (file_name, "r")
        l_names = file_handle.read().split(":")
        file_handle.close()
        l_paths = []
        for name in l_names:
            l_paths.append (os.path.join (backup_path, name))
    except:
        log_info_to_file ("Exception occured. Please check the log !")
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        sys.exit (1)

    return l_paths

def get_list_of_paths_to_extract_from_file (file_name, backup_path):
    try:
        file_handle = open (file_name, "r")
        l_temp_paths = file_handle.read ().split('\n')
        l_temp_paths.pop()
        path_of_paths_file.close()

        # since the files will be stored with the basename only
        l_names_of_tar_files_to_restore = map (os.path.basename, l_temp_paths)

        l_paths = []
        for name in l_names_of_tar_files_to_restore:
            l_paths.append (os.path.join (backup_path, name) + ".tar.gz")
    except:
        log_info_to_file ("Exception occured. Please check the log !")
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        sys.exit (1)

    return l_paths

def are_config_values_correct (config_parser, path_to_file_containing_paths_to_restore):

    incorrect_config_flag = True

    # to check config_values values
    try:

        backup_path = config_parser.get ('default_config_values', 'backup_path')
        if not os.path.exists (backup_path):
            incorrect_config_flag = False
            log_main.error ("restore_config:: No such directory: " + backup_path)
            raise IOError

        # if the options are not boolean, ConfigParser will throw exceptions
        bool_globally_installed_packages = config_parser.getboolean ('default_config_values', 'restore_globally_installed_packages')
        bool_locally_installed_packages = config_parser.getboolean ('default_config_values', 'restore_locally_installed_packages')

        if bool_globally_installed_packages:
            dpkg_selections_path = os.path.join (backup_path, "dpkg_set_selections")
            if not os.path.isfile (dpkg_selections_path):
                log_main.error (dpkg_selections_path + " does not exists!")
                raise IOError
        
        if bool_locally_installed_packages:
            list_of_locally_installed_packages_path = os.path.join (backup_path, "list_of_locally_installed_packages")
            if os.path.isfile (list_of_locally_installed_packages_path):
                l_paths = get_list_locally_installed_package_file_name_from_file (backup_path, "list_of_locally_installed_packages")
                for path in l_paths:
                    if not os.path.isfile (path):
                        log_main.error (path + " does not exists to install local package: " + os.path.basename (path))
                        raise IOError
            else:
                log_main.error (list_of_locally_installed_packages_path + " does not exists!")
                raise IOError

        
        # the number of retries should be non-negative integer
        if (config_parser.getint ('default_config_values', 'number_of_retries_to_do_restore_steps_if_failed') < 0):
            incorrect_config_flag = False
            log_main.error ('restore_config: Number of retries should be non-negative.')

        
        # there are extra files to restore
        if (path_to_file_containing_paths_to_restore):
            if not os.path.isfile (path_to_file_containing_paths_to_restore):
                log_main.error (path_to_file_containing_paths_to_restore + ":: not a file or does not exists..!!")
                raise IOError

            l_paths = get_list_of_paths_to_extract_from_file (path_to_file_containing_paths_to_restore, backup_path)
            for path in l_paths:
                if not os.path.isfile (path):
                    incorrect_config_flag = False
                    log_main.critical (path_to_file_containing_paths_to_restore + ": \"" + path + "\" does not exists..!!")
            
    except:
        exception_value = sys.exc_info()[1]
        log_main.critical (exception_value)
        return False

    return incorrect_config_flag


def get_command_to_extract_tar (path):
    return "sudo tar -xpzPf " + path 

def get_l_tasks_and_task_number_to_restore_globally_installed_packages (l_tasks,
                                                                 task_number,
                                                                 backup_path,
                                                                 try_number,
                                                                 status):
    
    copy_deb_files_to_cache = "sudo cp " + backup_path + "/*.deb /var/cache/apt/archives"
    clear_dpkg_selections = "sudo dpkg --clear-selections"

    dpkg_selections_path = os.path.join (backup_path, "dpkg_set_selections")
    dpkg_set_selections = "sudo dpkg --set-selections < " + dpkg_selections_path

    remove_stale_packages = "sudo apt-get autoremove -y"

    dselect_upgrade = "sudo apt-get dselect-upgrade -y"

    l_tasks.append(task (task_number, remove_stale_packages, try_number, status))
    task_number += 1

    l_tasks.append(task (task_number, copy_deb_files_to_cache, try_number, status))
    task_number += 1

    l_tasks.append(task (task_number, clear_dpkg_selections, try_number, status))
    task_number += 1
    
    l_tasks.append(task (task_number, dpkg_get_selections, try_number, status))
    task_number += 1

    l_tasks.append(task (task_number, dselect_upgrade, try_number, status))
    task_number += 1

    return l_tasks, task_number


def generate_lists_of_tasks (config_parser, path_to_file_containing_paths_to_restore):

    l_tasks = []
    task_number = 0
    try_number = 0
    status = "undone"
    backup_path = config_parser.get ('default_config_values', 'backup_path')

    if (config_parser.getboolean ('default_config_values', 'restore_globally_installed_packages')):

        l_tasks, task_number = get_l_tasks_and_task_number_to_restore_globally_installed_packages (l_tasks,
                                                                                                   task_number,
                                                                                                   backup_path,
                                                                                                   try_number,
                                                                                                   status)

    if (config_parser.getboolean ('default_config_values', 'restore_locally_installed_packages')):

        l_paths = get_list_locally_installed_package_file_name_from_file (backup_path, "list_of_locally_installed_packages")
        for path in l_paths:
            make_tar = get_command_to_extract_tar (path)
            l_tasks.append (task (task_number, make_tar, try_number, status))
            task_number += 1

        # restore local bashrc to maintain PATH
        path_of_bashrc_tar = os.path.join (backup_path, ".bashrc.tar.gz")
        make_tar = get_command_to_extract_tar (path_of_bashrc_tar)
        l_tasks.append (task (task_number, make_tar, try_number, status))
        task_number += 1


    if (path_to_file_containing_paths_to_restore):
        l_paths = get_list_of_paths_to_extract_from_file (path_to_file_containing_paths_to_restore, backup_path)
        for path in l_paths:
            make_tar = get_command_to_extract_tar (path)
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
        l_queue_head_read_from_file.append (queue_head (l_fields[0], l_fields[1]))
    return l_queue_head_read_from_file

def log_current_queue_head_to_file (current_queue_head):
    string_current_queue_head = str (current_queue_head.state_of_queue_head) + ","  
    string_current_queue_head += str (current_queue_head.queue_head_position) + "\n"

    file_queue_head = open ("restore_queue_head", "a")

    file_queue_head.write (string_current_queue_head)
    file_queue_head.flush()
    
    file_queue_head.close()
    

def log_task_to_file (task_to_write):
    string_task_to_do = str (task_to_write.task_number) + ","
    string_task_to_do += str (task_to_write.command) + ","
    string_task_to_do += str (task_to_write.try_number) + ","
    string_task_to_do += str (task_to_write.status) + "\n"

    file_lists_of_tasks_to_do = open ("restore_task_queue", "a")

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

    # if a command failed, append it to restore_task_queue with status="failed"
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
    
    max_number_of_retries_to_restore = config_parser.get ('default_config_values',
                                                         'number_of_retries_to_do_restore_steps_if_failed')

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
        
        elif ((task_status == "failed") and (task_try_number < (max_number_of_retries_to_restore + 1))):
            current_task = execute_task (current_queue_head, l_tasks[i], i, task_try_number)

        # number of retries are exceeded
        elif (task_try_number == max_number_of_retries_to_restore):
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


def get_list_of_tasks_in_namedtuple_format_from_file (restore_task_queue):
    try:
        file_lists_of_tasks_to_do = open (restore_task_queue, "r")
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
# Note : This method assumes that 'restore_task_queue' file and 'restore_queue_head' file are
# present after the normal attempt
def try_forcefully_running_the_script ():
    log_info_to_file ("Attempting forcefull retry for failed/cancelled tasks.")
    l_tasks = get_list_of_tasks_in_namedtuple_format_from_file ("restore_task_queue")
    current_queue_head = queue_head ("before", "0")
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
    args_parser.add_argument ("--path_to_file_containing_paths_to_restore=",
                              help="Absolute path to file containing list of paths to restore",
                              dest="path_to_file_containing_paths_to_restore")

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
        config_parser.read ('restore_config')
        if not are_config_values_correct (config_parser, args.path_to_file_containing_paths_to_restore):
            raise ValueError

    except:
        print >> sys.stderr, "Incorrect config file..!! Please check the log..!!"
        print >> sys.stderr, "Aborting ..."
        sys.exit (1)

    # generate list of tasks
    log_info_to_file ("Config values seems to be correct ...")
    l_tasks = generate_lists_of_tasks (config_parser, args.path_to_file_containing_paths_to_restore)
    if not l_tasks:
        log_info_to_file ("Nothing to restore..\nExiting from the script..")
        sys.exit (1)
    else:
        log_info_to_file ("Config values : OK !!\nGenerated list of tasks to do !")

    try:
        # check for 1st run..
        current_queue_head = queue_head ("before", "0")
        l_tasks_read_from_file = []

        log_info_to_file ("checking for 1st run ..")
        if (os.path.exists ("restore_task_queue")):
            # not 1st run 
            # check if the tasks are same as of now
            log_info_to_file ("Not the 1st run ..!!\nReading tasks from the existing file ..")
            l_tasks_read_from_file = get_list_of_tasks_in_namedtuple_format_from_file ("restore_task_queue")

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

                if (os.path.exists ("restore_task_queue")):
                    log_info_to_file ("Deleting existing restore_task_queue file.")
                    subprocess.call ("rm -f restore_task_queue", shell=True)

                if (os.path.exists ("queue_head")):
                    log_info_to_file ("Deleting existing restore_queue_head file.")
                    subprocess.call ("rm -f restore_queue_head", shell=True)
    
                log_list_of_tasks_to_file (l_tasks)

                log_info_to_file ("Created new restore_task_queue file. Starting executions of tasks ..")
                execute_list_of_tasks (l_tasks, current_queue_head, config_parser)
        # if 1st run of the script
        else:
            log_list_of_tasks_to_file (l_tasks)
            log_info_to_file ("Created restore_task_queue file.")

            if (os.path.exists ("queue_head")):
                log_info_to_file ("Deleting existing restore_queue_head file.")
                subprocess.call ("rm -f restore_queue_head", shell=True)

            log_info_to_file ("Starting execution of tasks ..")
            execute_list_of_tasks (l_tasks, current_queue_head, config_parser)

    except:
        exception_value = sys.exc_info()[1]
        log_main.critical (sys.exc_info())
        print >> sys.stderr, "Exception occured.. please check the log file.."
        sys.exit (1)

    log_info_to_file ("restore successfully done..!! :-)")

main()
