#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2018/12/23 13:03
@file    : task_manager.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import argparse
import atexit
import json
import logging
import os
import queue
import sys
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen, PIPE

import psutil

"""
1. 资源管理: ResourceManagement
2. 运行管理(多线程): MultiRunManager
3. 日志管理
4. 参数获取: if main中
5. pool创建工厂: CmdFactory -> CmdPool
6. 命令对象: Command
"""

POSIX = os.name == "posix"
WINDOWS = os.name == "nt"

threads_ref_set = weakref.WeakSet()
processes_ref_set = weakref.WeakSet()
bind_loggers = []


def _call_python_exit():
    """ Callback Function:
        kill all threads and subprocess when the program quit
            exit reasons: raise Exception, normal exit and so on
    :return:
    """
    logger_ = TaskLogger("logger").get_logger("callback_function")
    logger_.info("start closing all threads, all process, and io-handler"
                 " (those were not closed normally)")
    for p in processes_ref_set:
        p.kill()
    logger_.info("all processes are closed")
    for logger in bind_loggers:
        logger.close_all()
    logger_.info("all io-handlers are closed")
    logger_.info("stop callback_function, and end the program")


# Functions thus registered are automatically executed upon normal interpreter termination.
atexit.register(_call_python_exit)


class CommTools(object):
    @staticmethod
    def del_list_elements(target_list: list, *elems, del_all=False):
        if not elems: return
        # delete batch elements from a list
        if del_all:
            flag = 0
            check_dic = {e: 1 for e in elems}
            list_len = len(target_list)
            while flag < list_len:
                if check_dic.get(target_list[flag], 0):
                    target_list.pop(flag)
                    list_len -= 1
                else:
                    flag += 1
        else:
            for elem in elems:
                try:
                    target_list.remove(elem)
                except ValueError:
                    pass

    @staticmethod
    def check_file(*files):
        for file in files:
            if not os.path.isfile(file):
                raise FileNotFoundError('"{}" is not exiting, please check it manually')
        return files

    @staticmethod
    def check_dir(dir_path):
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        return dir_path

    def __new__(cls, *args, **kwargs):
        raise TypeError("CommTools has no instance")


class TaskLogger(object):
    __loggers_obj = dict()
    __LOCK__ = threading.Lock() if WINDOWS else threading.Semaphore(1)

    def __init__(self, logger_name, stream_on=True):
        self.__cmd_pool = None
        self.__outdir = CommTools.check_dir(os.path.join(os.getcwd(), logger_name))
        self.__status_handlers = dict()
        self.__status_dic = dict()
        self.__statuses = ['waiting', 'running', 'completed', 'error']

        self.__level = logging.DEBUG
        self.__streem_on = stream_on
        self.__format = '%(asctime)s    %(name)s    %(levelname)s : %(message)s'
        self.__formatter = logging.Formatter(self.__format, "%Y-%m-%d %H:%M:%S")

        self.__log_path = os.path.join(self.__outdir, "log.txt")
        self.__file_handler = logging.FileHandler(self.__log_path)
        self.__file_handler.setLevel(self.__level)
        self.__file_handler.setFormatter(self.__formatter)

        if self.__streem_on:
            self.__stream_handler = logging.StreamHandler()
            self.__stream_handler.setLevel(self.__level)
            self.__stream_handler.setFormatter(self.__formatter)

        self.__logger = None

    def bind_status(self, cmd_pool):
        self.__cmd_pool = cmd_pool
        for s in self.__statuses:
            file = os.path.join(self.__outdir, s + '_cmds.log')
            self.__status_handlers[s] = open(file, 'w')
        self.update_status()

    def __update_resource(self):
        self.__status_dic[self.__statuses[0]] = [self.__cmd_pool.waiting_list, self.__cmd_pool.remain_list]
        self.__status_dic[self.__statuses[1]] = [self.__cmd_pool.running_list]
        self.__status_dic[self.__statuses[2]] = [self.__cmd_pool.completed_list]
        self.__status_dic[self.__statuses[3]] = [self.__cmd_pool.error_list]

    def update_status(self):
        self.__update_resource()
        for s in self.__statuses:
            handler = self.__status_handlers[s]
            handler.seek(0)
            handler.truncate()

            for lst in self.__status_dic[s]:
                for cmd_name in lst:
                    handler.write(self.__cmd_pool[cmd_name].to_string())
            handler.flush()

    def close_all(self):
        if self.__status_dic:
            self.update_status()
        for _, handler in self.__status_handlers.items():
            handler.close()

    def get_logger(self, name=""):
        """
        :param name: logger name
        """
        self.__logger = logging.getLogger(name)
        self.__logger.propagate = 0
        self._add_handler(self.__logger)
        return self.__logger

    def _add_handler(self, logger):
        logger.setLevel(self.__level)
        logger.addHandler(self.__file_handler)
        if self.__streem_on:
            logger.addHandler(self.__stream_handler)

    def __new__(cls, *args, **kwargs):
        logger_name = args[0]
        # Double-Checked Locking: to increase concurrency
        if not cls.__loggers_obj.get(logger_name):
            with cls.__LOCK__:
                if not cls.__loggers_obj.get(logger_name):
                    cls.__loggers_obj[logger_name] = super().__new__(cls)
        return cls.__loggers_obj[logger_name]


class ResourceManagement(object):
    __LOCK__ = threading.Lock()  if WINDOWS else threading.Semaphore(1)
    __manager_obj = None

    def __init__(self):
        # self.total_cpu_num = psutil.cpu_count() if WINDOWS else psutil.cpu_count() * 2
        self.total_cpu_num = psutil.cpu_count()
        self.total_memory = psutil.virtual_memory().total
        # self.bind_cmds = weakref.WeakKeyDictionary()
        self.bind_cmds = dict()
        self.logger = TaskLogger("logger").get_logger("resource_manager")
        # self.available_memory = memory.available
        self.__available_mem_percent = 0.8
        self.__used_cpu = 0
        self.__used_mem = 0

    def bind_resource(self, cmd_obj):
        cpu = cmd_obj.cpu
        mem = cmd_obj.mem_byte
        with self.__LOCK__:
            if self.check_resource(cmd_obj):
                self.__used_cpu += cpu
                self.__used_mem += mem
                self.bind_cmds[cmd_obj] = 1
                return True
            return False

    def check_resource(self, cmd_obj) -> bool:
        def trans(dynamic_mem, initial_mem):
            val = initial_mem / dynamic_mem
            diff = initial_mem - dynamic_mem
            # g_1 = 1024 * 1024 * 1024
            return diff - val * 0.5

        cpu = cmd_obj.cpu
        mem = cmd_obj.mem_byte
        mem_adjust_value = 0
        for c_obj in self.bind_cmds:
            if hasattr(c_obj, 'dynamic_resource_list'):
                temp_list = c_obj.dynamic_resource_list[:]
                d_cpu, d_mem = [max(i) for i in zip(*temp_list)]
                if d_mem > 0:
                    mem_adjust_value += trans(d_mem, mem)
        available_mem = psutil.virtual_memory().available
        cpu_check = self.__used_cpu + cpu <= self.total_cpu_num
        mem_check = self.__used_mem + mem + mem_adjust_value <= available_mem * self.__available_mem_percent
        total_mem_check = self.__used_mem + mem + mem_adjust_value <= self.total_memory * self.__available_mem_percent
        return cpu_check and mem_check and total_mem_check

    @staticmethod
    def first_check(cmd_obj) -> bool:
        mem = cmd_obj.mem_byte
        available_mem = psutil.virtual_memory().available
        if mem >= available_mem * 0.9:
            return False
        return True

    def release_resource(self, cmd_obj):
        cpu = cmd_obj.cpu
        mem = cmd_obj.mem_byte
        with self.__LOCK__:
            cpu_ = self.__used_cpu - cpu
            mem_ = self.__used_mem - mem
            self.__used_cpu = cpu_ if cpu_ >= 0 else 0
            self.__used_mem = mem_ if mem_ >= 0 else 0
            self.bind_cmds.pop(cmd_obj, 1)
            delattr(cmd_obj, 'dynamic_resource_list')

    def monitor_resource(self, name, pid, result_list: list = None) -> str:
        self.logger.debug(" CMD: " + name + " start monitoring")
        try:
            p = psutil.Process(pid=pid)
        except psutil.NoSuchProcess as e:
            raise Exception(e.msg)
        res_list = result_list if isinstance(result_list, list) else []
        while p.is_running():
            with p.oneshot():
                try:
                    cpu = -1 if WINDOWS else p.cpu_num()
                    mem = p.memory_info().vms
                except psutil.NoSuchProcess:
                    self.logger.debug(" CMD: " + name + " is fast, so there is no resource statistics")
                    break
            res_list.append((cpu, mem))
            length = len(res_list)
            if length >= 600: res_list = [tuple(max(i) for i in zip(*res_list))]
            if p.is_running():
                if length < 60:
                    time.sleep(1)
                else:
                    time.sleep(10)
            else:
                break
        cpu, mem = [max(i) for i in zip(*res_list)]
        if mem != -1:
            mem = ResourceManagement.to_human_readable_format(mem)
        res = "max_cpu:{cpu}\tmax_memory:{mem}".format(
            cpu="not stat in win" if WINDOWS else "no stat" if cpu == -1 else cpu,
            mem= "no stat" if mem == -1 else mem)
        self.logger.debug(" CMD: " + name + " monitoring is ended")
        if result_list is None:
            return res

    @staticmethod
    def to_human_readable_format(*num):
        """
        byte format memory data ---> human readable format [G, M]
        :param num:
        :return:
        """
        temp_list = []
        for mem in num:
            g_mem = round(mem / 1024 / 1024 / 1024, 5)
            m_mem = round(mem / 1024 / 1024, 5)
            mem = (str(g_mem) + "G") if g_mem < 0 else (str(m_mem) + "M")
            temp_list.append(mem)
        return temp_list

    def __new__(cls, *args, **kwargs):
        if cls.__manager_obj is None:
            with cls.__LOCK__:
                if cls.__manager_obj is None:
                    cls.__manager_obj = object().__new__(cls)
        return cls.__manager_obj

    @property
    def used_cpu(self):
        return self.__used_cpu

    @property
    def used_mem(self):
        return self.__used_mem

    @property
    def total_cpu(self):
        return self.total_cpu_num

    @property
    def total_mem(self):
        return self.total_memory


class Command(object):
    def __init__(self, name, cmd: str, cpu: int, mem, depends: list, bind_pool):
        self.__name = name
        self.__cmd = cmd
        self.__cpu = cpu
        self.__mem = mem
        self.__mem_byte = self.parse_mem()
        self.__depends = depends
        self.__bind_pool = bind_pool
        self.is_running = False
        self.is_completed = False
        self.is_waiting = False
        self.is_error = False
        self.is_in_queue = False
        self.is_ready_to_run = False
        self.attempt_times = 0
        self.__lock__ = threading.Lock()  if WINDOWS else threading.Semaphore(1)
        self.__depends_completed_num = 0
        self.logger = TaskLogger("logger").get_logger("command_" + self.name)

    def update_depends_completed_num(self):
        """
        add 1 to self.__depends_completed_num
        :return: None
        """
        with self.__lock__:
            self.__depends_completed_num += 1

    @property
    def is_ready(self):
        """
        judge whether the depended commands are completed
        if all completed, this cmd is ready(we can run it)
        :return: None
        """
        with self.__lock__:
            if self.__depends_completed_num == len(self.__depends):
                return True
            return False

    def run(self, monitor):
        thread_name = threading.current_thread().name
        # ----------------- update state -----------------
        # print(self.is_waiting, self.is_in_queue, self.is_running,
        #       self.is_completed, self.is_error, 'running start')
        # self.is_waiting = False  # this is set in cmd_pool.next() function
        self.is_running = True
        self.is_ready_to_run = False
        # self.__bind_pool.add_running_cmd(self.name)  # this is set in cmd_pool.next() function

        process = Popen(self.__cmd, shell=True, stderr=PIPE, stdout=PIPE, universal_newlines=True)
        result_list = [(-1, -1)]
        # bind dynamic resource list to cmd_obj
        self.dynamic_resource_list = result_list
        # Monitor resource usage of this process
        if callable(monitor):
            # def monitor_resource(self, name, pid, result_list: list = None) -> str:
            thread = threading.Thread(target=monitor, args=(self.name, process.pid, result_list))
            thread.setDaemon(True)
            thread.start()
            # add thread to
            threads_ref_set.add(thread)
        processes_ref_set.add(process)

        # read err, and stdout to avoid pipe blockage
        out, err = [], []

        for line in process.stdout:
            out.append(line)
            err_line = process.stderr.readline()
            if err_line: err.append(err_line)
        for line in process.stderr:
            err.append(line)
            out_line = process.stdout.readline()
            if out_line: out.append(out_line)

        process.wait()  # block the process
        # wait and get the computer resources needed
        if process.returncode != 0:
            self.logger.error(self.name + ' cmd STDERR : ' + ''.join(err).strip())
            self.is_error = True
        if out:
            self.logger.info(self.name + ' cmd STDOUT : ' + ''.join(out).strip())
        cpu, mem = [max(i) for i in zip(*result_list)]
        if mem != -1:
            mem = ResourceManagement.to_human_readable_format(mem)[0]
        res = "max_cpu:{cpu}\tmax_memory:{mem}".format(
            cpu="not stat in win" if WINDOWS else "no stat" if cpu == -1 else cpu,
            mem="no stat" if mem == -1 else mem)

        # ----------------- update state -----------------
        self.__bind_pool.del_running_cmd(self.name)
        self.is_completed = True
        self.is_running = False
        # print(self.is_waiting, self.is_in_queue, self.is_running,
        #       self.is_completed, self.is_error, 'running end')
        # self.logger.info(self.name + " running is stoped, in " + thread_name)
        return res if len(result_list) > 0 else "may be error"

    def parse_mem(self):
        # Make memory machine-readable (int)
        if isinstance(self.__mem, str):
            mem = self.__mem.strip()
            if mem.endswith("G") or mem.endswith("g"):
                mem = int(float(mem[: -1]) * 1024 * 1024 * 1024)
            elif mem.endswith("M") or mem.endswith("m"):
                mem = int(float(mem[: -1]) * 1024 * 1024)
            elif mem.endswith("K") or mem.endswith("k"):
                mem = int(float(mem[: -1]) * 1024)
            else:
                try:
                    mem = int(mem)
                except ValueError:
                    raise Exception("command memory is error, ERROR VALUE: " + mem)
            return mem
        return self.__mem if isinstance(self.mem, int) else -1

    def __str__(self):
        return "{name}\t{cpu}\t{mem}\t{cmd}".format(
            name=self.name,
            cpu=self.cpu,
            mem=self.mem,
            cmd=self.cmd
        )

    def __status(self):
        status = ""
        if self.is_waiting or self.is_in_queue:
            status = 'waiting'
        elif self.is_running:
            status = 'running'
        elif self.is_completed:
            if self.is_error:
                status = 'error'
            else:
                status = 'completed'
        return status

    def to_string(self):
        return "{name}\t{status}\t{cmd}\n".format(
            name=self.name,
            status=self.__status(),
            cmd=self.cmd,
        )

    @property
    def cmd_str_status(self):
        status_list = ['is_waiting', 'is_in_queue', 'is_ready_to_run',
                       'is_running', 'is_completed', 'is_error']
        status_res = []
        for s in status_list:
            status_res.append('{}({})'.format(s, self[s]))
        return 'cmd {name}: ' + ', '.join(status_list)

    def __getitem__(self, item):
        if hasattr(self, item):
            return getattr(self, item)

    @property
    def name(self):
        return self.__name

    @property
    def cmd(self):
        return self.__cmd

    @property
    def cpu(self):
        return self.__cpu

    @property
    def mem(self):
        return self.__mem

    @property
    def mem_byte(self):
        return self.__mem_byte

    @property
    def depends(self):
        return self.__depends


class CmdPool(dict):
    __LOCK__ = threading.Lock() if WINDOWS else threading.Semaphore(1)

    def __init__(self, re_manager: ResourceManagement):
        super(CmdPool, self).__init__()
        self.__manager = re_manager
        self.__is_running_list = []  # cmd is running
        self.__is_waiting_list = []  # cmd is in queue
        self.__is_remain_list = []  # cmd does not meet the running requirements
        self.__is_completed_list = []  # cmd is completed
        self.__is_error_list = []  # # cmd is error
        self.__f2c = {}  # key: depended, value: cmd
        self.__is_ready = False
        self._signal_queue = queue.Queue()
        self.__cmd_queue = queue.Queue()
        self.__total_task = 0
        self.logger = TaskLogger("logger").get_logger("cmd_pool")
        # add one thread to update list
        # thread = threading.Thread(target=self._update_wait_list)
        # thread.setDaemon(True)
        # thread.start()
        # threads_ref_set.add(thread)

    def get_all_deps(self, cmd_obj: Command):
        """
        Recursively retrieves all commands that depend on this cmd_obj
        :param cmd_obj:
        :return: [cmd_obj.name, ...]
        """
        res_list = []
        deps = self.__f2c.get(cmd_obj.name, [])
        if not deps:
            return res_list
        for n in deps:
            res_list.append(n)
            res_list.extend(self.get_all_deps(self[n]))
        return res_list

    def _update_queue(self):
        trans_list = []
        for name, cmd_obj in self.items():
            is_ready = True
            for dname in cmd_obj.depends:
                # cmd is unfinished and in waiting and not in queue
                if not self[dname].is_completed:
                    is_ready = False
                    break
            # is_ready == True: all depends are completed 
            # cmd is not in cmd queue
            # cmd is not in __is_completed_list or cmd_obj.is_completed is True
            cmd_obj = self[name]
            if is_ready and cmd_obj.is_waiting \
                    and not cmd_obj.is_in_queue \
                    and (name not in trans_list and name in self.remain_list):
                # and (not cmd_obj.is_completed and name not in self.__is_completed_list):
                cmd_obj.is_in_queue = True
                self.__cmd_queue.put(name)
                trans_list.append(name)
        # self.__cmd_queue.put(name)
        # <-- self.__is_remain_list
        CommTools.del_list_elements(self.__is_remain_list, *trans_list, del_all=True)
        # --> self.__is_waiting_list
        self.__is_waiting_list.extend(trans_list)
        if not trans_list: return
        self.logger.info('[%s] has/have being add to the cmd_queue' % ', '.join(trans_list))

    @property
    def is_all_completed(self):
        # check_dic = {name: 1 for name in self.completed_list}
        for cmd in self:
            # if not check_dic.get(cmd, 0) or not self[cmd].is_completed:
            if not self[cmd].is_completed:
                return False
        return True

    def _solve_error_cmd(self, run_cmd):
        name = run_cmd.name
        all_relevant_cmds = self.get_all_deps(run_cmd)
        all_relevant_cmds.append(name)
        for i in all_relevant_cmds:
            cmd_obj = self[i]
            cmd_obj.is_running = False
            cmd_obj.is_completed = True
            cmd_obj.is_waiting = False
            cmd_obj.is_error = True
            cmd_obj.is_in_queue = False

        # <----- self.__is_remain_list
        CommTools.del_list_elements(self.__is_remain_list, *all_relevant_cmds, del_all=True)
        # <----- self.__is_waiting_list
        CommTools.del_list_elements(self.__is_waiting_list, *all_relevant_cmds, del_all=True)
        if all_relevant_cmds:
            self.logger.warning(
                " [%s] were added to error queue because of %s's failure"
                % (', '.join(all_relevant_cmds), name))

    def _solve_retry_cmd(self, run_cmd):
        run_cmd.is_running = False
        run_cmd.is_completed = False
        run_cmd.is_waiting = True
        run_cmd.is_error = False
        run_cmd.is_in_queue = True
        run_cmd.is_ready_to_run = False
        self.__cmd_queue.put(run_cmd.name)
        # -----> self.__is_waiting_list
        self.__is_waiting_list.extend(run_cmd.name)

    def next(self, now_run_cmd=None):
        """
            --> queue.put():
                <-- self.__is_remain_list
                --> self.__is_waiting_list
                    cmd_obj.is_in_queue = True

            <-- queue.get(timeout=2)
                <-- self.__is_waiting_list
                if bind_resource is True:
                        cmd_obj.is_in_queue = False
                    else:
                        --> self.__is_waiting_list
                        --> queue.put()
                --> self.__is_running_list

            --> run:
                start:
                    cmd_obj.is_waiting = False
                    cmd_obj.is_running = True
                end:
                    cmd_obj.is_running = False
                    cmd_obj.is_completed  = True
                    -->
                    if error:
                        cmd_obj.is_error = True
                        <-- find all cmds of relying on self
                            (self.__is_waiting_list, self.__is_running_list, self.__is_remain_list)
                            cmd_obj.is_completed  = True
                            cmd_obj.is_error = True
                            cmd_obj.is_waiting = False
                        --> self.__is_completed_list

        :param now_run_cmd:
        :return: "waite", None, cmd_obj
        """
        # Double-Checked Locking: to increase concurrency
        if not self.__is_ready:
            with self.__LOCK__:
                if not self.__is_ready:
                    self.__is_waiting_list = [i for i in set(self.__is_waiting_list)]
                    self.__is_remain_list = [i for i in set(self.__is_remain_list)]
                    self.__is_ready = True

        with self.__LOCK__:  # take command
            if isinstance(now_run_cmd, Command):
                name = now_run_cmd.name
                # <----- cmd_obj running
                CommTools.del_list_elements(self.__is_running_list, name)

                if now_run_cmd.is_completed:

                    # -----> self.__is_completed_list
                    self.__is_completed_list.extend(name)

                    # update attempt_times of cmd running
                    now_run_cmd.attempt_times += 1
                    # cmd error# cmd error
                    if now_run_cmd.is_error and now_run_cmd.attempt_times <= 3:
                        # Give three tries when cmd_obj's running goes wrong
                        self._solve_retry_cmd(run_cmd=now_run_cmd)
                    elif now_run_cmd.is_error:
                        self.logger.error(
                            name + ' cmd : %s tries all failed' % now_run_cmd.attempt_times)
                        # -----> self.__is_error_list
                        self.__is_error_list.append(name)
                        self._solve_error_cmd(run_cmd=now_run_cmd)

                # elif not now_run_cmd.is_waiting and not now_run_cmd.is_error:
                else:
                    assert not now_run_cmd.is_waiting \
                           and now_run_cmd.is_ready_to_run \
                           and not now_run_cmd.is_error, \
                        'cmd status error[bug], please check program\n' + now_run_cmd.cmd_str_status
                    # tries when cmd_obj's resource bind error
                    self._solve_retry_cmd(now_run_cmd)

            # termination condition
            # if len(self.__is_completed_list) == self.__total_task or:
            #     return None
            if self.is_all_completed: return None

            if self.__cmd_queue.qsize() < 2: self._update_queue()
            th = threading.current_thread().name
            flag = 0  # flag: it's used to mark the number of times of taking cmd
            while True:
                flag += 1
                # "waite": the thread getting the this signal will waite a few second
                if flag == 3: return "waite"
                size = self.__cmd_queue.qsize()
                # if size < 2: self._update_queue()
                self._update_queue()
                # if size < 2: self._update_queue()
                for _ in range(size):
                    cmd_name = self.__cmd_queue.get(timeout=2)
                    cmd_obj = self[cmd_name]
                    if cmd_obj.is_waiting and not cmd_obj.is_running and \
                            not cmd_obj.is_completed and \
                            self.__manager.check_resource(cmd_obj=cmd_obj):
                        # cmd_name = self.__cmd_queue.get(timeout=2)
                        # <-- self.__is_waiting_list
                        CommTools.del_list_elements(self.__is_waiting_list, cmd_name)
                        # --> self.__is_running_list
                        self.__is_running_list.append(cmd_name)

                        # queue is False
                        cmd_obj.is_ready_to_run = True
                        cmd_obj.is_in_queue = False
                        cmd_obj.is_waiting = False
                        return cmd_obj
                    else:  # not enough resources
                        cmd_obj.is_in_queue = True
                        self.__cmd_queue.put(cmd_name)
                # self._update_queue()

    def _update_wait_list(self):
        while 1:
            try:
                signal = self._signal_queue.get(timeout=2)
            except queue.Empty:
                time.sleep(2)
                continue
            if signal is None:
                break
            temp = []
            with self.__LOCK__:
                for item in self.__is_remain_list:
                    cmd_obj = self[item]
                    if cmd_obj.is_ready:
                        temp.append(item)
                        self.__is_waiting_list.append(item)
                CommTools.del_list_elements(self.__is_remain_list, *temp)

    def update_dep(self):
        """
        Create dependency network and object and update pool states
        :return:
        """
        for k, v in self.items():
            if v.depends:
                for i in v.depends:
                    if i not in self.__f2c:
                        self.__f2c[i] = []
                    self.__f2c[i].append(k)
            else:
                self[k].is_in_queue = True
                self.__cmd_queue.put(k)
                self.__is_waiting_list.append(k)

        if len(self.__is_waiting_list) != len(set(self.__is_waiting_list)):
            msg = "self.__is_waiting_list has duplication"
            self.logger.error(msg)
            raise Exception(msg)

        CommTools.del_list_elements(self.__is_remain_list, *self.__is_waiting_list, del_all=True)

    def add_waiting_cmds(self, *cmd_names):
        """
        add commands which do not depend any commands
        :param cmd_names: list of command' name
        :return: None
        """
        with self.__LOCK__:
            self.__is_waiting_list.extend(cmd_names)

    def del_waiting_cmds(self, *cmd_names):
        """
        add commands which do not depend any commands
        :param cmd_names: list of command' name
        :return: None
        """
        CommTools.del_list_elements(self.__is_waiting_list, *cmd_names)

    def add_running_cmd(self, name):
        with self.__LOCK__:
            self.__is_running_list.append(name)

    def del_running_cmd(self, name):
        CommTools.del_list_elements(self.__is_running_list, name, del_all=True)

    def is_ready(self):
        self.__is_ready = True

    def __setitem__(self, key, value):
        self.__is_remain_list.append(key)
        if key not in self:
            self.__total_task += 1
        else:
            raise Exception("the cmd name has duplication, please check them")
        super(CmdPool, self).__setitem__(key, value)

    @property
    def remain_list(self):
        return self.__is_remain_list

    @property
    def waiting_list(self):
        return self.__is_waiting_list

    @property
    def running_list(self):
        return self.__is_running_list

    @property
    def completed_list(self):
        return self.__is_completed_list

    @property
    def error_list(self):
        return self.__is_error_list


class CmdFactory(object):
    def __init__(self, cmd_json: str, relation_json: str):
        self.__cmd_dict = None
        self.__rel_dict = None

        def catch_except(func, *args_, **kwargs_):
            try:
                return func(*args_, **kwargs_)
            except json.decoder.JSONDecodeError as e:
                raise Exception("json.decoder.JSONDecodeError: " + e.msg)
            except Exception:
                raise Exception("json decoder error")

        with open(cmd_json) as cmd_handler, open(relation_json) as rela_handler:
            self.__cmd_dict = catch_except(json.load, cmd_handler)
            self.__rel_dict = catch_except(json.load, rela_handler)

    def manuf_cmd_pool(self, resource_manager):
        """
        create CmdPool(resource_manager) instance
        :param resource_manager:
        :return: CmdPool()
        """
        pool = CmdPool(resource_manager)
        for k, v in self.__cmd_dict.items():
            cmd_obj = Command(
                k, v,
                cpu=self.__rel_dict[k]["cpu"],
                mem=self.__rel_dict[k]["mem"],
                depends=self.__rel_dict[k]["depends"],
                bind_pool=pool
            )
            # first resource check
            if resource_manager.first_check(cmd_obj):
                cmd_obj.is_waiting = True
                pool[k] = cmd_obj
            else:
                available_mem = psutil.virtual_memory().available / 1024 / 1024 / 1024
                available_mem = str(round(available_mem * 1024, 5)) + "M" \
                    if available_mem < 1 else str(round(available_mem, 5)) + "G"
                raise Exception(
                    "Task: " + k + "'s memory (%s) exceed the available_mem (%s)"
                    % (cmd_obj.mem, available_mem))
        pool.update_dep()
        return pool


class MultiRunManager(object):
    def __init__(self, cmd_pool: CmdPool, resource_manager: ResourceManagement, max_thread):
        self.__pool = cmd_pool
        self.__max_thread = max_thread
        self.__resource_manager = resource_manager
        self.__logger = TaskLogger("logger")
        bind_loggers.append(self.__logger)
        self.__logger.bind_status(cmd_pool=self.__pool)
        self.logger = self.__logger.get_logger("running_log")

    def _cmd_run(self):
        thread = threading.current_thread()
        cmd_obj = self.__pool.next()

        self.logger.info(" THREAD: " + thread.name + ' running is starting')
        while True:
            if isinstance(cmd_obj, Command):
                # bind resource (determine whether the resources meet the cmd requirements)
                # if the bind is successful, it will run, or else it will add cmd to queue
                if self.__resource_manager.bind_resource(cmd_obj):
                    run_debug = " resource binding is successful and start running in -- "
                    self.logger.debug(" CMD: " + cmd_obj.name + run_debug + thread.name)
                    resource_stat = cmd_obj.run(self.__resource_manager.monitor_resource)
                    # release resource bound before
                    self.__resource_manager.release_resource(cmd_obj)
                    self.logger.info(
                        " CMD: " + cmd_obj.name + " completed and resource is released successfully")
                    self.logger.info(" CMD: " + cmd_obj.name + " resource stat: " + resource_stat)
                    cmd_obj = self.__pool.next(cmd_obj)
                    self.__logger.update_status()
                else:
                    warning = " resource binding is unsuccessful, and add it to the queue, and sleep 2s"
                    self.logger.warning(" CMD: " + cmd_obj.name + warning)
                    self.logger.warning(" CMD RAW resource: " + str(cmd_obj))  # write cmd_obj's raw resource to log
                    self.logger.warning(" CMD RUNNING STATUS: " + cmd_obj.cmd_str_status)
                    time.sleep(2)
                    cmd_obj = self.__pool.next(cmd_obj)
                # self.__pool.add_waiting_cmds(cmd_obj)
            else:
                if cmd_obj is None:
                    # self.logger.info(thread.name + ' running is end finally')
                    break
                else:
                    time.sleep(2)
                    cmd_obj = self.__pool.next()
        self.__logger.update_status()
        self.logger.info(" THREAD: " + thread.name + ' running is end finally')

    def run(self):
        with ThreadPoolExecutor(self.__max_thread) as executor:
            exc_list = [executor.submit(self._cmd_run) for _ in range(self.__max_thread)]
        res = [i.result() for i in exc_list]
        if any(res):
            self.logger.debug('threads returns: ' + '\n'.join(res))
        # exc_list = [threading.Thread(target=self._cmd_run) for _ in range(self.__max_thread)]
        # _ = [t.start() for t in exc_list]
        # _ = [t.join() for t in exc_list]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cmd_json", required=True, help="commands file of json")
    parser.add_argument("-r", "--relation_json", required=True, help="file of json: relations, cpu, mem")
    parser.add_argument("-t", "--max_threads", type=int, default=10,
                        help="max numbers of threads [default: %(default)s]")
    argvs = sys.argv[1:]
    if len(argvs) == 0 or '-h' in argvs or "--help" in argvs:
        parser.parse_args(['-h'])

    args_obj = parser.parse_args()
    cmd_json_file = args_obj.cmd_json
    relation_json_file = args_obj.relation_json

    cmd_json_file, relation_json_file = CommTools.check_file(cmd_json_file, relation_json_file)

    res_manager = ResourceManagement()
    cmd_factory = CmdFactory(cmd_json_file, relation_json_file)
    cmds_pool = cmd_factory.manuf_cmd_pool(res_manager)

    runner = MultiRunManager(cmds_pool, res_manager, args_obj.max_threads)
    runner.run()
