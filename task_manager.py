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


def _call_python_exit():
    """ Callback Function:
        kill all threads and subprocess when the program quit
            exit reasons: raise Exception, normal exit and so on
    :return:
    """
    for t in threads_ref_set:
        t.join()
    for p in processes_ref_set:
        p.kill()


# Functions thus registered are automatically executed upon normal interpreter termination.
atexit.register(_call_python_exit)


class CommTools(object):
    @staticmethod
    def del_list_elements(target_list: list, *elems, del_all=False):
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

    def __new__(cls, *args, **kwargs):
        raise TypeError("CommTools has no instance")


class ResourceManagement(object):
    __LOCK__ = threading.Lock()

    def __init__(self):
        self.total_cpu_num = psutil.cpu_count() if WINDOWS else psutil.cpu_count() * 2
        self.total_memory = psutil.virtual_memory().total
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
                return True
            return False

    def check_resource(self, cmd_obj) -> bool:
        cpu = cmd_obj.cpu
        mem = cmd_obj.mem_byte
        available_mem = psutil.virtual_memory().available
        cpu_check = self.__used_cpu + cpu <= self.total_cpu_num
        mem_check = self.__used_mem + mem <= available_mem * self.__available_mem_percent
        total_mem_check = self.__used_mem + mem <= self.total_memory * self.__available_mem_percent
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

    @staticmethod
    def monitor_resource(pid: int, result_list: list = None) -> str:
        try:
            p = psutil.Process(pid=pid)
        except psutil.NoSuchProcess as e:
            raise Exception(e.msg)
        res_list = result_list if isinstance(result_list, list) else []
        while p.is_running():
            cpu = -1 if WINDOWS else p.cpu_num()
            mem = p.memory_info().vms
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

        (cpu, mem) = [max(i) for i in zip(*res_list)]
        mem = ResourceManagement.to_human_readable_format(mem)
        res = "max_cpu:{cpu}\tmax_memory:{mem}".format(
            cpu="not stat in win" if WINDOWS else cpu, mem=mem)

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
        self.first_is_check = False
        self.__lock__ = threading.Lock()
        self.__depends_completed_num = 0

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
        # ----------------- update state -----------------
        self.is_waiting = False  # waiting is False
        self.is_running = True
        self.__bind_pool.add_running_cmd(self.name)

        process = Popen(self.__cmd, shell=True, stderr=PIPE, stdout=PIPE, universal_newlines=True)
        result_list = []

        # Monitor resource usage of this process
        if callable(monitor):
            thread = threading.Thread(target=monitor, args=(process.pid, result_list))
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
            if out_line: err.append(out_line)

        process.wait()  # block the process
        # wait and get the computer resources needed
        # if thread is not None: thread.join()
        if process.returncode != 0: self.is_error = True
        cpu, mem = [max(i) for i in zip(*result_list)]
        mem = ResourceManagement.to_human_readable_format(mem)
        res = "max_cpu:{cpu}\tmax_memory:{mem}".format(
            cpu="not stat in win" if WINDOWS else cpu, mem=mem[0])

        # ----------------- update state -----------------
        self.is_running = False
        self.is_completed = True
        self.__bind_pool.del_running_cmd(self.name)

        return res if len(result_list) > 0 else "may be error"

    def parse_mem(self):
        # Make memory machine-readable (int)
        if isinstance(self.__mem, str):
            print("mem", self.__mem)
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
    __LOCK__ = threading.Lock()

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
            print(is_ready,is_ready
                    ,not cmd_obj.is_in_queue, (name not in self.__is_completed_list or not cmd_obj.is_completed),self.__is_completed_list)
            if is_ready \
                    and not cmd_obj.is_in_queue \
                    and (name not in self.__is_completed_list or not cmd_obj.is_completed):
                cmd_obj.is_in_queue = True
                self.__cmd_queue.put(name)
                trans_list.append(name)
        # self.__cmd_queue.put(name)
        # <-- self.__is_remain_list
        CommTools.del_list_elements(self.__is_remain_list, *trans_list)
        # --> self.__is_waiting_list
        self.__is_waiting_list.extend(trans_list)

    def next(self, now_run_cmd=None):
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
                if now_run_cmd.is_completed:
                    all_relevant_cmds = []
                    # cmd error# cmd error
                    if now_run_cmd.is_error:
                        # <-- cmd_obj running error
                        CommTools.del_list_elements(self.__is_running_list, name)
                        # --> self.__is_error_list
                        self.__is_error_list.append(name)
                        all_relevant_cmds = self.get_all_deps(now_run_cmd)
                        all_relevant_cmds.append(name)
                        for i in all_relevant_cmds:
                            cmd_obj = self[i]
                            cmd_obj.is_running = False
                            cmd_obj.is_completed = True
                            cmd_obj.is_waiting = False
                            cmd_obj.is_error = True
                            cmd_obj.is_in_queue = False
                        # <-- self.__is_remain_list
                        CommTools.del_list_elements(self.__is_remain_list, *all_relevant_cmds, del_all=True)
                    # normal accomplishment
                    else:
                        all_relevant_cmds.append(name)
                        # <-- self.__is_running_list cmd_obj running
                        CommTools.del_list_elements(self.__is_running_list, name)
                    # <-- self.__is_waiting_list
                    CommTools.del_list_elements(self.__is_waiting_list, *all_relevant_cmds, del_all=True)
                    # --> self.__is_completed_list
                    self.__is_completed_list.extend(all_relevant_cmds)
                elif now_run_cmd.is_waiting and not now_run_cmd.is_error:
                    now_run_cmd.is_running = False
                    now_run_cmd.is_completed = False
                    now_run_cmd.is_waiting = True
                    now_run_cmd.is_error = False
                    now_run_cmd.is_in_queue = True
                    self.__cmd_queue.put(name)
                    # <-- cmd_obj running
                    CommTools.del_list_elements(self.__is_running_list, name)
                    # --> self.__is_waiting_list
                    self.__is_waiting_list.extend(name)

            # termination condition
            if len(self.__is_completed_list) == self.__total_task:
                return None

            flag = 0  # flag: it's used to mark the number of times of taking cmd
            while True:
                flag += 1
                # "waite": the thread getting the this signal will waite a few second
                if flag == 3: return "waite"
                size = self.__cmd_queue.qsize()
                if size < 2: self._update_queue()
                # if size < 2: self._update_queue()
                for _ in range(size):
                    cmd_name = self.__cmd_queue.get(timeout=2)
                    cmd_obj = self[cmd_name]
                    if cmd_obj.is_waiting and self.__manager.check_resource(cmd_obj=cmd_obj):
                        # cmd_name = self.__cmd_queue.get(timeout=2)
                        # <-- self.__is_waiting_list
                        CommTools.del_list_elements(self.__is_waiting_list, cmd_name)
                        # --> self.__is_running_list
                        self.__is_running_list.append(cmd_name)
                        # queue is False
                        cmd_obj.is_in_queue = False
                        return cmd_obj
                    else:  # not enough resources
                        cmd_obj.is_in_queue = True
                        self.__cmd_queue.put(cmd_name)
                self._update_queue()

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
        del_list = []
        for k, v in self.items():
            if v.depends:
                for i in v.depends:
                    if i not in self.__f2c:
                        self.__f2c[i] = []
                        del_list.append(k)
                    self.__f2c[i].append(k)
            else:
                self[k].is_in_queue = True
                self.__cmd_queue.put(k)
                self.__is_waiting_list.append(k)

        if len(self.__is_waiting_list) != len(set(self.__is_waiting_list)):
            raise Exception("self.__is_waiting_list has duplication")

        CommTools.del_list_elements(self.__is_remain_list, *del_list, del_all=True)

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

    def _cmd_run(self):
        cmd_obj = self.__pool.next()
        while True:
            if isinstance(cmd_obj, Command):
                # bind resource (determine whether the resources meet the cmd requirements)
                # if the bind is successful, it will run, or else it will add cmd to queue
                if self.__resource_manager.bind_resource(cmd_obj):
                    resource_stat = cmd_obj.run(self.__resource_manager.monitor_resource)
                    # release resource bound before
                    self.__resource_manager.release_resource(cmd_obj)
                    print(threading.current_thread().name, cmd_obj, " resource_stat: " + resource_stat)
                    cmd_obj = self.__pool.next(cmd_obj)
                else:
                    time.sleep(2)
                    cmd_obj = self.__pool.next(cmd_obj)
                # self.__pool.add_waiting_cmds(cmd_obj)
            else:
                if cmd_obj is None:
                    break
                else:
                    time.sleep(2)
                    cmd_obj = self.__pool.next()

    def run(self):
        with ThreadPoolExecutor(self.__max_thread) as executor:
                exc_list = [executor.submit(self._cmd_run) for _ in range(self.__max_thread)]
        _ = [i.result() for i in exc_list]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cmd_json", required=True, help="commands file of json")
    parser.add_argument("-r", "--relation_json", required=True, help="file of json: relations, cpu, mem")
    parser.add_argument("-t", "--max_threads", type=int, default=10,
                        help="max numbers of threads [default: %(default)s]")
    argvs = sys.argv[1:]
    if len(argvs) == 0 or '-h' in argvs or "--help" in argvs:
        parser.parse_args(['-h'])
    print(argvs, "argv")
    args_obj = parser.parse_args()
    cmd_json_file = args_obj.cmd_json
    relation_json_file = args_obj.relation_json

    cmd_json_file, relation_json_file = CommTools.check_file(cmd_json_file, relation_json_file)

    res_manager = ResourceManagement()
    cmd_factory = CmdFactory(cmd_json_file, relation_json_file)
    cmds_pool = cmd_factory.manuf_cmd_pool(res_manager)

    runner = MultiRunManager(cmds_pool, res_manager, args_obj.max_threads)
    runner.run()
