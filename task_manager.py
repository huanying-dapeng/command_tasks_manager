#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2018/12/23 13:03
@file    : task_manager.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""
import atexit
import os
import sys
import queue
import time
import json
import threading
import psutil
import weakref
import argparse
from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor

"""
1. 资源管理
2. 运行管理（多线程）
3. 日志管理
4. 参数获取
5. pool创建工厂
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
        return self.__used_cpu + cpu <= self.total_cpu_num \
                and self.__used_mem + mem <= available_mem * self.__available_mem_percent \
                and self.__used_mem + mem <= self.total_memory * self.__available_mem_percent

    def first_check(self, cmd_obj) -> bool:
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
    def monitor_resource(pid: int, result_list: list=None) -> str:
        try:
            p = psutil.Process(pid=pid)
        except psutil._exceptions.NoSuchProcess as e:
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


class CmdPool(dict):
    __LOCK__ = threading.Lock()

    def __init__(self, re_manager: ResourceManagement):
        super(CmdPool, self).__init__()
        self.__manager = re_manager
        self.is_running_list = []
        self.__is_waiting_list = []
        self.is_remain_list = []
        self.is_completed_list = []
        self.is_error_list = []
        self.__c2f = {}  # key: cmd, value: depend
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

    def get_all_deps(self, cmd_obj):
        res_list = []
        if isinstance(cmd_obj, Command):
            deps = self.__f2c.get(cmd_obj.name, [])
            if not deps:
                return [cmd_obj.name]
            for n in deps:
                res_list.append(n)
                res_list.extend(self.get_all_deps(self[n]))
        return res_list

    def next(self, now_run_cmd=None):
        if not self.__is_ready:
            with self.__LOCK__:
                if not self.__is_ready:
                    self.__is_waiting_list = [i for i in set(self.__is_waiting_list)]
                    self.is_remain_list = [i for i in set(self.is_remain_list)]
                    self.__is_ready = True

        with self.__LOCK__:
            if isinstance(now_run_cmd, Command):
                if now_run_cmd.is_completed:
                    if now_run_cmd.is_error:
                        self.is_error_list.append(now_run_cmd.name)
                        for i in self.get_all_deps(now_run_cmd):
                            self[i].is_running = False
                            self[i].is_completed = True
                            self[i].is_waiting = False
                            self[i].is_error = True
                    else:
                        self.is_completed_list.append(now_run_cmd.name)
                elif now_run_cmd.is_waiting and not now_run_cmd.is_error:
                    self[now_run_cmd.name].is_running = False
                    self[now_run_cmd.name].is_completed = False
                    self[now_run_cmd.name].is_waiting = True
                    self[now_run_cmd.name].is_error = False
                    self.__cmd_queue.put(now_run_cmd.name)

            if len(self.is_completed_list) == self.__total_task: return None
            # print(self.is_completed_list)
            flag = 0
            while True:
                flag += 1
                if flag == 3: return "waite"
                size = self.__cmd_queue.qsize()
                for i in range(size):
                    cmd_name = self.__cmd_queue.get(timeout=2)
                    cmd_obj = self[cmd_name]
                    if cmd_obj.is_waiting and self.__manager.check_resource(cmd_obj=cmd_obj):
                        cmd_obj.is_waiting = False
                        return cmd_obj
                    else:
                        self.__cmd_queue.put(cmd_name)
                for name, cmd_obj in self.items():
                    is_ready = True
                    for dname in cmd_obj.depends:
                        if not self[dname].is_completed:
                            is_ready = False
                            break
                    if is_ready and name not in self.is_completed_list:
                        self.__cmd_queue.put(name)

        # with self.__LOCK__:
        #     if now_run_cmd is not None:
        #         if now_run_cmd.is_completed:
        #             for i in self.__f2c.get(now_run_cmd.name, []):
        #                 self[i].update_depends_completed_num()
        #         self.is_completed_list.append(now_run_cmd.name)
        #         self.is_running_list.pop(self.is_running_list.index(now_run_cmd.name))
        #
        #     is_all_completed = True
        #     for name, cmd_obj in self.items():
        #         is_ready = True
        #         for dname in cmd_obj.depends:
        #             # if not self[dname].is_completed:
        #                 is_ready = False
        #                 break
        #         if is_all_completed and not self[name].is_completed:
        #             is_all_completed = False
        #         if is_ready: return cmd_obj
        #     return None if is_all_completed else "waite"
            # if len(self.__is_waiting_list) > 0:
            #     for i, item in enumerate(self.__is_waiting_list[:]):
            #         cmd_obj = self[item]
            #         if self.__manager.check_resource(cmd_obj=cmd_obj):
            #             self._signal_queue.put(1)
            #             self.__is_waiting_list.pop(i)
            #             print(self.__is_waiting_list, cmd_obj, "self.__is_waiting_list" )
            #             return cmd_obj
            #         else:
            #             return "waite"
            # elif len(self.__is_waiting_list) == 0 and len(self.is_remain_list) > 0:
            #     self._signal_queue.put(1)
            #     return "waite"
            # elif len(self.__is_waiting_list) == 0 and len(self.is_remain_list) == 0:
            #     self._signal_queue.put(None)
            #     return None
            # else:
            #     print("******************************************")

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
                for item in self.is_remain_list:
                    cmd_obj = self[item]
                    if cmd_obj.is_ready:
                        temp.append(item)
                        self.__is_waiting_list.append(item)
                for i in temp:
                    self.is_remain_list.pop(self.is_remain_list.index(i))

    def add_waiting_cmds(self, *cmd_names):
        """
        add commands which do not depend any commands
        :param cmd_names: list of command' name
        :return: None
        """
        with self.__LOCK__:
            self.__is_waiting_list.extend(cmd_names)

    def update_dep(self):
        temp_list = self.is_remain_list[:]
        for k, v in self.items():
            for i in v.depends:
                if i not in self.__f2c:
                    self.__f2c[i] = []
                if k in temp_list:
                    temp_list.pop(temp_list.index(k))
                self.__f2c[i].append(k)
        self.__is_waiting_list = temp_list
        for i in temp_list:
            self.__cmd_queue.put(i)
            self.is_remain_list.pop(self.is_remain_list.index(i))

    def add_running_cmd(self, name):
        with self.__LOCK__:
            self.is_running_list.append(name)

    def is_ready(self):
        self.__is_ready = True

    def __setitem__(self, key, value):
        self.is_remain_list.append(key)
        if key not in self:
            self.__total_task += 1
        super(CmdPool, self).__setitem__(key, value)


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
        self.first_is_check = False
        self.__lock__ = threading.Lock()
        self.__depends_completed_num = 0

    def update_depends_completed_num(self):
        """
        add 1 to self.__depends_completed_num
        :param num:
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
        self.is_waiting = False
        self.is_running = True
        process = Popen(self.__cmd, shell=True, stderr=PIPE, stdout=PIPE, universal_newlines=True)
        self.__bind_pool.add_running_cmd(self.name)
        result_list = []
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
        self.is_running = False
        self.is_completed = True
        return res if len(result_list) > 0 else "may be error"

    def parse_mem(self):
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
        pool = CmdPool(resource_manager)
        for k, v in self.__cmd_dict.items():
            cmd_obj = Command(
                k, v,
                cpu=self.__rel_dict[k]["cpu"],
                mem=self.__rel_dict[k]["mem"],
                depends=self.__rel_dict[k]["depends"],
                bind_pool=pool
            )
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
                if self.__resource_manager.bind_resource(cmd_obj):
                    resource_stat = cmd_obj.run(self.__resource_manager.monitor_resource)
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
        exc_list = []
        with ThreadPoolExecutor(self.__max_thread) as executor:
            for i in range(self.__max_thread):
                exc_list.append(executor.submit(self._cmd_run))
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
    args = parser.parse_args()
    cmd_dic = args.cmd_json
    relation_dic = args.relation_json

    res_manager = ResourceManagement()
    cmd_factory = CmdFactory(cmd_dic, relation_dic)
    cmds_pool = cmd_factory.manuf_cmd_pool(res_manager)

    runner = MultiRunManager(cmds_pool, res_manager, args.max_threads)
    runner.run()
