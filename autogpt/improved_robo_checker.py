from urllib import robotparser
from urllib.parse import urlparse
import time
import uuid
import os
import json


class RoboChecker:
    def __init__(self):
        self.id = uuid.uuid4()
        self.hosts_key = 'hosts'
        self.last_visit_s_key = 'last_visit_s'
        self.wait_s_key = 'wait_s'
        self.rp_key = 'parser'
        self.na_key = 'na' # robots txt not available
        self.buffer_s = 10 # additional seconds to wait until next request / next fetch of robots txt
        self.max_wait_s = 60
        self.cached_visits_path = './visits.json'
        self.cached_visits = self.init_and_get_cached_visits() # Gets persistet when changed & loaded on startup
        self.cached_rps = {self.hosts_key: {}, self.na_key: []} # Only during runtime

    # persist: {hosts: {host: {last_visit_s: int, wait_s: int}}, na: [hosts without robots txt]} 
    # {hosts: {host: {parser: rp, last_visit_s: int, wait_s: int}}, na: [hosts without robots txt]}


    def init_and_get_cached_visits(self):
        cached_exists = os.path.isfile(self.cached_visits_path)
        if cached_exists:
            with open(self.cached_visits_path, 'r') as cached_visits_file:
                data = json.load(cached_visits_file)
                if self.hosts_key in data and self.na_key in data:
                    print(f'Load cached visits from json:\n{data}')
                    return data
        return {self.hosts_key: {}, self.na_key: []}
    

    def persist_cached_visits(self):
        with open(self.cached_visits_path, 'w') as cached_visits_file:
            json.dump(self.cached_visits, cached_visits_file)
            print('Persisted cached visits')


    def add_host_robots_na(self, host):
        self.cached_rps[self.na_key].append(host)
        self.cached_visits[self.na_key].append(host)
        self.persist_cached_visits()


    def is_cached(self, host):
        return host in self.cached_rps[self.hosts_key]


    def is_visit_cached(self, host):
        return host in self.cached_visits[self.hosts_key]


    def is_host_na(self, host):
        return host in self.cached_rps[self.na_key] or host in self.cached_visits[self.na_key]


    def add_host_to_hosts(self, host, parser, wait_s):
        last_visit_s = int(time.time()) + 1
        host_data_dict = {self.rp_key: parser, self.last_visit_s_key: last_visit_s, self.wait_s_key: wait_s}
        self.cached_rps[self.hosts_key][host] = host_data_dict
        self.cached_visits[self.hosts_key][host] = {self.last_visit_s_key: last_visit_s, self.wait_s_key: wait_s}
        self.persist_cached_visits()


    def update_last_visit_s(self, host, last_visit_s):
        self.cached_rps[self.hosts_key][host][self.last_visit_s_key] = last_visit_s
        self.cached_visits[self.hosts_key][host][self.last_visit_s_key] = last_visit_s
        self.persist_cached_visits()


    def get_cached_host_data(self, host):
        data = self.cached_rps[self.hosts_key][host]
        return (data[self.rp_key], data[self.last_visit_s_key], data[self.wait_s_key])


    def get_cached_visit_data(self, host):
        data = self.cached_visits[self.hosts_key][host]
        return (data[self.last_visit_s_key], data[self.wait_s_key])


    def get_robo_parser(self, url, user_agent):
        base_url = urlparse(url).scheme + '://' + urlparse(url).hostname
        robots_url = base_url + '/robots.txt'
        try:
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            delay = rp.crawl_delay(user_agent)
            delay_s = self.buffer_s if delay == None else int(delay) + self.buffer_s
            print(f'Got robots txt for {base_url}')
            return (rp, delay_s)
        except Exception as e:
            print(f'Cannot find robots txt: {base_url}')
            return (None, None)
        

    def get_robo_parser_for_visit(self, visit_data, url, user_agent):
        if visit_data == None:
            return None
        
        (last_visit_s, wait_s) = visit_data
        elapsed_since_last_visit = (int(time.time()) - 1) - last_visit_s
        remaining_wait = wait_s - elapsed_since_last_visit

        if remaining_wait < 0:
            return self.get_robo_parser(url, user_agent)
        elif remaining_wait < self.max_wait_s:
            print(f'WAITING {remaining_wait}s to reload robots txt for {url}')
            time.sleep(remaining_wait)
            return self.get_robo_parser(url, user_agent)
        else:
            print(f'NOT WAITING {remaining_wait}s to reload robots txt for {url}')
            return None
        

    def check_is_ua_allowed(self, host, url, user_agent):
        print(f'Checking with robo checker {self.id}')
        (rp, last_visit_s, wait_s) = self.get_cached_host_data(host)
        elapsed_since_last_visit = (int(time.time()) - 1) - last_visit_s
        remaining_wait = wait_s - elapsed_since_last_visit
        if remaining_wait < 0:
            return rp.can_fetch(user_agent, url)
        elif remaining_wait < self.max_wait_s:
            print(f'WAITING {remaining_wait}s to check permission for {url}')
            time.sleep(remaining_wait)
            return rp.can_fetch(user_agent, url)
        else:
            print(f'NOT WAITING {remaining_wait}s for {url}')
            return False


    def is_local(self, url: str):
        if url == None:
            return False
        is_localhost = url.startswith('localhost') or url.startswith('http://localhost') or url.startswith('127.0.0.1') or url.startswith('http://127.0.0.1')
        is_local_file = url.startswith('file:///')
        return is_localhost or is_local_file
        

    def is_allowed(self, url, user_agent):
        print(f'CHECKING url: {url} for ua: {user_agent}')
        if self.is_local(url):
            print(f'ACCESS ALLOWED, is local url: {url}')
            return True
        base_url = urlparse(url).scheme + '://' + urlparse(url).hostname
        if self.is_host_na(base_url):
            print(f'Not allowed according to cache: {base_url}')
            return False
        if self.is_cached(base_url):
            is_allowed = self.check_is_ua_allowed(base_url, url, user_agent)
            self.update_last_visit_s(base_url, int(time.time()) + 1)
            return is_allowed
        elif self.is_visit_cached(base_url):
            (last_visit_s, wait_s) = self.get_cached_visit_data(base_url)
            robo_parser_for_visit = self.get_robo_parser_for_visit((last_visit_s, wait_s), url, user_agent)

            # return false, add to not allowed if failed getting robot parser data
            if robo_parser_for_visit == None:
                self.add_host_robots_na(base_url)
                return False
            (rp, wait_s_new) = robo_parser_for_visit
            if rp == None or wait_s_new == None:
                self.add_host_robots_na(base_url)
                return False
            
            self.add_host_to_hosts(base_url, rp, wait_s_new)
            is_allowed = self.check_is_ua_allowed(base_url, url, user_agent)
            self.update_last_visit_s(base_url, int(time.time()) + 1)
            return is_allowed
        else:
            (rp, wait_s) = self.get_robo_parser(url, user_agent)

            # return false, add to not allowed if failed getting robot parser data
            if rp == None or wait_s == None:
                self.add_host_robots_na(base_url)
                return False
            
            self.add_host_to_hosts(base_url, rp, wait_s)
            is_allowed = self.check_is_ua_allowed(base_url, url, user_agent)
            self.update_last_visit_s(base_url, int(time.time()) + 1)
            return is_allowed


RoboCheckerInstance = RoboChecker()
