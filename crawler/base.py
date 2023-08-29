# -*- coding: utf-8 -*-
# @author: caoyang
# @email: caoyang@163.sufe.edu.cn

import re
import time
import logging
import requests

class BaseCrawler:

	tag_regex = re.compile(r"<[^>]+>|\n|\t")
	global_timeout = 60
	global_interval = 300
	
	def headers_to_dict(headers: str) -> dict:
		lines = headers.splitlines()
		headers_dict = {}
		for line in lines:
			key, value = line.strip().split(':', 1)
			headers_dict[key.strip()] = value.strip()
		return headers_dict


	def easy_requests(self, method, url, **kwargs):
		while True:
			try:
				response = requests.request(method, url, **kwargs)
				break
			except Exception as e:
				logging.warning(f"Error {method} {url}, exception information: {e}")
				time.sleep(self.global_interval)
		return response
