# -*- coding: utf-8 -*-
# @author: caoyang
# @email: caoyang@163.sufe.edu.cn

import os
import re
import time
import json
import random
import logging
import requests

from crawler.base import BaseCrawler
from bs4 import BeautifulSoup


class ONetCrawler(BaseCrawler):
	home_url = "https://www.onetonline.org"
	headers = """Host: www.onetonline.org
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
Accept-Language: zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2
Accept-Encoding: gzip, deflate, br
Connection: keep-alive
Cookie: session3=Ci78F2TofJK2QcRsFFqoAg==
Upgrade-Insecure-Requests: 1
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1"""
	headers_dict = BaseCrawler.headers_to_dict(headers)
	
	def __init__(self):
		super(ONetCrawler, self).__init__()

	def run(self):
		save_root = os.path.join("data", "onet")
		os.makedirs(save_root, exist_ok=True)
		# title_results = self.extract_title_results(save_path="content.json")
		title_results = json.load(open("content.json", 'r'))
		for title_result in title_results:
			title_id = title_result["id"]
			title_name = title_result["name"]
			title_url = title_result["url"]
			save_dir = os.path.join(save_root, title_name)
			os.makedirs(save_dir, exist_ok=True)
			skill_results = self.extract_skill_results(title_url, verbose=True)
			with open(os.path.join(save_dir, "meta.json"), 'w', encoding="utf8") as f:
				json.dump(skill_results, f, indent=4)
			for skill_result in skill_results:
				skill_id = skill_result["id"]
				skill_name = skill_result["name"]
				skill_url = skill_result["url"]
				if not skill_url.startswith('#'):
					self.download_excel_and_csv(self.home_url + skill_url, save_dir)

	def download_excel_and_csv(self, skill_url, save_dir=str()):
		response = self.easy_requests(method="GET", url=skill_url, headers=self.headers_dict)
		html = response.text
		soup = BeautifulSoup(html, "lxml")
		h2_tag = soup.find("h2", class_="reportdesc")
		a_tags = h2_tag.find_all("a", class_="ms-2")
		assert len(a_tags) == 2, a_tags
		for a_tag in a_tags:
			href = a_tag.attrs["href"]
			filename = href.split('/')[-1].split('?')[0]
			if href.endswith("xlsx"):
				excel_url = self.home_url + href
				response = self.easy_requests(method="GET", url=excel_url, headers=self.headers_dict)
				with open(os.path.join(save_dir, filename), "wb") as f:
					f.write(response.content)
			elif href.endswith("csv"):
				csv_url = self.home_url + href
				response = self.easy_requests(method="GET", url=csv_url, headers=self.headers_dict)
				with open(os.path.join(save_dir, filename), "wb") as f:
					f.write(response.content)
			else:
				logging.warning(f"Unknown href format: {href}")

	# Extract skill URLs by each title URL
	def extract_skill_results(self, title_url, verbose=False):
		response = self.easy_requests(method="GET", url=title_url, headers=self.headers_dict)
		html = response.text
		soup = BeautifulSoup(html, "lxml")
		# Find top <div> tag by id
		div_tags = soup.find_all("div", id="cmtop")
		assert len(div_tags) == 1, f"Extract more than 1 top <div> tags on {title_url}"
		top_div_tag = div_tags[0]
		# Find top <ul> tag, which is usually the unique sibling of top <div> tag
		ul_tags = top_div_tag.find_all("ul", recursive=False)
		assert len(ul_tags) == 1, f"Extract more than 1 top <ul> tags on {title_url}"
		top_ul_tag = ul_tags[0]
		skill_results = list()
		if verbose:
			# Detailedly build the skill hierarchical tree
			# The hierarchical architecture is like below:
			# <div id="cmtop">
			#   <ul>
			#     <li><a>...</a><div>...</div>...</li>
			#     <li><a>...</a><div>...</div>...</li>
			#     <li><a>...</a><div>...</div>...</li>
			#   </ul>
			# </div>
			# 1. The <a> tag in <li> tag contains the name of the skill
			# 2. The <div> tag in <li> tag contains the description of the skill
			# 3. There may be recursive <div><ul><li>...</li></ul></div> block in the last ellipsis:
			#    - If there is no <div><ul><li>...</li></ul></div> block in ellipsis, then this <li> tag is the leaf node
			#    - Another method to distinguish leaf node is judge whether there is "cm-toggle" in the class of <a> tag
			#    - Another method to distinguish leaf node is judge whether the href of <a> tag starts with '#' (usually "#cm-")
			def _recursive_extract_li_tag(_li_tag):
				_li_tag_id = _li_tag.attrs["id"]
				_li_tag_id_split_list = _li_tag_id.split('-')
				assert _li_tag_id_split_list[0] == "cm", _li_tag_id
				assert _li_tag_id_split_list[1] == "wrap", _li_tag_id
				_a_tag = _li_tag.find('a', recursive=False)									# The <a> tag contains the name of the skill
				_div_tags = _li_tag.find_all("div", recursive=False)						# The first <div> tag contains the description of the skill
				_skill_id = '.'.join(_li_tag_id_split_list[2:])								# e.g. 4.C.1
				_skill_name = self.tag_regex.sub(str(), str(_a_tag)).strip()				# e.g. Interpersonal Relationships
				_skill_url = _a_tag.attrs["href"]											# e.g. #cm-4-C-1-c
				_skill_description = self.tag_regex.sub(str(), str(_div_tags[0])).strip()	# e.g. This category describes the context of the job in terms of human interaction processes.
				skill_results.append({"id": _skill_id, "name": _skill_name, "url": _skill_url, "description": _skill_description})
				# Judge if recursive extraction is required 
				_child_ul_tag = _li_tag.find("ul")
				_leaf_flag_1 = len(_div_tags) == 2
				_leaf_flag_2 = _skill_url.startswith('#')
				assert _leaf_flag_1 == _leaf_flag_2, f"{len(_div_tags)}, {_skill_url}"
				if _leaf_flag_1 and _leaf_flag_2:
					# Recursive extraction
					_top_div_tag = _div_tags[1]
					_ul_tags = _top_div_tag.find("ul", recursive=False),
					assert len(_ul_tags) == 1, f"Extract more than 1 top <ul> tags in skill {_skill_id}"
					_top_url_tag = _ul_tags[0]
					_li_tags = _top_url_tag.find_all("li", recursive=False)
					for _li_tag in _li_tags:
						_recursive_extract_li_tag(_li_tag)
			li_tags = top_ul_tag.find_all("li", recursive=False)
			for li_tag in li_tags:
				_recursive_extract_li_tag(_li_tag=li_tag)
		else:
			# Just find <a> tags of each skill entry
			a_tags = top_ul_tag.find_all('a')
			for a_tag in a_tags:
				href = a_tag.attrs["href"]
				if not href.startswith('#'):
					skill_id = href.split('/')[-1]								# e.g. 1.A.1.d.1
					skill_name = self.tag_regex.sub(str(), str(a_tag)).strip()	# e.g. Memorization 
					skill_url = self.home_url + href							# e.g. https://www.onetonline.org/find/descriptor/result/1.A.1.d.1
					skill_results.append({"id": skill_id, "name": skill_name, "url": skill_url})
		return skill_results

	# Extract title URLs on home page
	def extract_title_results(self, save_path=None):
		response = self.easy_requests(method="GET", url=self.home_url, headers=self.headers_dict)
		html = response.text
		soup = BeautifulSoup(html, "lxml")
		div_tag = soup.find("div", id="hsec-odata")
		a_tags = div_tag.find_all('a')
		title_results = list()
		for a_tag in a_tags:
			href = a_tag.attrs["href"]
			title_id = href.split('/')[-1]								# e.g. 1.A, 1.B.1
			title_name = self.tag_regex.sub(str(), str(a_tag)).strip()	# e.g. Abilities, Interests
			title_url = self.home_url + href							# e.g. https://www.onetonline.org/find/descriptor/browse/1.A
			title_results.append({"id": title_id, "name": title_name, "url": title_url})
		if save_path is not None:
			with open(save_path, 'w', encoding="utf8") as f:
				json.dump(title_results, f, indent=4)			
		return title_results
