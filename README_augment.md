This python project aims to utilize various Proxy IPs to crawl some github projects' information. Do noy worry about the safety problem - we have contacted the github official and they told me to do so - as IP limits still apply, this will make sure we do not do harm to the website.

The proxy usage is very simple. Just make sure all your requests are under the proxy address:
```
http://127.0.0.1:7892
```
(both http_proxy and https_proxy)
And each request sent out will be automatically proxied by the system.

We will have several `.json` input file, and each file is in the following format:
```
{
  "language": "Python",
  "summary": {
    "total_repositories": 388970,
    "total_stars": 56926598,
    "average_stars": 146.35,
    "top_repository": {
      "url": "https://github.com/public-apis/public-apis",
      "stars": 327362
    }
  },
  "repositories": [
    {
      "url": "https://github.com/public-apis/public-apis",
      "stars": 327362,
      "language": [
        "Python"
      ]
    },
    ...
  ]
}
```
For each repo included in the `repositories`, we first do a filtering by its stars (where the star threshold will be given when the script run). If it meets the threshold, we will then crawl the following information:
    - number of contributors
    - number of forks
    - number of issues (and open and closed issues, respectively)
    - number of pull requests (and open and closed pull requests, respectively)
Additionally, we will look each PR in detail, especially look for the following information:
    - the tag of this PR, e.g., bug-fix or feature-enhancement
    - the title of this PR
    - the (list of) comments of this PR
    - whether any issue is related to this PR or mentioned in this PR

The crawled result should be saved in a `.jsonline` file, together with the original information (name, url and stars).

A general requirement of the project:
1. No tool / module limit or preference. Just make sure you can complete all the tasks.
2. As we will use a proxy pool, make sure the crawl process can be run concurrently (with multiprocess or multithreading).
3. The code should be well-documented and easy to read.
4. The code should be easily launched with commandline.
5. The code utilizes `loguru`'s logger to show messages. Also keep a local logfile besides stdout.
6. Keep the code simple and stupid.
7. Make network request rebust enough - add retry and logging.
8. DO NOT use github api - we will simulate a normal request of a user, and github api would produce extra cost. So we make sure the whole process is free.