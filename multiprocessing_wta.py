# from multiprocessing import Pool
import concurrent.futures
import pandas as pd
import time

#Having the function definition here results in
#AttributeError: Can't get attribute 'f' on <module '__main__' (built-in)>

#The solution seems to be importing the function from a separate file.

from web_scraping import create_hiking_csv_2, create_hiking_csv

#Also, the original version of f only had a print statement in it.  
#That doesn't work with Process - in the sense that it prints to the console 
#instead of the notebook.
#The trick is to let f write the string to print into an output-queue.
#When Process is done, the result is retrieved from the queue and printed.

MAX_THREADS = 30

df = pd.read_csv('WTA_Hiking.csv')
URLs = df['link'].values[:100]

def main(urls):
    t0 = time.time()
    # create_hiking_csv_2(urls)
    create_hiking_csv(urls)
    t1 = time.time()
    print(f"{t1-t0} seconds to download {len(urls)} stories.")

main(URLs)
# if __name__ == '__main__':    
#     URL = 'https://www.wta.org/go-outside/hikes'
#     with Pool(5) as p:
#         print(p.map(create_hiking_csv, df['link'].values))

#     Define an output queue
    # output=Queue()
    
  
    # # Setup a list of processes that we want to run
    # p = Process(target=collect_links, args=(URL,1))

    # # Run process
    # p.start()

    # # Exit the completed process
    # p.join()

    # # Get process results from the output queue
    # result = output.get(p)

    # print(result)