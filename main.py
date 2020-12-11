from urllib.parse import urlencode
from urllib.request import Request, urlopen
from requests import get
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from flask import request
import json 
import csv

def push_test(request):

    names = []
    costs = []
    reviews = []
    materials = []
    ratings = []
    
    requestNum = 0

    # request_args = request.args
    # name = request_args['url']

    # TODO(developer)
    project_id = "faasproject"
    topic_id = "pub-sub-test"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)



    while True:
        
#     Requesting Data from Amazon URL
        url = r'https://www.amazon.com/gp/bestsellers/books/283155/ref=s9_acsd_ri_bw_clnk/ref=s9_acsd_ri_bw_c2_x_c2cl?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=3V527206ZDK85DVV0K1Y&pf_rd_t=101&pf_rd_p=d2e01c79-7462-4300-8d41-3633536344dc&pf_rd_i=283155'
        
    #     Controlling amount of data and request made
        requestNum += 1
        if requestNum > 5:
            print('Number of requests was greater than expected.')  
            break 
        
        
        # Creating request headers
        req_headers = {}
        req_headers['user-agent'] = r'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17'

        # Creating a request object
        req = Request(url, headers=req_headers)

        try:
            books = 0
            response = urlopen(req)
            page_content = response.read()
            page_html = BeautifulSoup(page_content, 'lxml')
            
    #       Writing to Books.csv
            # file_name = 'Books.csv' 
            # with open(file_name, 'w') as f:
            #     op_writer = csv.writer(f)

            #     field_names = ['Book', 'Review', 'Book Type', 'Cost', 'Rate']
            #     op_writer.writerow(field_names)
                
    #             Fetching data from container
            mv_containers = page_html.find_all('li', class_ = 'zg-item-immersion')
            for container in mv_containers:
                
                    if container.find('a',class_='a-size-small a-link-normal') is not None and container.find('a',class_='a-size-small a-link-child') is not None:
                        completeData = {}
                        qualities =[]   
#                       scrape name
                        name =  container.find('a', class_ = 'a-size-small a-link-child').text
                        names.append(name)

#                       scrape review
                        review = container.find('a',class_='a-size-small a-link-normal').text
                        review = review.replace(',', '')
                        reviews.append(int(review))
                        qualities.append(review)

#                       scrape material
                        material = container.find('span',class_='a-size-small a-color-secondary').text
                        materials.append(material)
                        qualities.append(material)

#                       scrape cost
                        cost = container.find('span', class_ = 'p13n-sc-price').text
                        cost = cost[1:]
                        cost = cost[:-3].replace(',', '')
                        costs.append(int(cost))
                        qualities.append(cost)

#                       scrape rate
                        rate = container.find('span', class_ = 'a-icon-alt').text
                        rate = rate[:3]
                        ratings.append(float(rate))
                        qualities.append(rate)

                        # op_writer.writerow([name,review,material,cost,rate])

                        completeData[name] = list()
                        completeData[name].extend(qualities)
                        res = not bool(completeData)
                        if res is False:
                            result = json.dumps(completeData)
                            result = result.encode("utf-8")

                            future = publisher.publish(topic_path, result)
                            print(future.result())
            print(completeData)
        except Exception as e:
            print(e)    
        
    

    response = get("https://us-central1-faasproject.cloudfunctions.net/pull_test")
    print(response.text)
    done = {}
    done["Result"] = "Done"
    return addCors(json.dumps(done))

def addCors(response, code=200):
    headers = {'Access-Control-Allow-Origin': '*'}
    return (response, code, headers)